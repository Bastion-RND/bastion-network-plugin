import Foundation
#if canImport(Darwin)
import Darwin
#endif

/// Универсальный UDP-broadcast discovery + TCP-транспорт на POSIX-сокетах.
/// Используются низкоуровневые BSD-сокеты, а не Network.framework, т.к. приём
/// broadcast-датаграмм от произвольного отправителя там менее предсказуем, чем
/// NWListener/NWConnection.
///
/// TCP работает всегда без какой-либо настройки. Для UDP (sendBroadcastUDP/
/// onReceiveUDP) обязателен конфиг плагина в capacitor.config.json
/// (portListTransmit/portListReceive) — без него UDP не работает, никаких
/// дефолтов не подставляется.
final class NetworkTransport {
    private static let readBufferSize = 8192

    var onReceiveUDP: ((String, UInt16, Data) -> Void)?
    var onReceiveTCP: ((Int32, Data) -> Void)?
    var onDisconnectTCP: ((Int32) -> Void)?

    private let portListTransmit: [UInt16]?
    private let portListReceive: [UInt16]?
    private let tcpNoDelay: Bool
    private let tcpConnectTimeoutMs: Int
    private let tcpKeepAliveIdleSeconds: Int32
    private let tcpKeepAliveIntervalSeconds: Int32
    private let tcpKeepAliveProbeCount: Int32
    private var udpSockets: [Int32] = []
    private var tcpConnections: [Int32: Int32] = [:]
    private var pendingConnections: Set<Int32> = []
    private var tcpReaders: Set<Int32> = []
    private var stopped = false
    private let tcpQueue = DispatchQueue(label: "ru.bast.plugins.network.tcp")
    private var linkIdCounter: Int32 = 0

    init(portListTransmit: [UInt16]?, portListReceive: [UInt16]?, tcpNoDelay: Bool,
         tcpConnectTimeoutMs: Int, tcpKeepAliveIdleSeconds: Int32,
         tcpKeepAliveIntervalSeconds: Int32, tcpKeepAliveProbeCount: Int32) {
        self.portListTransmit = portListTransmit
        self.portListReceive = portListReceive
        self.tcpNoDelay = tcpNoDelay
        self.tcpConnectTimeoutMs = max(1, tcpConnectTimeoutMs)
        self.tcpKeepAliveIdleSeconds = tcpKeepAliveIdleSeconds
        self.tcpKeepAliveIntervalSeconds = tcpKeepAliveIntervalSeconds
        self.tcpKeepAliveProbeCount = tcpKeepAliveProbeCount
    }

    func start() {
        guard let portListReceive else { return }
        for port in portListReceive {
            startUdpListener(port: port)
        }
    }

    func stop() {
        for socketFD in udpSockets {
            close(socketFD)
        }
        udpSockets.removeAll()

        tcpQueue.sync {
            stopped = true
            for socketFD in pendingConnections { shutdown(socketFD, SHUT_RDWR) }
            for (linkId, socketFD) in tcpConnections {
                shutdown(socketFD, SHUT_RDWR)
                if !tcpReaders.contains(linkId) { close(socketFD) }
            }
            tcpConnections.removeAll()
        }
    }

    // MARK: - UDP

    private func startUdpListener(port: UInt16) {
        let socketFD = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
        guard socketFD >= 0 else { return }

        var reuse: Int32 = 1
        setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size))
        setsockopt(socketFD, SOL_SOCKET, SO_REUSEPORT, &reuse, socklen_t(MemoryLayout<Int32>.size))

        var addr = sockaddr_in()
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = port.bigEndian
        addr.sin_addr.s_addr = INADDR_ANY

        let bindResult = withUnsafePointer(to: &addr) { ptr -> Int32 in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                bind(socketFD, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindResult == 0 else {
            close(socketFD)
            return
        }

        udpSockets.append(socketFD)

        let thread = Thread { [weak self] in self?.udpReceiveLoop(socketFD: socketFD) }
        thread.name = "BastionNetworkPlugin.udp.\(port)"
        thread.start()
    }

    private func udpReceiveLoop(socketFD: Int32) {
        var buffer = [UInt8](repeating: 0, count: 65535)
        while true {
            var senderAddr = sockaddr_in()
            var senderLen = socklen_t(MemoryLayout<sockaddr_in>.size)

            let bytesRead = withUnsafeMutablePointer(to: &senderAddr) { addrPtr -> Int in
                addrPtr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
                    recvfrom(socketFD, &buffer, buffer.count, 0, sockaddrPtr, &senderLen)
                }
            }
            if bytesRead < 0 { break }

            let data = Data(bytes: buffer, count: bytesRead)
            var hostBuffer = [CChar](repeating: 0, count: Int(NI_MAXHOST))
            var senderAddrForConversion = senderAddr
            inet_ntop(AF_INET, &senderAddrForConversion.sin_addr, &hostBuffer, socklen_t(hostBuffer.count))
            let ip = String(cString: hostBuffer)
            let port = UInt16(bigEndian: senderAddr.sin_port)

            onReceiveUDP?(ip, port, data)
        }
    }

    @discardableResult
    func sendBroadcastUDP(data: Data) -> Result<Void, NetworkTransportError> {
        guard let portListTransmit else { return .failure(.portsNotConfigured) }

        let socketFD = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
        guard socketFD >= 0 else { return .failure(.socketCreationFailed) }
        defer { close(socketFD) }

        var broadcastEnable: Int32 = 1
        setsockopt(socketFD, SOL_SOCKET, SO_BROADCAST, &broadcastEnable, socklen_t(MemoryLayout<Int32>.size))

        var hadError = false
        for address in getBroadcastAddresses() {
            for port in portListTransmit {
                var addr = sockaddr_in()
                addr.sin_family = sa_family_t(AF_INET)
                addr.sin_port = port.bigEndian
                inet_pton(AF_INET, address, &addr.sin_addr)

                let sent = data.withUnsafeBytes { rawBuffer -> Int in
                    withUnsafePointer(to: &addr) { addrPtr -> Int in
                        addrPtr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
                            sendto(socketFD, rawBuffer.baseAddress, data.count, 0, sockaddrPtr, socklen_t(MemoryLayout<sockaddr_in>.size))
                        }
                    }
                }
                if sent != data.count { hadError = true }
            }
        }

        return hadError ? .failure(.writeFailed) : .success(())
    }

    private func getBroadcastAddresses() -> [String] {
        var addresses: [String] = []
        var ifaddrPtr: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&ifaddrPtr) == 0, let firstAddr = ifaddrPtr else { return addresses }
        defer { freeifaddrs(ifaddrPtr) }

        var pointer: UnsafeMutablePointer<ifaddrs>? = firstAddr
        while let current = pointer {
            let interface = current.pointee
            let flags = Int32(interface.ifa_flags)
            let isUp = (flags & IFF_UP) != 0
            let isLoopback = (flags & IFF_LOOPBACK) != 0
            let hasBroadcast = (flags & IFF_BROADCAST) != 0

            if let addr = interface.ifa_addr, addr.pointee.sa_family == UInt8(AF_INET),
               isUp, !isLoopback, hasBroadcast, let dstAddr = interface.ifa_dstaddr {
                var hostBuffer = [CChar](repeating: 0, count: Int(NI_MAXHOST))
                getnameinfo(dstAddr, socklen_t(dstAddr.pointee.sa_len), &hostBuffer, socklen_t(hostBuffer.count), nil, 0, NI_NUMERICHOST)
                addresses.append(String(cString: hostBuffer))
            }
            pointer = interface.ifa_next
        }
        return addresses
    }

    // MARK: - TCP

    /// `IP_BOUND_IF` из <netinet/in.h> (Darwin) — не экспортируется в Swift-модуль Darwin
    /// напрямую, поэтому задаём значением. Используется, чтобы явно привязать сокет к
    /// интерфейсу, у которого есть локальный маршрут до нужного хоста: iOS сам выбирает
    /// "сеть по умолчанию" (например, сотовую вместо Wi-Fi), и сокет без привязки может
    /// уйти не туда и не достучаться до устройства в локальной сети — тот же класс
    /// проблемы, что и на Android (см. `findNetworkForHost` в BastionNetworkPlugin.java).
    private static let ipBoundIf: Int32 = 25

    /// Ищет интерфейс, чей локальный адрес+маска покрывают хост — то есть интерфейс,
    /// с которого этот хост реально достижим напрямую. Не завязано на конкретный
    /// тип интерфейса (Wi-Fi/сотовая/что угодно), поэтому остаётся универсальным.
    private func interfaceIndex(forHost host: String) -> UInt32? {
        var targetAddr = in_addr()
        guard inet_pton(AF_INET, host, &targetAddr) == 1 else { return nil }

        var ifaddrPtr: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&ifaddrPtr) == 0, let firstAddr = ifaddrPtr else { return nil }
        defer { freeifaddrs(ifaddrPtr) }

        var pointer: UnsafeMutablePointer<ifaddrs>? = firstAddr
        while let current = pointer {
            let interface = current.pointee
            let flags = Int32(interface.ifa_flags)
            let isUp = (flags & IFF_UP) != 0
            let isLoopback = (flags & IFF_LOOPBACK) != 0

            if isUp, !isLoopback,
               let addrPtr = interface.ifa_addr, addrPtr.pointee.sa_family == UInt8(AF_INET),
               let maskPtr = interface.ifa_netmask, maskPtr.pointee.sa_family == UInt8(AF_INET) {
                let ifaceAddr = addrPtr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee.sin_addr }
                let ifaceMask = maskPtr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee.sin_addr }

                if (ifaceAddr.s_addr & ifaceMask.s_addr) == (targetAddr.s_addr & ifaceMask.s_addr) {
                    let index = if_nametoindex(interface.ifa_name)
                    if index != 0 {
                        return index
                    }
                }
            }
            pointer = interface.ifa_next
        }
        return nil
    }

    func connectTCP(host: String, port: UInt16, initialData: Data,
                    completion: @escaping (Result<Int32, Error>) -> Void) {
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self = self else { return }
            let socketFD = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)
            guard socketFD >= 0 else { completion(.failure(NetworkTransportError.socketCreationFailed)); return }
            let accepted = self.tcpQueue.sync { () -> Bool in
                guard !self.stopped else { return false }
                self.pendingConnections.insert(socketFD)
                return true
            }
            guard accepted else { close(socketFD); completion(.failure(NetworkTransportError.connectFailed)); return }
            var registered = false
            defer {
                self.tcpQueue.sync { _ = self.pendingConnections.remove(socketFD) }
                if !registered { close(socketFD) }
            }
            if var index = self.interfaceIndex(forHost: host) {
                setsockopt(socketFD, IPPROTO_IP, Self.ipBoundIf, &index, socklen_t(MemoryLayout<UInt32>.size))
            }
            var noDelay: Int32 = self.tcpNoDelay ? 1 : 0
            var keepAlive: Int32 = 1
            var noSigPipe: Int32 = 1
            guard setsockopt(socketFD, IPPROTO_TCP, TCP_NODELAY, &noDelay, socklen_t(MemoryLayout<Int32>.size)) == 0,
                  setsockopt(socketFD, SOL_SOCKET, SO_KEEPALIVE, &keepAlive, socklen_t(MemoryLayout<Int32>.size)) == 0,
                  setsockopt(socketFD, SOL_SOCKET, SO_NOSIGPIPE, &noSigPipe, socklen_t(MemoryLayout<Int32>.size)) == 0 else {
                completion(.failure(NetworkTransportError.socketConfigurationFailed)); return
            }
            if self.tcpKeepAliveIdleSeconds > 0 && self.tcpKeepAliveIntervalSeconds > 0 && self.tcpKeepAliveProbeCount > 0 {
                var idle = self.tcpKeepAliveIdleSeconds
                var interval = self.tcpKeepAliveIntervalSeconds
                var count = self.tcpKeepAliveProbeCount
                guard setsockopt(socketFD, IPPROTO_TCP, TCP_KEEPALIVE, &idle, socklen_t(MemoryLayout<Int32>.size)) == 0,
                      setsockopt(socketFD, IPPROTO_TCP, TCP_KEEPINTVL, &interval, socklen_t(MemoryLayout<Int32>.size)) == 0,
                      setsockopt(socketFD, IPPROTO_TCP, TCP_KEEPCNT, &count, socklen_t(MemoryLayout<Int32>.size)) == 0 else {
                    completion(.failure(NetworkTransportError.socketConfigurationFailed)); return
                }
            }
            let flags = fcntl(socketFD, F_GETFL, 0)
            guard flags >= 0, fcntl(socketFD, F_SETFL, flags | O_NONBLOCK) == 0 else {
                completion(.failure(NetworkTransportError.socketConfigurationFailed)); return
            }
            let deadline = DispatchTime.now().uptimeNanoseconds + UInt64(self.tcpConnectTimeoutMs) * 1_000_000
            func waitWritable() -> Bool {
                while true {
                    let now = DispatchTime.now().uptimeNanoseconds
                    if now >= deadline { return false }
                    let remaining = Int32(min((deadline - now + 999_999) / 1_000_000, UInt64(Int32.max)))
                    var descriptor = pollfd(fd: socketFD, events: Int16(POLLOUT), revents: 0)
                    let result = poll(&descriptor, 1, remaining)
                    if result > 0 { return (descriptor.revents & Int16(POLLOUT)) != 0 }
                    if result == 0 || errno != EINTR { return false }
                }
            }
            var addr = sockaddr_in()
            addr.sin_family = sa_family_t(AF_INET)
            addr.sin_port = port.bigEndian
            guard inet_pton(AF_INET, host, &addr.sin_addr) == 1 else {
                completion(.failure(NetworkTransportError.connectFailed)); return
            }
            let result = withUnsafePointer(to: &addr) { ptr in
                ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    connect(socketFD, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
                }
            }
            if result != 0 {
                guard errno == EINPROGRESS, waitWritable() else {
                    completion(.failure(NetworkTransportError.connectFailed)); return
                }
                var error: Int32 = 0
                var length = socklen_t(MemoryLayout<Int32>.size)
                guard getsockopt(socketFD, SOL_SOCKET, SO_ERROR, &error, &length) == 0, error == 0 else {
                    completion(.failure(NetworkTransportError.connectFailed)); return
                }
            }
            let written = initialData.withUnsafeBytes { raw -> Bool in
                var offset = 0
                while offset < raw.count {
                    guard waitWritable(), let base = raw.baseAddress else { return false }
                    let size = write(socketFD, base.advanced(by: offset), raw.count - offset)
                    if size > 0 { offset += size }
                    else if size < 0 && (errno == EINTR || errno == EAGAIN) { continue }
                    else { return false }
                }
                return true
            }
            guard written, fcntl(socketFD, F_SETFL, flags) == 0 else {
                completion(.failure(NetworkTransportError.writeFailed)); return
            }
            let linkId: Int32? = self.tcpQueue.sync {
                guard !self.stopped else { return nil }
                self.pendingConnections.remove(socketFD)
                self.linkIdCounter += 1
                self.tcpConnections[self.linkIdCounter] = socketFD
                return self.linkIdCounter
            }
            guard let linkId = linkId else { completion(.failure(NetworkTransportError.connectFailed)); return }
            registered = true
            completion(.success(linkId))
            self.startTcpReader(linkId: linkId)
        }
    }

    private func startTcpReader(linkId: Int32) {
        tcpQueue.sync {
            guard let socketFD = tcpConnections[linkId] else { return }
            if tcpReaders.insert(linkId).inserted {
                let thread = Thread { self.tcpReadLoop(linkId: linkId, socketFD: socketFD) }
                thread.start()
            }
        }
    }

    private func tcpReadLoop(linkId: Int32, socketFD: Int32) {
        var buffer = [UInt8](repeating: 0, count: Self.readBufferSize)
        while true {
            let bytesRead = read(socketFD, &buffer, buffer.count)
            if bytesRead <= 0 { break }

            let data = Data(bytes: buffer, count: bytesRead)
            onReceiveTCP?(linkId, data)
        }

        tcpQueue.async {
            self.tcpReaders.remove(linkId)
            if self.tcpConnections[linkId] == socketFD {
                self.tcpConnections.removeValue(forKey: linkId)
            }
            close(socketFD)
            self.onDisconnectTCP?(linkId)
        }
    }

    func sendTCP(linkId: Int32, data: Data, completion: @escaping (Result<Void, Error>) -> Void) {
        tcpQueue.async { [weak self] in
            guard let socketFD = self?.tcpConnections[linkId] else {
                completion(.failure(NetworkTransportError.unknownLinkId))
                return
            }
            let writeOk = data.withUnsafeBytes { rawBuffer -> Bool in
                var offset = 0
                while offset < rawBuffer.count {
                    guard let base = rawBuffer.baseAddress else { return false }
                    let written = write(socketFD, base.advanced(by: offset), rawBuffer.count - offset)
                    if written > 0 { offset += written }
                    else if written < 0 && errno == EINTR { continue }
                    else { return false }
                }
                return true
            }
            completion(writeOk ? .success(()) : .failure(NetworkTransportError.writeFailed))
        }
    }

    func disconnectTCP(linkId: Int32) {
        tcpQueue.async { [weak self] in
            guard let socketFD = self?.tcpConnections.removeValue(forKey: linkId) else { return }
            shutdown(socketFD, SHUT_RDWR)
            if self?.tcpReaders.contains(linkId) != true {
                close(socketFD)
                self?.onDisconnectTCP?(linkId)
            }
        }
    }
}

enum NetworkTransportError: Error {
    case portsNotConfigured
    case socketCreationFailed
    case socketConfigurationFailed
    case connectFailed
    case writeFailed
    case unknownLinkId
}

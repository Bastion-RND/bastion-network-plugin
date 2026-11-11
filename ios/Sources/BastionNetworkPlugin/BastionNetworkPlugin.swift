import Foundation
import Capacitor

@objc(BastionNetworkPlugin)
public class BastionNetworkPlugin: CAPPlugin {
    private var transport: NetworkTransport!

    deinit { transport?.stop() }

    override public func load() {
        transport = NetworkTransport(
            portListTransmit: readPortListConfig("portListTransmit"),
            portListReceive: readPortListConfig("portListReceive"),
            tcpNoDelay: getConfig().getBoolean("tcpNoDelay", false),
            tcpConnectTimeoutMs: getConfig().getInt("tcpConnectTimeoutMs", 10000),
            tcpKeepAliveIdleSeconds: Int32(getConfig().getInt("tcpKeepAliveIdleSeconds", 0)),
            tcpKeepAliveIntervalSeconds: Int32(getConfig().getInt("tcpKeepAliveIntervalSeconds", 0)),
            tcpKeepAliveProbeCount: Int32(getConfig().getInt("tcpKeepAliveProbeCount", 0))
        )

        transport.onReceiveUDP = { [weak self] ip, port, data in
            let result: [String: Any] = [
                "ip": ip,
                "port": Int(port),
                "data": data.base64EncodedString(),
            ]
            self?.notifyListeners("receiveUDP", data: result)
        }

        transport.onReceiveTCP = { [weak self] linkId, data in
            let result: [String: Any] = [
                "linkId": Int(linkId),
                "data": data.base64EncodedString(),
            ]
            self?.notifyListeners("receiveTCP", data: result)
        }

        transport.onDisconnectTCP = { [weak self] linkId in
            self?.notifyListeners("disconnectTCP", data: ["linkId": Int(linkId)])
        }

        transport.start()
    }

    private func readPortListConfig(_ key: String) -> [UInt16]? {
        guard let values: [Int] = getConfig().getArray(key) else { return nil }
        return values.map { UInt16($0) }
    }

    @objc func sendBroadcastUDP(_ call: CAPPluginCall) {
        guard let base64Data = call.getString("data"), let data = Data(base64Encoded: base64Data) else {
            call.reject("data is required")
            return
        }

        switch transport.sendBroadcastUDP(data: data) {
        case .success:
            call.resolve()
        case .failure(.portsNotConfigured):
            call.reject("portListTransmit is not configured in capacitor.config.json for BastionNetworkPlugin")
        case .failure(let error):
            call.reject("UDP broadcast socket error", nil, error)
        }
    }

    @objc func connectTCP(_ call: CAPPluginCall) {
        guard let host = call.getString("ip"),
              let port = call.getInt("port"),
              let base64Data = call.getString("data"),
              let initialData = Data(base64Encoded: base64Data) else {
            call.reject("ip, port and data are required")
            return
        }

        transport.connectTCP(host: host, port: UInt16(port), initialData: initialData, deferEvents: call.getBool("deferEvents") ?? false) { result in
            switch result {
            case .success(let linkId):
                call.resolve(["linkId": Int(linkId)])
            case .failure(let error):
                call.reject("TCP connection error", nil, error)
            }
        }
    }

    @objc func resumeTCP(_ call: CAPPluginCall) {
        guard let linkId = call.getInt("linkId"), transport.resumeTCP(linkId: Int32(linkId)) else {
            call.reject("Unknown linkId")
            return
        }
        call.resolve()
    }

    @objc func sendTCP(_ call: CAPPluginCall) {
        guard let linkId = call.getInt("linkId"),
              let base64Data = call.getString("data"),
              let data = Data(base64Encoded: base64Data) else {
            call.reject("linkId and data are required")
            return
        }

        transport.sendTCP(linkId: Int32(linkId), data: data) { result in
            switch result {
            case .success:
                call.resolve()
            case .failure(let error):
                call.reject("TCP transmit error", nil, error)
            }
        }
    }

    @objc func disconnectTCP(_ call: CAPPluginCall) {
        guard let linkId = call.getInt("linkId") else {
            call.reject("linkId is required")
            return
        }
        transport.disconnectTCP(linkId: Int32(linkId))
        call.resolve()
    }
}

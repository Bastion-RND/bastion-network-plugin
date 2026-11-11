package ru.bast.plugins.network;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.LinkProperties;
import android.net.Network;
import android.net.RouteInfo;
import android.net.wifi.WifiManager;
import android.os.ParcelFileDescriptor;
import android.os.Build;
import android.util.Base64;
import android.util.Log;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONArray;

/**
 * Универсальный UDP-broadcast discovery + TCP-транспорт для Capacitor. TCP работает
 * всегда без какой-либо настройки. Для UDP (sendBroadcastUDP/receiveUDP) обязателен
 * конфиг плагина в capacitor.config.json (portListTransmit/portListReceive) — без
 * него UDP-методы отклоняются с понятной ошибкой, никаких дефолтов не подставляется.
 */
@CapacitorPlugin(name = "BastionNetworkPlugin")
public class BastionNetworkPlugin extends Plugin {

    static { System.loadLibrary("bastion_network"); }

    private static native int configureKeepAlive(int fd, int idle, int interval, int count);

    private static final String TAG = "BastionNetworkPlugin";
    private static final int TCP_READ_BUFFER_SIZE = 8192;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Map<Integer, Socket> tcpConnections = new ConcurrentHashMap<>();
    private final Set<Socket> pendingConnections = ConcurrentHashMap.newKeySet();
    private final Map<Socket, ParcelFileDescriptor> borrowedDescriptors = new ConcurrentHashMap<>();
    private final Set<Integer> tcpReaders = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Object lifecycleLock = new Object();
    private final AtomicInteger linkIdCounter = new AtomicInteger(1);
    private final List<DatagramSocket> udpReceiveSockets = new ArrayList<>();
    private WifiManager.MulticastLock multicastLock;
    private volatile boolean destroyed = false;
    private int[] portListTransmit;
    private int[] portListReceive;
    private boolean tcpNoDelay;
    private int tcpConnectTimeoutMs;
    private int tcpKeepAliveIdleSeconds;
    private int tcpKeepAliveIntervalSeconds;
    private int tcpKeepAliveProbeCount;

    @Override
    public void load() {
        super.load();
        portListTransmit = readPortListConfig("portListTransmit");
        portListReceive = readPortListConfig("portListReceive");
        tcpNoDelay = getConfig().getBoolean("tcpNoDelay", false);
        tcpConnectTimeoutMs = Math.max(1, getConfig().getInt("tcpConnectTimeoutMs", 10000));
        tcpKeepAliveIdleSeconds = getConfig().getInt("tcpKeepAliveIdleSeconds", 0);
        tcpKeepAliveIntervalSeconds = getConfig().getInt("tcpKeepAliveIntervalSeconds", 0);
        tcpKeepAliveProbeCount = getConfig().getInt("tcpKeepAliveProbeCount", 0);
        acquireMulticastLock();
        if (portListReceive != null) {
            startUdpListeners();
        } else {
            Log.i(TAG, "portListReceive is not configured — UDP discovery disabled");
        }
    }

    /**
     * @return null, если ключ не задан в конфиге плагина — в отличие от отсутствующих
     * значений внутри массива (которые обнуляются через optInt), null конфига обязан
     * означать "не настроено", а не "порт 0".
     */
    private int[] readPortListConfig(String configKey) {
        JSONArray array = getConfig().getConfigJSON().optJSONArray(configKey);
        if (array == null) return null;

        int[] result = new int[array.length()];
        for (int i = 0; i < array.length(); i++) {
            result[i] = array.optInt(i, 0);
        }
        return result;
    }

    @Override
    protected void handleOnDestroy() {
        super.handleOnDestroy();
        synchronized (lifecycleLock) {
            destroyed = true;
            for (Socket socket : pendingConnections) closeQuietly(socket);
            pendingConnections.clear();
            for (Socket socket : tcpConnections.values()) closeQuietly(socket);
            tcpConnections.clear();
        }
        scheduler.shutdownNow();
        for (DatagramSocket socket : udpReceiveSockets) {
            socket.close();
        }
        udpReceiveSockets.clear();
        if (multicastLock != null && multicastLock.isHeld()) {
            multicastLock.release();
        }
        executor.shutdownNow();
    }

    private void acquireMulticastLock() {
        WifiManager wifiManager = (WifiManager) getContext().getApplicationContext().getSystemService(Context.WIFI_SERVICE);
        if (wifiManager == null) return;
        multicastLock = wifiManager.createMulticastLock(TAG);
        multicastLock.setReferenceCounted(true);
        multicastLock.acquire();
    }

    private void startUdpListeners() {
        for (int port : portListReceive) {
            executor.execute(() -> listenUdp(port));
        }
    }

    private void listenUdp(int port) {
        try {
            DatagramSocket socket = new DatagramSocket(null);
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("0.0.0.0", port));
            udpReceiveSockets.add(socket);
            Log.i(TAG, "UDP listener starts 0.0.0.0:" + port);

            byte[] buffer = new byte[65535];
            while (!socket.isClosed()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                byte[] data = new byte[packet.getLength()];
                System.arraycopy(packet.getData(), packet.getOffset(), data, 0, packet.getLength());

                JSObject result = new JSObject();
                result.put("ip", packet.getAddress().getHostAddress());
                result.put("port", packet.getPort());
                result.put("data", Base64.encodeToString(data, Base64.NO_WRAP));
                notifyListeners("receiveUDP", result);
            }
        } catch (IOException e) {
            if (!destroyed) {
                Log.e(TAG, "UDP listener error on port " + port, e);
            }
        }
    }

    @PluginMethod
    public void sendBroadcastUDP(PluginCall call) {
        if (portListTransmit == null) {
            call.reject("portListTransmit is not configured in capacitor.config.json for BastionNetworkPlugin");
            return;
        }
        String base64Data = call.getString("data");
        if (base64Data == null) {
            call.reject("data is required");
            return;
        }
        byte[] data = Base64.decode(base64Data, Base64.NO_WRAP);

        executor.execute(() -> {
            List<InetAddress> broadcastAddresses = getBroadcastAddresses();
            try (DatagramSocket socket = new DatagramSocket()) {
                socket.setBroadcast(true);
                boolean hadError = false;
                for (InetAddress address : broadcastAddresses) {
                    for (int port : portListTransmit) {
                        try {
                            socket.send(new DatagramPacket(data, data.length, address, port));
                            Log.i(TAG, "UDP broadcast send successfully " + address.getHostAddress() + ":" + port);
                        } catch (IOException e) {
                            Log.e(TAG, "UDP broadcast send error " + address.getHostAddress() + ":" + port, e);
                            hadError = true;
                        }
                    }
                }
                if (hadError) call.reject("UDP broadcast send error");
                else call.resolve();
            } catch (IOException e) {
                call.reject("UDP broadcast socket error", e);
            }
        });
    }

    private List<InetAddress> getBroadcastAddresses() {
        List<InetAddress> result = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isLoopback() || !networkInterface.isUp()) continue;

                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    InetAddress broadcast = interfaceAddress.getBroadcast();
                    if (broadcast != null) {
                        result.add(broadcast);
                    }
                }
            }
        } catch (SocketException e) {
            Log.e(TAG, "Failed to enumerate network interfaces", e);
        }
        return result;
    }

    /**
     * Ищет среди всех активных сетей ту, у которой есть локальный (не дефолтный) маршрут
     * до нужного хоста, и биндит сокет на неё. Это нужно, потому что Android выбирает
     * "сеть по умолчанию" по своим критериям (например, предпочитает мобильный интернет
     * Wi-Fi без validated-доступа в интернет) — обычный сокет без привязки уедет через неё
     * и не сможет достучаться до устройства в локальной сети. Метод не завязан на
     * конкретный транспорт (Wi-Fi/Ethernet/что угодно) — работает с любой сетью, реально
     * имеющей маршрут до хоста, поэтому плагин остаётся универсальным.
     */
    @Nullable
    private Network findNetworkForHost(InetAddress host) {
        ConnectivityManager connectivityManager =
            (ConnectivityManager) getContext().getApplicationContext().getSystemService(Context.CONNECTIVITY_SERVICE);
        if (connectivityManager == null) return null;

        for (Network network : connectivityManager.getAllNetworks()) {
            LinkProperties linkProperties = connectivityManager.getLinkProperties(network);
            if (linkProperties == null) continue;

            for (RouteInfo route : linkProperties.getRoutes()) {
                if (!route.isDefaultRoute() && route.matches(host)) {
                    return network;
                }
            }
        }
        return null;
    }

    @PluginMethod
    public void connectTCP(PluginCall call) {
        String host = call.getString("ip");
        Integer port = call.getInt("port");
        String base64Data = call.getString("data");

        if (host == null || port == null || base64Data == null) {
            call.reject("ip, port and data are required");
            return;
        }
        byte[] initialData = Base64.decode(base64Data, Base64.NO_WRAP);

        boolean deferEvents = Boolean.TRUE.equals(call.getBoolean("deferEvents", false));
        executor.execute(() -> {
            int linkId = linkIdCounter.getAndIncrement();
            Socket socket = new Socket();
            ScheduledFuture<?> timeout;
            AtomicBoolean finished = new AtomicBoolean(false);
            synchronized (lifecycleLock) {
                if (destroyed) { closeQuietly(socket); call.reject("Plugin destroyed"); return; }
                pendingConnections.add(socket);
                timeout = scheduler.schedule(() -> {
                    synchronized (socket) { if (!finished.get()) closeQuietly(socket); }
                }, tcpConnectTimeoutMs, TimeUnit.MILLISECONDS);
            }
            boolean registered = false;
            try {
                InetAddress hostAddress = InetAddress.getByName(host);
                Network targetNetwork = findNetworkForHost(hostAddress);
                if (targetNetwork != null) targetNetwork.bindSocket(socket);
                socket.connect(new InetSocketAddress(hostAddress, port), tcpConnectTimeoutMs);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(tcpNoDelay);
                if (tcpKeepAliveIdleSeconds > 0 && tcpKeepAliveIntervalSeconds > 0 && tcpKeepAliveProbeCount > 0) {
                    ParcelFileDescriptor borrowed = ParcelFileDescriptor.fromSocket(socket);
                    if (borrowed == null) throw new IOException("Unable to access TCP socket");
                    if (Build.VERSION.SDK_INT >= 29) {
                        try (ParcelFileDescriptor descriptor = borrowed) {
                            if (configureKeepAlive(descriptor.getFd(), tcpKeepAliveIdleSeconds, tcpKeepAliveIntervalSeconds, tcpKeepAliveProbeCount) != 0)
                                throw new IOException("Unable to configure TCP keepalive");
                        }
                    } else {
                        borrowedDescriptors.put(socket, borrowed);
                        int result = configureKeepAlive(borrowed.getFd(), tcpKeepAliveIdleSeconds, tcpKeepAliveIntervalSeconds, tcpKeepAliveProbeCount);
                        if (result != 0) throw new IOException("Unable to configure TCP keepalive");
                    }
                }
                OutputStream out = socket.getOutputStream();
                out.write(initialData);
                out.flush();
                synchronized (lifecycleLock) {
                    synchronized (socket) {
                        if (destroyed || socket.isClosed()) throw new IOException("TCP connection timeout");
                        finished.set(true);
                        timeout.cancel(false);
                        pendingConnections.remove(socket);
                        tcpConnections.put(linkId, socket);
                        registered = true;
                    }
                }
                JSObject result = new JSObject();
                result.put("linkId", linkId);
                call.resolve(result);
                if (!deferEvents) startTcpReader(linkId, socket);
            } catch (IOException | RuntimeException e) {
                call.reject("TCP connection error", e);
            } finally {
                timeout.cancel(false);
                pendingConnections.remove(socket);
                if (!registered) closeQuietly(socket);
            }
        });
    }

    private void startTcpReader(int linkId, Socket socket) {
        synchronized (lifecycleLock) {
            if (tcpConnections.get(linkId) == socket && tcpReaders.add(linkId)) {
                executor.execute(() -> readTcpLoop(linkId, socket));
            }
        }
    }

    @PluginMethod
    public void resumeTCP(PluginCall call) {
        Integer linkId = call.getInt("linkId");
        Socket socket = linkId == null ? null : tcpConnections.get(linkId);
        if (socket == null) { call.reject("Unknown linkId " + linkId); return; }
        startTcpReader(linkId, socket);
        call.resolve();
    }

    private void readTcpLoop(int linkId, Socket socket) {
        try {
            InputStream in = socket.getInputStream();
            byte[] buffer = new byte[TCP_READ_BUFFER_SIZE];
            int read;
            while ((read = in.read(buffer)) != -1) {
                byte[] data = new byte[read];
                System.arraycopy(buffer, 0, data, 0, read);

                JSObject result = new JSObject();
                result.put("linkId", linkId);
                result.put("data", Base64.encodeToString(data, Base64.NO_WRAP));
                notifyListeners("receiveTCP", result);
            }
        } catch (IOException e) {
            Log.i(TAG, "TCP connection closed with error, linkId=" + linkId, e);
        } finally {
            synchronized (lifecycleLock) {
                tcpConnections.remove(linkId, socket);
                tcpReaders.remove(linkId);
                closeQuietly(socket);
            }

            JSObject result = new JSObject();
            result.put("linkId", linkId);
            notifyListeners("disconnectTCP", result);
        }
    }

    @PluginMethod
    public void sendTCP(PluginCall call) {
        Integer linkId = call.getInt("linkId");
        String base64Data = call.getString("data");
        if (linkId == null || base64Data == null) {
            call.reject("linkId and data are required");
            return;
        }

        Socket socket = tcpConnections.get(linkId);
        if (socket == null) {
            call.reject("Unknown linkId " + linkId);
            return;
        }
        byte[] data = Base64.decode(base64Data, Base64.NO_WRAP);

        executor.execute(() -> {
            try {
                OutputStream out = socket.getOutputStream();
                out.write(data);
                out.flush();
                call.resolve();
            } catch (IOException e) {
                Log.e(TAG, "TCP transmit error, linkId=" + linkId, e);
                call.reject("TCP transmit error", e);
            }
        });
    }

    @PluginMethod
    public void disconnectTCP(PluginCall call) {
        Integer linkId = call.getInt("linkId");
        if (linkId == null) {
            call.reject("linkId is required");
            return;
        }

        synchronized (lifecycleLock) {
            Socket socket = tcpConnections.remove(linkId);
            if (socket != null) {
                boolean readerStarted = tcpReaders.contains(linkId);
                closeQuietly(socket);
                if (!readerStarted) {
                    JSObject result = new JSObject();
                    result.put("linkId", linkId);
                    notifyListeners("disconnectTCP", result);
                }
            }
        }
        call.resolve();
    }

    private void closeQuietly(@NonNull Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            Log.e(TAG, "Socket close error", e);
        } finally {
            ParcelFileDescriptor borrowed = borrowedDescriptors.remove(socket);
            if (borrowed != null) {
                try { borrowed.close(); } catch (IOException e) { Log.e(TAG, "Descriptor close error", e); }
            }
        }
    }
}

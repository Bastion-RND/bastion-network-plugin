import type { PluginListenerHandle } from '@capacitor/core';

/**
 * Все бинарные payload'ы (UDP-датаграммы, TCP-пакеты) передаются через мост
 * Capacitor как base64-строки — JS<->native bridge работает только с JSON.
 */
export type TNetworkData = {
  ip: string;
  port: number;
  data: string;
};

export type TTcpData = {
  linkId: number;
  data: string;
};

export type TTcpDisconnect = {
  linkId: number;
};

export type TSendBroadcastUdpOptions = {
  data: string;
};

export type TConnectTcpOptions = {
  ip: string;
  port: number;
  data: string;
};

export type TConnectTcpResult = {
  linkId: number;
};

export type TDisconnectTcpOptions = {
  linkId: number;
};

export type TSendTcpOptions = {
  linkId: number;
  data: string;
};

export interface IBastionNetworkPlugin {
  /** Разослать UDP broadcast-пакет на все локальные подсети (discovery устройств). */
  sendBroadcastUDP(options: TSendBroadcastUdpOptions): Promise<void>;

  /** Открыть TCP-соединение и отправить первым пакетом данные авторизации. */
  connectTCP(options: TConnectTcpOptions): Promise<TConnectTcpResult>;

  /** Закрыть TCP-соединение по linkId. */
  disconnectTCP(options: TDisconnectTcpOptions): Promise<void>;

  /** Отправить данные в уже открытое TCP-соединение. */
  sendTCP(options: TSendTcpOptions): Promise<void>;

  addListener(
    eventName: 'receiveUDP',
    listenerFunc: (data: TNetworkData) => void,
  ): Promise<PluginListenerHandle>;

  addListener(
    eventName: 'receiveTCP',
    listenerFunc: (data: TTcpData) => void,
  ): Promise<PluginListenerHandle>;

  addListener(
    eventName: 'disconnectTCP',
    listenerFunc: (data: TTcpDisconnect) => void,
  ): Promise<PluginListenerHandle>;

  removeAllListeners(): Promise<void>;
}

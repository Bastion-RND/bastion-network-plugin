# bastion-network-plugin

Raw TCP/UDP socket plugin for Capacitor 6 and higher. Даёт прямой доступ к сырым
сокетам из JS: UDP-broadcast для обнаружения устройств в локальной сети и TCP для
подключения к ним и обмена произвольными данными. Порты и формат данных полностью
на стороне приложения — плагин ничего не знает о конкретном протоколе.

Веб-платформа не поддерживается (сырые сокеты в браузере недоступны).

## Install

```bash
npm i @bs-solutions/bastion-network-plugin
npx cap sync
```

### Android

Плагин объявляет `INTERNET`, `ACCESS_WIFI_STATE`, `CHANGE_WIFI_MULTICAST_STATE` в
собственном манифесте — дополнительно ничего добавлять не нужно.

### iOS

Начиная с iOS 14 приложению нужно самому объявить local network permission в
своём `Info.plist` — плагин это сделать не может:

```xml
<key>NSLocalNetworkUsageDescription</key>
<string>Нужен доступ к локальной сети для поиска устройств</string>
```

## Конфигурация

TCP (`connectTCP`/`sendTCP`/`disconnectTCP`) работает всегда без какой-либо
настройки. Для UDP (`sendBroadcastUDP`/`receiveUDP`) порты обязательно нужно
задать конфигом плагина в `capacitor.config.json` — дефолтов нет намеренно,
без конфига UDP-методы отклоняются с понятной ошибкой:

```json
{
  "plugins": {
    "BastionNetworkPlugin": {
      "portListTransmit": [23629, 47701],
      "portListReceive": [23569, 21913]
    }
  }
}
```

- `portListTransmit` — порты, на которые уходит `sendBroadcastUDP` (на все локальные подсети). Без этого ключа `sendBroadcastUDP` вернёт reject.
- `portListReceive` — порты, которые слушаются на входящие UDP-пакеты (`receiveUDP`). Без этого ключа слушатели просто не поднимаются (без ошибки — приложению, которому не нужен UDP, ничего делать не надо).

## API

Все бинарные данные (UDP-датаграммы, TCP-пакеты) передаются как base64-строки.

```ts
import { BastionNetworkPlugin } from '@bs-solutions/bastion-network-plugin';

await BastionNetworkPlugin.addListener('receiveUDP', ({ ip, port, data }) => {
  // data — base64
});
await BastionNetworkPlugin.addListener('receiveTCP', ({ linkId, data }) => {});
await BastionNetworkPlugin.addListener('disconnectTCP', ({ linkId }) => {});

await BastionNetworkPlugin.sendBroadcastUDP({ data: base64Data });

const { linkId } = await BastionNetworkPlugin.connectTCP({
  ip: '192.168.1.50',
  port: 1234,
  data: base64AuthToken,
});

await BastionNetworkPlugin.sendTCP({ linkId, data: base64Data });
await BastionNetworkPlugin.disconnectTCP({ linkId });
```

### Методы

| Метод              | Описание                                                             |
| ------------------ | --------------------------------------------------------------------- |
| `sendBroadcastUDP` | Разослать UDP broadcast на все локальные подсети (порты из `portListTransmit`) |
| `connectTCP`       | Открыть TCP-соединение, первым пакетом отправить данные авторизации  |
| `sendTCP`          | Отправить данные в открытое TCP-соединение                            |
| `disconnectTCP`    | Закрыть TCP-соединение                                                |

### События

| Событие          | Когда срабатывает                                    |
| ---------------- | ------------------------------------------------------ |
| `receiveUDP`     | Пришёл UDP-пакет на один из портов `portListReceive`   |
| `receiveTCP`     | Пришли данные по открытому TCP-соединению               |
| `disconnectTCP`  | TCP-соединение закрыто (в т.ч. по инициативе устройства) |

## Maintainer

[sudondie](https://github.com/sudondie)

## Contributing

Please contribute! [Look at the issues](https://github.com/Bastion-RND/bastion-network-plugin/issues).

## License

MIT © 2026

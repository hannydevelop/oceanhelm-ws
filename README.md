# oceanhelm-ws

OceanHelm AIS gateway + decoder + WebSocket stream server.

## Architecture

- Legacy Go agents can continue connecting to `ws://HOST:9000/`.
- The public/client stream is `ws://HOST:9000/ws/stream`.
- Incoming agent messages use:
  `{ receiver_id, timestamp, nmea }`
- GPS NMEA sentences are dropped immediately.
- Only `!AIVDM` / `!AIVDO` sentences enter the AIS decoder.
- Each receiver has its own decoder instance so multipart AIS messages cannot be mixed between receivers.
- `receiver-001` can forward raw AIS NMEA to AISHub via UDP.
- `receiver_id` is never included in public WebSocket messages.
- Structured AIS messages are filtered by client bounding box and AIS message type.

## Install

```bash
npm install
cp .env.example .env
npm start
```

## Legacy Go-agent endpoint

The former server listened on:

```text
0.0.0.0:9000
```

This application preserves that endpoint. A Go agent can send:

```json
{
  "receiver_id": "receiver-002",
  "timestamp": "2026-09-11T19:30:00.000Z",
  "nmea": "!AIVDM,1,1,,A,...*00"
}
```

The agent receives:

```json
{
  "type": "connection_ack",
  "message": "OceanHelm WebSocket connection established"
}
```

## Public stream

Connect to:

```text
ws://HOST:9000/ws/stream
```

Then subscribe:

```json
{
  "action": "subscribe",
  "bbox": [-20, -35, 55, 37],
  "message_types": [1, 2, 3, 5, 18, 19, 21, 24]
}
```

The server responds:

```json
{
  "type": "subscription",
  "status": "ok"
}
```

Public AIS messages do not contain `receiver_id`.

## Important

The exact decoded field names depend on the installed `ais-stream-decoder` version. `src/ais/AisDecoder.js` intentionally isolates that package API from the rest of the application.

## Port 9000 is preserved

The original OceanHelm server used:

```js
const HOST = "0.0.0.0";
const PORT = 9000;
```

This application preserves that default exactly.

The legacy Go agent therefore continues to use:

```text
ws://<server>:9000/
```

The new structured AIS client stream is:

```text
ws://<server>:9000/ws/stream
```

Both use the same HTTP server and the `ws` `noServer`/`handleUpgrade` pattern.

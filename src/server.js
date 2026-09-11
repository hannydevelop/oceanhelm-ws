require("dotenv").config();

const http = require("node:http");
const express = require("express");

const receivers = require("./config/receivers");
const {
  SubscriptionManager,
  createWebSocketServer
} = require("./websocket");

const { AisGateway } = require("./ais/AisGateway");
const { AishubForwarder } = require("./aishub/AishubForwarder");

const app = express();
app.use(express.json({ limit: "1mb" }));

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 9000);
const STREAM_PATH = process.env.WS_PATH || "/ws/stream";

const server = http.createServer(app);

const subscriptions = new SubscriptionManager();

const gateway = new AisGateway({
  broadcaster: subscriptions
});

function createAishubForReceiver(config) {
  if (
    config.id !== "receiver-001" ||
    process.env.AISHUB_ENABLED !== "true" ||
    !process.env.AISHUB_HOST ||
    !process.env.AISHUB_PORT
  ) {
    return null;
  }

  console.log(
    `[AISHub] receiver-001 forwarding enabled to ${process.env.AISHUB_HOST}:${process.env.AISHUB_PORT}`
  );

  return new AishubForwarder({
    host: process.env.AISHUB_HOST,
    port: process.env.AISHUB_PORT
  });
}

for (const config of Object.values(receivers)) {
  config.aishubForwarder = createAishubForReceiver(config);
}

const wss = createWebSocketServer({
  server,
  streamPath: STREAM_PATH,
  legacyPath: "/",

  onAgentConnection: (_socket, remoteAddress) => {
    console.log(`Agent connected from ${remoteAddress}`);
  },

  onAgentMessage: (incoming, _socket, remoteAddress) => {
    /*
     * Legacy Go agent payload:
     *
     * {
     *   receiver_id: "...",
     *   timestamp: "...",
     *   nmea: "..."
     * }
     *
     * The receiver_id is used only internally to route the
     * incoming NMEA to the correct decoder.
     */

    const receiverId = incoming.receiver_id;

    if (!receiverId || !receivers[receiverId]) {
      console.warn(
        `[AIS] Unknown receiver from ${remoteAddress}: ${receiverId || "null"}`
      );
      return;
    }

    const config = receivers[receiverId];

    /*
     * The legacy WebSocket agent path is deliberately accepted here.
     * The actual NMEA processing is delegated to a per-receiver
     * pipeline so multipart state remains isolated.
     */
    let pipeline = gateway.pipelines[receiverId];

    if (!pipeline) {
      const dynamicConfig = {
        ...config,
        enabled: true,
        host: null,
        port: null,
        aishubForwarder: config.aishubForwarder || null
      };

      /*
       * A legacy Go agent sends NMEA through this WebSocket, so it
       * does not need an NMEA TCP connection. For that case create
       * an in-memory WebSocket receiver pipeline.
       */
      const { ReceiverPipeline } = require("./ais/ReceiverPipeline");
      const { normalizeAisMessage } = require("./ais/AisNormalizer");

      const { AisStateStore } = require("./ais/AisStateStore");

      if (!gateway._agentPipelines) {
        gateway._agentPipelines = new Map();
      }

      let agentPipeline = gateway._agentPipelines.get(receiverId);

      if (!agentPipeline) {
        const { AisDecoder } = require("./ais/AisDecoder");
        const { isGpsSentence } = require("./utils/isGpsSentence");
        const { isAisSentence } = require("./utils/isAisSentence");

        agentPipeline = {
          decoder: new AisDecoder(receiverId),

          handleSentence(sentence) {
            const value = String(sentence).trim();
            if (!value || isGpsSentence(value) || !isAisSentence(value)) {
              return;
            }

            if (config.aishubForwarder) {
              config.aishubForwarder.send(value);
            }

            this.decoder.write(value, (error, decoded) => {
              if (error) {
                console.error(
                  `[AIS decoder:${receiverId}] ${error.message}`
                );
                return;
              }

              if (!decoded) return;

              const normalized = normalizeAisMessage(decoded);
              const latest = gateway.state.update(normalized);

              subscriptions.broadcast(latest);
            });
          }
        };

        gateway._agentPipelines.set(receiverId, agentPipeline);
      }

      pipeline = agentPipeline;
    }

    if (incoming.nmea) {
      pipeline.handleSentence(incoming.nmea);
    }
  },

  onClientConnection: (socket, remoteAddress) => {
    console.log(`AIS stream client connected from ${remoteAddress}`);

    const subscription = subscriptions.add(socket);

    socket.send(JSON.stringify({
      type: "connection_ack",
      message: "OceanHelm AIS stream connected"
    }));

    socket.on("message", data => {
      try {
        const message = JSON.parse(data.toString());

        if (message.action === "subscribe") {
          subscription.update(message);

          socket.send(JSON.stringify({
            type: "subscription",
            status: "ok",
            bbox: subscription.bbox,
            message_types: subscription.messageTypes
              ? Array.from(subscription.messageTypes)
              : null
          }));

          /*
           * Send current state immediately after subscription.
           * This prevents the map from waiting for the next
           * transmission from every vessel.
           */
          for (const vessel of gateway.snapshot()) {
            if (subscription.accepts(vessel)) {
              subscription.send(vessel);
            }
          }

          return;
        }

        if (message.action === "unsubscribe") {
          subscription.bbox = null;
          subscription.messageTypes = null;

          socket.send(JSON.stringify({
            type: "subscription",
            status: "cleared"
          }));

          return;
        }

        socket.send(JSON.stringify({
          type: "error",
          message: "Unknown action"
        }));
      } catch (error) {
        socket.send(JSON.stringify({
          type: "error",
          message: "Invalid JSON"
        }));
      }
    });

    socket.on("close", () => {
      subscriptions.remove(subscription);
      console.log(`AIS stream client disconnected: ${remoteAddress}`);
    });

    socket.on("error", error => {
      subscriptions.remove(subscription);
      console.error(
        `AIS stream client error: ${error.message}`
      );
    });
  }
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "oceanhelm-ws",
    port: PORT,
    stream: STREAM_PATH,
    subscribers: subscriptions.count(),
    receivers: Object.fromEntries(
      Object.entries(receivers).map(([id, config]) => [
        id,
        {
          type: config.type,
          configured: Boolean(config.enabled)
        }
      ])
    )
  });
});

app.get("/health/ais", (_req, res) => {
  res.json({
    ok: true,
    vessels: gateway.snapshot().length,
    subscribers: subscriptions.count()
  });
});

gateway.startAll(receivers);

server.listen(PORT, HOST, () => {
  console.log(
    `OceanHelm WebSocket server listening on ${HOST}:${PORT}`
  );
  console.log(
    `Legacy agent endpoint: ws://HOST:${PORT}/`
  );
  console.log(
    `AIS client stream: ws://HOST:${PORT}${STREAM_PATH}`
  );
});

function shutdown(signal) {
  console.log(`Received ${signal}; shutting down...`);

  gateway.stopAll();

  for (const config of Object.values(receivers)) {
    config.aishubForwarder?.close();
  }

  wss.close(() => {
    server.close(() => {
      process.exit(0);
    });
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

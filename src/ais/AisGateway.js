const { ReceiverPipeline } = require("./ReceiverPipeline");
const { normalizeAisMessage } = require("./AisNormalizer");
const { AisStateStore } = require("./AisStateStore");

class AisGateway {
  constructor({ broadcaster, pipelines = {} }) {
    this.broadcaster = broadcaster;
    this.pipelines = pipelines;
    this.state = new AisStateStore();
  }

  startReceiver(config) {
    // Only receivers explicitly marked mode: "tcp" get an outbound
    // connection attempt. Agent-push receivers (mode: "agent") are
    // deliberately skipped here — they're wired up lazily in
    // server.js's onAgentMessage handler instead, keyed by
    // receiver_id, with no TCP dial involved.
    if (config?.mode !== "tcp") {
      console.log(`[AIS] ${config?.id || "unknown"} is agent-push; skipping TCP connect`);
      return null;
    }

    if (!config?.enabled || !config.host || !config.port) {
      console.log(`[AIS] ${config?.id || "unknown"} not configured; skipping`);
      return null;
    }

    const pipeline = new ReceiverPipeline({
      receiverId: config.id,
      host: config.host,
      port: config.port,
      aishubForwarder: config.aishubForwarder || null,

      onDecoded: decoded => {
        const normalized = normalizeAisMessage(decoded);

        // receiver_id deliberately does not enter the normalized public message.
        const latest = this.state.update(normalized);

        this.broadcaster.broadcast(latest);
      }
    });

    this.pipelines[config.id] = pipeline;
    pipeline.start();

    return pipeline;
  }

  startAll(receiverConfigs) {
    for (const config of Object.values(receiverConfigs)) {
      this.startReceiver(config);
    }
  }

  stopAll() {
    for (const pipeline of Object.values(this.pipelines)) {
      pipeline.stop();
    }
  }

  snapshot() {
    return this.state.snapshot();
  }
}

module.exports = { AisGateway };

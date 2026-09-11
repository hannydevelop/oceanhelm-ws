const net = require("node:net");
const { AisDecoder } = require("./AisDecoder");
const { isGpsSentence } = require("../utils/isGpsSentence");
const { isAisSentence } = require("../utils/isAisSentence");

class ReceiverPipeline {
  constructor({
    receiverId,
    host,
    port,
    onDecoded,
    aishubForwarder = null
  }) {
    this.receiverId = receiverId;
    this.host = host;
    this.port = Number(port);
    this.onDecoded = onDecoded;
    this.aishubForwarder = aishubForwarder;

    // One decoder per physical/external receiver.
    this.decoder = new AisDecoder(receiverId);

    this.socket = null;
    this.buffer = "";
    this.reconnectTimer = null;
    this.stopped = false;
  }

  start() {
    this.stopped = false;
    this.connect();
  }

  connect() {
    if (this.stopped) return;

    this.socket = net.createConnection(
      { host: this.host, port: this.port },
      () => {
        console.log(
          `[AIS] ${this.receiverId} connected to ${this.host}:${this.port}`
        );
      }
    );

    this.socket.setEncoding("utf8");

    this.socket.on("data", chunk => {
      this.handleData(chunk);
    });

    this.socket.on("error", error => {
      console.error(`[AIS] ${this.receiverId}: ${error.message}`);
    });

    this.socket.on("close", () => {
      console.log(`[AIS] ${this.receiverId} disconnected`);

      if (!this.stopped) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = setTimeout(() => this.connect(), 5000);
      }
    });
  }

  handleData(chunk) {
    this.buffer += chunk;

    const lines = this.buffer.split(/\r?\n/);
    this.buffer = lines.pop() || "";

    for (const line of lines) {
      this.handleSentence(line);
    }
  }

  handleSentence(sentence) {
    const value = String(sentence).trim();

    if (!value) return;

    // GPS is dropped immediately.
    if (isGpsSentence(value)) return;

    // Only AIS VDM/VDO enters the decoder.
    if (!isAisSentence(value)) return;

    // receiver-001 may forward raw AIS to AISHub.
    if (this.aishubForwarder) {
      this.aishubForwarder.send(value);
    }

    this.decoder.write(value, (error, decoded) => {
      if (error) {
        console.error(
          `[AIS decoder:${this.receiverId}] ${error.message}`
        );
        return;
      }

      if (!decoded) return;

      this.onDecoded(decoded);
    });
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);

    try {
      this.socket?.destroy();
    } catch (_) {}

    this.decoder.end();
  }
}

module.exports = { ReceiverPipeline };

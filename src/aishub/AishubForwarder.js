const dgram = require("node:dgram");

class AishubForwarder {
  constructor({ host, port }) {
    this.host = host;
    this.port = Number(port);
    this.socket = dgram.createSocket("udp4");
  }

  send(sentence) {
    if (!this.host || !this.port) return;

    const payload = Buffer.from(String(sentence).trim() + "\r\n");

    this.socket.send(payload, 0, payload.length, this.port, this.host, error => {
      if (error) {
        console.error(`[AISHub] ${error.message}`);
      }
    });
  }

  close() {
    try {
      this.socket.close();
    } catch (_) {}
  }
}

module.exports = { AishubForwarder };

const AisDecoderStream = require("ais-stream-decoder").default;

class AisDecoder {
  constructor(receiverId) {
    this.receiverId = receiverId;
    this.decoder = new AisDecoderStream();
    this.queue = [];

    this.decoder.on("data", message => {
      const callback = this.queue.shift();
      if (callback) callback(null, message);
    });

    this.decoder.on("error", error => {
      const callback = this.queue.shift();
      if (callback) callback(error);
      else console.error(`[AIS decoder:${this.receiverId}] ${error.message}`);
    });
  }

  write(sentence, callback) {
    this.queue.push(callback);
    this.decoder.write(String(sentence).trim() + "\r\n");
  }

  end() {
    if (typeof this.decoder.end === "function") {
      this.decoder.end();
    }
  }
}

module.exports = { AisDecoder };
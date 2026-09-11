const AisDecoderStream = require("ais-stream-decoder").default;

class AisDecoder {
  constructor(receiverId) {
    this.receiverId = receiverId;
    this.decoder = new AisDecoderStream();
    this.queue = [];

    this.decoder.on("data", message => {
      const callback = this.queue.shift();

      if (!callback) return;

      /*
       * ais-stream-decoder pushes each decoded message as a JSON
       * *string* (it's a plain, non-objectMode Transform stream),
       * not a parsed object. Every downstream consumer
       * (AisNormalizer, etc.) expects a real object — without this
       * parse, every field lookup silently returns undefined/null
       * and messages get dropped by subscription filters with no
       * visible error.
       */
      try {
        callback(null, JSON.parse(message));
      } catch (error) {
        callback(new Error(`Failed to parse decoder output: ${error.message}`));
      }
    });

    this.decoder.on("error", error => {
      const callback = this.queue.shift();
      if (callback) callback(error);
      else console.error(`[AIS decoder:${this.receiverId}] ${error.message}`);
    });
  }

  write(sentence, callback) {
    this.queue.push(callback);

    /*
     * IMPORTANT: do not append "\r\n" (or any trailing whitespace)
     * here. ais-stream-decoder's _transform() treats each write() as
     * one complete sentence (no internal line buffering), and its
     * checksum check reads everything after "*" verbatim as the
     * expected checksum. A trailing "\r\n" ends up appended to that
     * checksum string, so it can never match and every sentence
     * fails with "Invalid checksum" even when the NMEA is perfectly
     * valid.
     */
    this.decoder.write(String(sentence).trim());
  }

  end() {
    if (typeof this.decoder.end === "function") {
      this.decoder.end();
    }
  }
}

module.exports = { AisDecoder };
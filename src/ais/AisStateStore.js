class AisStateStore {
  constructor() {
    this.vessels = new Map();
  }

  update(message) {
    if (message.mmsi == null) return message;

    const key = String(message.mmsi);
    const previous = this.vessels.get(key) || {};

    const next = {
      ...previous,
      ...message,
      navigation: {
        ...(previous.navigation || {}),
        ...(message.navigation || {})
      },
      vessel: {
        ...(previous.vessel || {}),
        ...(message.vessel || {})
      },
      position: message.position || previous.position || null,
      timestamp: message.timestamp || previous.timestamp
    };

    this.vessels.set(key, next);
    return next;
  }

  snapshot() {
    return Array.from(this.vessels.values());
  }

  get(mmsi) {
    return this.vessels.get(String(mmsi)) || null;
  }
}

module.exports = { AisStateStore };

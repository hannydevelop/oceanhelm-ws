const { WebSocketServer } = require("ws");

function pointInsideBbox(position, bbox) {
  if (!position || !Array.isArray(bbox) || bbox.length !== 4) return false;

  const [west, south, east, north] = bbox;
  const lon = Number(position.lon);
  const lat = Number(position.lat);

  if (![west, south, east, north, lon, lat].every(Number.isFinite)) return false;

  const longitudeMatches =
    west <= east
      ? lon >= west && lon <= east
      : lon >= west || lon <= east;

  return longitudeMatches && lat >= south && lat <= north;
}

class Subscription {
  constructor(ws) {
    this.ws = ws;
    this.bbox = null;
    this.messageTypes = null;
  }

  update(payload) {
    if (Array.isArray(payload.bbox) && payload.bbox.length === 4) {
      const bbox = payload.bbox.map(Number);
      if (bbox.every(Number.isFinite)) this.bbox = bbox;
    } else if (payload.bbox === null) {
      this.bbox = null;
    }

    if (Array.isArray(payload.message_types)) {
      const types = payload.message_types.map(Number);
      this.messageTypes = new Set(types.filter(Number.isFinite));
    } else if (payload.message_types === null) {
      this.messageTypes = null;
    }
  }

  accepts(message) {
    if (
      this.messageTypes &&
      !this.messageTypes.has(Number(message.message_type))
    ) {
      return false;
    }

    if (this.bbox && !pointInsideBbox(message.position, this.bbox)) {
      return false;
    }

    return true;
  }

  send(message) {
    if (this.ws.readyState !== 1) return;
    this.ws.send(JSON.stringify(message));
  }
}

class SubscriptionManager {
  constructor() {
    this.subscriptions = new Set();
  }

  add(ws) {
    const subscription = new Subscription(ws);
    this.subscriptions.add(subscription);
    return subscription;
  }

  remove(subscription) {
    this.subscriptions.delete(subscription);
  }

  broadcast(message) {
    for (const subscription of this.subscriptions) {
      if (subscription.accepts(message)) {
        subscription.send(message);
      }
    }
  }

  count() {
    return this.subscriptions.size;
  }
}

function createWebSocketServer({
  server,
  streamPath = "/ws/stream",
  legacyPath = "/",
  onAgentMessage,
  onClientConnection,
  onAgentConnection
}) {
  const wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (request, socket, head) => {
    const url = new URL(request.url, `http://${request.headers.host}`);
    const pathname = url.pathname;

    if (pathname !== streamPath && pathname !== legacyPath) {
      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, ws => {
      wss.emit("connection", ws, request, pathname);
    });
  });

  wss.on("connection", (socket, request, pathname) => {
    const remoteAddress = request.socket.remoteAddress;

    if (pathname === streamPath) {
      onClientConnection?.(socket, remoteAddress);
      return;
    }

    onAgentConnection?.(socket, remoteAddress);

    socket.send(JSON.stringify({
      type: "connection_ack",
      message: "OceanHelm WebSocket connection established"
    }));

    socket.on("message", data => {
      try {
        const incoming = JSON.parse(data.toString());
        onAgentMessage?.(incoming, socket, remoteAddress);
      } catch (error) {
        console.error("Invalid WebSocket message:", error.message);
      }
    });

    socket.on("close", () => {
      console.log(`Agent disconnected: ${remoteAddress}`);
    });

    socket.on("error", error => {
      console.error(`WebSocket error: ${error.message}`);
    });
  });

  return wss;
}

module.exports = {
  Subscription,
  SubscriptionManager,
  createWebSocketServer
};

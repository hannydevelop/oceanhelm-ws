/*
 * Every receiver here needs a `mode`:
 *
 *   "agent" — the receiver has NO reachable NMEA TCP server of its
 *     own. It only ever reaches us by pushing NMEA through the
 *     legacy Go-agent WebSocket connection, keyed by receiver_id.
 *     AisGateway never dials out for these — server.js builds an
 *     in-memory decode pipeline for them lazily, on first message.
 *     host/port are irrelevant and ignored for this mode.
 *
 *   "tcp" — the receiver exposes its own NMEA TCP server and this
 *     app is expected to connect OUT to host:port to pull the feed.
 *
 * Get a receiver's mode wrong (say "tcp" for something that's
 * actually agent-push) and you'll see this at startup for every
 * request cycle:
 *
 *   [AIS] <id>: connect ECONNREFUSED <host>:<port>
 *
 * because nothing is listening on the other end. If that happens,
 * the fix is almost never a firewall/port issue — it's that the
 * receiver was never a TCP source in the first place. Set its mode
 * back to "agent" instead of chasing the network error.
 */
const receivers = {
  "receiver-001": {
    id: "receiver-001",
    type: "external",
    mode: "tcp", // real external/AISHub exchange feed — dials out once configured
    enabled: Boolean(process.env.RECEIVER_001_HOST && process.env.RECEIVER_001_PORT),
    host: process.env.RECEIVER_001_HOST || null,
    port: Number(process.env.RECEIVER_001_PORT || 0),
    aishub: true
  },

  // receiver-002 .. receiver-005, and the receiver formerly
  // configured via AIS_001_HOST/AIS_001_PORT (port 8118), are all
  // physical/local receivers that only push NMEA to us through the
  // Go agent's WebSocket connection. None of them run a TCP server
  // we can dial into, so `mode` is hardcoded to "agent" and AisGateway
  // will never attempt to connect out to them.

  "receiver-002": {
    id: "receiver-002",
    type: "local",
    mode: "agent",
    enabled: false,
    host: null,
    port: null,
    aishub: false
  },

  "receiver-003": {
    id: "receiver-003",
    type: "local",
    mode: "agent",
    enabled: false,
    host: null,
    port: null,
    aishub: false
  },

  "receiver-004": {
    id: "receiver-004",
    type: "local",
    mode: "agent",
    enabled: false,
    host: null,
    port: null,
    aishub: false
  },

  "receiver-005": {
    id: "receiver-005",
    type: "local",
    mode: "agent",
    enabled: false,
    host: null,
    port: null,
    aishub: false
  }
};

module.exports = receivers;

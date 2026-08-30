import http from "http";
import dgram from "dgram";
import WebSocket, { WebSocketServer } from "ws";
import aisDecoderPkg from "ais-stream-decoder";

// -----------------------------------------------------------------------------
// AIS decoder
// -----------------------------------------------------------------------------

const AisDecoder = aisDecoderPkg.default ?? aisDecoderPkg;

// -----------------------------------------------------------------------------
// CONFIGURATION
// -----------------------------------------------------------------------------

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 9000);

// -----------------------------------------------------------------------------
// Python authentication
//
// Optional.
//
// If PYTHON_STREAM_TOKEN is set, Python must connect with:
//
// ws://SERVER:9000/ws/python?token=YOUR_TOKEN
// -----------------------------------------------------------------------------

const PYTHON_STREAM_TOKEN =
  process.env.PYTHON_STREAM_TOKEN || null;

// -----------------------------------------------------------------------------
// Receiver authentication
//
// Optional.
//
// If RECEIVER_TOKEN is set, receivers must connect with:
//
// ws://SERVER:9000/ws/receiver?token=YOUR_TOKEN
//
// You can leave this empty during initial testing.
// -----------------------------------------------------------------------------

const RECEIVER_TOKEN =
  process.env.RECEIVER_TOKEN || null;

// -----------------------------------------------------------------------------
// Public stream authentication
//
// Optional for now.
//
// Example:
//
// STREAM_API_KEYS=key1,key2,key3
//
// If empty, /ws/stream is open.
//
// For production, we should enable this.
// -----------------------------------------------------------------------------

const STREAM_API_KEYS = new Set(
  (process.env.STREAM_API_KEYS || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
);

// -----------------------------------------------------------------------------
// Receiver configuration
// -----------------------------------------------------------------------------

const RECEIVERS = {
  "receiver-001": {
    externalFeed: true,
    python: false,
    publicStream: false,
  },

  "receiver-002": {
    externalFeed: false,
    python: true,
    publicStream: true,
  },

  "receiver-003": {
    externalFeed: false,
    python: true,
    publicStream: true,
  },

  "receiver-004": {
    externalFeed: false,
    python: true,
    publicStream: true,
  },

  "receiver-005": {
    externalFeed: false,
    python: true,
    publicStream: true,
  },
};

// =============================================================================
// AISHUB CONFIGURATION
// =============================================================================
//
// AISHub provides the destination host/IP and dedicated UDP port after joining.
//
// Example:
//
// AISHUB_HOST=data.aishub.net
// AISHUB_PORT=12345
//
// We do not invent the port here. Use the one AISHub gives OceanHelm.
//
// AISHub requires raw NMEA.
// -----------------------------------------------------------------------------

const AISHUB_HOST =
  process.env.AISHUB_HOST || null;

const AISHUB_PORT = process.env.AISHUB_PORT
  ? Number(process.env.AISHUB_PORT)
  : null;

// =============================================================================
// HTTP SERVER
// =============================================================================

const server = http.createServer(
  (request, response) => {
    if (request.url === "/health") {
      response.writeHead(200, {
        "Content-Type": "application/json",
      });

      response.end(
        JSON.stringify({
          status: "ok",
          service: "oceanhelm-ais",
          timestamp: new Date().toISOString(),
        })
      );

      return;
    }

    response.writeHead(404);
    response.end("Not Found");
  }
);

// =============================================================================
// WEBSOCKET SERVERS
// =============================================================================
//
// /ws/receiver
//
// OceanHelm AIS agents connect here.
//
// /ws/python
//
// Our Python processing service connects here.
//
// /ws/stream
//
// Future OceanHelm AISStream-style clients connect here.
//
// =============================================================================

const receiverWSS =
  new WebSocketServer({
    noServer: true,
  });

const pythonWSS =
  new WebSocketServer({
    noServer: true,
  });

const streamWSS =
  new WebSocketServer({
    noServer: true,
  });

// =============================================================================
// CONNECTION STATE
// =============================================================================

// receiver_id -> WebSocket
const connectedReceivers = new Map();

// Python consumers
const pythonClients = new Set();

// Public stream clients
const streamClients = new Set();

// =============================================================================
// AIS DECODERS
// =============================================================================
//
// IMPORTANT:
//
// We use ONE decoder PER RECEIVER.
//
// AIS multipart messages can be interleaved.
//
// Example:
//
// receiver-002:
//   !AIVDM,2,1,...

// receiver-003:
//   !AIVDM,1,1,...

// receiver-002:
//   !AIVDM,2,2,...
//
// If we used one global decoder, multipart state from different receivers
// could potentially interfere.
//
// -----------------------------------------------------------------------------

const receiverDecoders = new Map();

// -----------------------------------------------------------------------------
// Pending raw AIS records per receiver.
//
// The decoder emits "data" asynchronously.
//
// We keep the original record so that the decoded message can retain:
//
// timestamp
// receiver source internally
// nmea
//
// -----------------------------------------------------------------------------

const pendingAISRecords = new Map();

// =============================================================================
// STATISTICS
// =============================================================================

const stats = {
  totalMessages: 0,

  aisMessages: 0,
  gpsMessages: 0,
  invalidMessages: 0,

  aisDecoded: 0,
  aisDecodeErrors: 0,

  pythonMessages: 0,
  externalMessages: 0,
  streamMessages: 0,
};

// =============================================================================
// NMEA CLASSIFICATION
// =============================================================================

function isAIS(nmea) {
  if (!nmea || typeof nmea !== "string") {
    return false;
  }

  const sentence = nmea.trim();

  return (
    sentence.startsWith("!AIVDM") ||
    sentence.startsWith("!AIVDO")
  );
}

function isGPS(nmea) {
  if (!nmea || typeof nmea !== "string") {
    return false;
  }

  return nmea.trim().startsWith("$");
}

// =============================================================================
// RECEIVER DECODER INITIALIZATION
// =============================================================================

function getReceiverDecoder(receiverId) {
  if (receiverDecoders.has(receiverId)) {
    return receiverDecoders.get(receiverId);
  }

  const decoder = new AisDecoder();

  receiverDecoders.set(
    receiverId,
    decoder
  );

  pendingAISRecords.set(
    receiverId,
    []
  );

  decoder.on(
    "data",
    (decodedMessage) => {
      handleDecodedAIS(
        receiverId,
        decodedMessage
      );
    }
  );

  decoder.on(
    "error",
    (error) => {
      stats.aisDecodeErrors++;

      console.error(
        `[AIS decoder:${receiverId}] ${error.message}`
      );

      // IMPORTANT:
      //
      // Do NOT terminate the receiver connection.
      //
      // A malformed AIS message should not kill the stream.
    }
  );

  return decoder;
}

// =============================================================================
// CREATE RAW AIS RECORD
// =============================================================================

function createRawAISRecord(
  receiverId,
  timestamp,
  nmea
) {
  return {
    receiver_id: receiverId,

    timestamp:
      timestamp ||
      new Date().toISOString(),

    nmea,

    received_at:
      new Date().toISOString(),
  };
}

// =============================================================================
// AIS MESSAGE TYPE
// =============================================================================

function getAISMessageType(type) {
  switch (Number(type)) {
    case 1:
      return "PositionReport";

    case 2:
      return "PositionReport";

    case 3:
      return "PositionReport";

    case 4:
      return "BaseStationReport";

    case 5:
      return "ShipStaticData";

    case 6:
      return "AddressedBinaryMessage";

    case 7:
      return "BinaryAcknowledgement";

    case 8:
      return "BinaryBroadcastMessage";

    case 9:
      return "StandardSARAircraftPositionReport";

    case 10:
      return "UTCDateResponse";

    case 11:
      return "UTCDateResponse";

    case 12:
      return "AddressedSafetyRelatedMessage";

    case 13:
      return "SafetyRelatedAcknowledgement";

    case 14:
      return "SafetyRelatedBroadcastMessage";

    case 15:
      return "Interrogation";

    case 16:
      return "AssignmentModeCommand";

    case 17:
      return "GNSSBroadcastBinaryMessage";

    case 18:
      return "StandardClassBPositionReport";

    case 19:
      return "ExtendedClassBPositionReport";

    case 20:
      return "DataLinkManagementMessage";

    case 21:
      return "AidToNavigationReport";

    case 22:
      return "ChannelManagement";

    case 23:
      return "GroupAssignmentCommand";

    case 24:
      return "StaticDataReport";

    case 25:
      return "SingleSlotBinaryMessage";

    case 26:
      return "MultipleSlotBinaryMessage";

    case 27:
      return "LongRangeAISBroadcastMessage";

    default:
      return "Unknown";
  }
}

// =============================================================================
// GET MMSI
// =============================================================================

function getMMSI(decoded) {
  return (
    decoded?.mmsi ??
    decoded?.MMSI ??
    decoded?.userId ??
    decoded?.UserID ??
    null
  );
}

// =============================================================================
// GET LAT/LON
// =============================================================================

function getLatitude(decoded) {
  return (
    decoded?.lat ??
    decoded?.latitude ??
    decoded?.Latitude ??
    null
  );
}

function getLongitude(decoded) {
  return (
    decoded?.lon ??
    decoded?.longitude ??
    decoded?.Longitude ??
    null
  );
}

// =============================================================================
// BUILD PUBLIC AIS EVENT
// =============================================================================
//
// This is the structure customers receive.
//
// We deliberately do NOT include:
//
// receiver_id
//
// received_at
//
// station location
//
// internal infrastructure metadata
//
// -----------------------------------------------------------------------------

function buildPublicAISMessage(
  record,
  decodedMessage
) {
  const numericType =
    Number(
      decodedMessage?.type ??
      decodedMessage?.messageType ??
      decodedMessage?.msgType
    );

  const messageType =
    getAISMessageType(
      numericType
    );

  const mmsi =
    getMMSI(decodedMessage);

  const latitude =
    getLatitude(decodedMessage);

  const longitude =
    getLongitude(decodedMessage);

  return {
    type: "ais",

    source: "oceanhelm",

    timestamp:
      record.timestamp,

    message_type:
      messageType,

    ais_message_type:
      numericType || null,

    mmsi,

    latitude,

    longitude,

    ais: decodedMessage,
  };
}

// =============================================================================
// HANDLE DECODED AIS
// =============================================================================

function handleDecodedAIS(
  receiverId,
  decodedMessage
) {
  stats.aisDecoded++;

  const pending =
    pendingAISRecords.get(
      receiverId
    );

  if (!pending || pending.length === 0) {
    console.warn(
      `Decoded AIS without pending record: ${receiverId}`
    );

    return;
  }

  const record =
    pending.shift();

  const receiverConfig =
    RECEIVERS[receiverId];

  if (!receiverConfig) {
    return;
  }

  // ---------------------------------------------------------------------------
  // Public stream
  // ---------------------------------------------------------------------------
  //
  // Only receivers explicitly marked publicStream:true are included.
  //
  // receiver-001 is NOT included.
  //
  // ---------------------------------------------------------------------------

  if (
    receiverConfig.publicStream
  ) {
    const publicMessage =
      buildPublicAISMessage(
        record,
        decodedMessage
      );

    broadcastToStreamClients(
      publicMessage
    );
  }

  // ---------------------------------------------------------------------------
  // Python
  // ---------------------------------------------------------------------------
  //
  // Python receives RAW NMEA, not the decoded object.
  //
  // Python remains responsible for OceanHelm's processing/data layer.
  //
  // ---------------------------------------------------------------------------

  if (receiverConfig.python) {
    sendRawToPython(record);
  }
}

// =============================================================================
// PYTHON RAW STREAM
// =============================================================================

function sendRawToPython(record) {
  if (pythonClients.size === 0) {
    return;
  }

  const message =
    JSON.stringify(record);

  for (
    const client of pythonClients
  ) {
    if (
      client.readyState !==
      WebSocket.OPEN
    ) {
      continue;
    }

    try {
      client.send(message);

      stats.pythonMessages++;
    } catch (error) {
      console.error(
        "Python stream send error:",
        error.message
      );
    }
  }
}

// =============================================================================
// PUBLIC STREAM
// =============================================================================
//
// AISStream-style decoded stream.
//
// Clients receive structured AIS JSON.
//
// They never receive:
//
// receiver_id
// receiver GPS
// station coordinates
// internal metadata
//
// =============================================================================

function broadcastToStreamClients(
  message
) {
  if (streamClients.size === 0) {
    return;
  }

  for (
    const client of streamClients
  ) {
    if (
      client.readyState !==
      WebSocket.OPEN
    ) {
      continue;
    }

    if (
      !matchesSubscription(
        message,
        client.subscription
      )
    ) {
      continue;
    }

    try {
      client.send(
        JSON.stringify(message)
      );

      stats.streamMessages++;
    } catch (error) {
      console.error(
        "Stream broadcast error:",
        error.message
      );
    }
  }
}

// =============================================================================
// SUBSCRIPTION FILTERING
// =============================================================================

function matchesSubscription(
  message,
  subscription
) {
  if (!subscription) {
    return true;
  }

  // ---------------------------------------------------------------------------
  // Message type filter
  // ---------------------------------------------------------------------------

  if (
    Array.isArray(
      subscription.message_types
    ) &&
    subscription.message_types.length > 0
  ) {
    if (
      !subscription.message_types.includes(
        message.message_type
      )
    ) {
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // MMSI filter
  // ---------------------------------------------------------------------------

  if (
    Array.isArray(
      subscription.mmsi
    ) &&
    subscription.mmsi.length > 0
  ) {
    if (
      !subscription.mmsi.includes(
        Number(message.mmsi)
      )
    ) {
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Bounding box
  // ---------------------------------------------------------------------------
  //
  // Format:
  //
  // bbox: {
  //   minLat: 4,
  //   maxLat: 7,
  //   minLon: 3,
  //   maxLon: 8
  // }
  //
  // ---------------------------------------------------------------------------

  if (subscription.bbox) {
    const {
      minLat,
      maxLat,
      minLon,
      maxLon,
    } = subscription.bbox;

    const lat =
      Number(message.latitude);

    const lon =
      Number(message.longitude);

    if (
      !Number.isFinite(lat) ||
      !Number.isFinite(lon)
    ) {
      return false;
    }

    if (
      lat < Number(minLat) ||
      lat > Number(maxLat) ||
      lon < Number(minLon) ||
      lon > Number(maxLon)
    ) {
      return false;
    }
  }

  return true;
}

// =============================================================================
// EXTERNAL FEED QUEUE
// =============================================================================
//
// receiver-001 is the exchange receiver.
//
// We keep a small in-memory queue so a temporary AISHub/network issue doesn't
// immediately block receiver ingestion.
//
// This is NOT a permanent archive.
//
// =============================================================================

const externalFeedQueue = [];

const MAX_EXTERNAL_QUEUE = 10000;

let externalFeedProcessing = false;

// =============================================================================
// QUEUE EXTERNAL FEED
// =============================================================================

function queueExternalFeed(
  record
) {
  if (
    externalFeedQueue.length >=
    MAX_EXTERNAL_QUEUE
  ) {
    console.error(
      "External feed queue full. Dropping oldest message."
    );

    externalFeedQueue.shift();
  }

  externalFeedQueue.push(record);

  processExternalFeedQueue();
}

// =============================================================================
// PROCESS EXTERNAL FEED QUEUE
// =============================================================================

async function processExternalFeedQueue() {
  if (externalFeedProcessing) {
    return;
  }

  externalFeedProcessing = true;

  try {
    while (
      externalFeedQueue.length > 0
    ) {
      const record =
        externalFeedQueue[0];

      try {
        await sendToExternalFeeds(
          record
        );

        externalFeedQueue.shift();

        stats.externalMessages++;
      } catch (error) {
        console.error(
          "External feed error:",
          error.message
        );

        // Leave the message in the queue.
        //
        // Retry after a short delay.
        await sleep(3000);

        break;
      }
    }
  } finally {
    externalFeedProcessing = false;
  }

  if (
    externalFeedQueue.length > 0
  ) {
    setTimeout(
      processExternalFeedQueue,
      3000
    );
  }
}

// =============================================================================
// SEND TO EXTERNAL FEEDS
// =============================================================================
//
// At the moment receiver-001 goes to AISHub.
//
// AIS Friends can be added as another adapter.
//
// =============================================================================

async function sendToExternalFeeds(
  record
) {
  await sendToAISHub(
    record.nmea
  );

  // ---------------------------------------------------------------------------
  // AIS Friends
  // ---------------------------------------------------------------------------
  //
  // Add the AIS Friends transport here once their exact contributor protocol
  // and destination details are configured.
  //
  // Example:
  //
  // await sendToAISFriends(record.nmea);
  //
  // ---------------------------------------------------------------------------
}

// =============================================================================
// AISHUB UDP
// =============================================================================

const aishubSocket =
  dgram.createSocket("udp4");

function sendToAISHub(nmea) {
  // ---------------------------------------------------------------------------
  // If AISHub isn't configured, don't fail the whole server.
  //
  // This allows us to run the server while waiting for AISHub's destination.
  // ---------------------------------------------------------------------------

  if (
    !AISHUB_HOST ||
    !AISHUB_PORT
  ) {
    return Promise.resolve();
  }

  const payload =
    Buffer.from(
      `${nmea}\r\n`,
      "utf8"
    );

  return new Promise(
    (resolve, reject) => {
      aishubSocket.send(
        payload,
        0,
        payload.length,
        AISHUB_PORT,
        AISHUB_HOST,
        (error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        }
      );
    }
  );
}

// =============================================================================
// RECEIVER MESSAGE PROCESSING
// =============================================================================

function processReceiverMessage(
  incoming
) {
  // ---------------------------------------------------------------------------
  // Receiver ID
  // ---------------------------------------------------------------------------

  if (
    !incoming ||
    !incoming.receiver_id
  ) {
    return;
  }

  const receiverId =
    String(
      incoming.receiver_id
    );

  const receiverConfig =
    RECEIVERS[receiverId];

  if (!receiverConfig) {
    console.warn(
      `Unknown receiver: ${receiverId}`
    );

    return;
  }

  // ---------------------------------------------------------------------------
  // NMEA
  // ---------------------------------------------------------------------------

  if (
    typeof incoming.nmea !==
    "string"
  ) {
    return;
  }

  const nmea =
    incoming.nmea.trim();

  if (!nmea) {
    return;
  }

  stats.totalMessages++;

  // ===========================================================================
  // GPS / NON-AIS
  // ===========================================================================
  //
  // IMPORTANT:
  //
  // GPS is discarded here.
  //
  // It does NOT:
  //
  //   - go to Python
  //   - go to AISHub
  //   - go to AIS Friends
  //   - go to public clients
  //
  // ===========================================================================

  if (!isAIS(nmea)) {
    if (isGPS(nmea)) {
      stats.gpsMessages++;
    }

    return;
  }

  stats.aisMessages++;

  // ---------------------------------------------------------------------------
  // Raw record
  // ---------------------------------------------------------------------------

  const record =
    createRawAISRecord(
      receiverId,
      incoming.timestamp,
      nmea
    );

  // ===========================================================================
  // RECEIVER 001
  // ===========================================================================
  //
  // External exchange only.
  //
  // It does NOT go to Python.
  //
  // It does NOT go to OceanHelm's public stream.
  //
  // It does NOT get permanently archived.
  //
  // ===========================================================================

  if (
    receiverConfig.externalFeed
  ) {
    queueExternalFeed(
      record
    );

    return;
  }

  // ===========================================================================
  // RECEIVERS 002-005
  // ===========================================================================
  //
  // These enter the OceanHelm processing pipeline.
  //
  // ===========================================================================

  const decoder =
    getReceiverDecoder(
      receiverId
    );

  const pending =
    pendingAISRecords.get(
      receiverId
    );

  pending.push(record);

  try {
    decoder.write(nmea);
  } catch (error) {
    stats.aisDecodeErrors++;

    console.error(
      `AIS write error (${receiverId}):`,
      error.message
    );

    // -------------------------------------------------------------------------
    // IMPORTANT:
    //
    // Do not kill the receiver.
    // Remove the failed record and continue.
    // -------------------------------------------------------------------------

    pending.pop();
  }
}

// =============================================================================
// RECEIVER WEBSOCKET
// =============================================================================

receiverWSS.on(
  "connection",
  (socket, request) => {
    const remoteAddress =
      request.socket.remoteAddress;

    let receiverId = null;

    console.log(
      `Receiver connected: ${remoteAddress}`
    );

    socket.send(
      JSON.stringify({
        type: "connection_ack",
        message:
          "OceanHelm AIS receiver connection established",
      })
    );

    socket.on(
      "message",
      (data) => {
        try {
          const incoming =
            JSON.parse(
              data.toString()
            );

          // ---------------------------------------------------------------
          // Identify receiver
          // ---------------------------------------------------------------

          if (
            incoming.receiver_id &&
            RECEIVERS[
              incoming.receiver_id
            ]
          ) {
            receiverId =
              String(
                incoming.receiver_id
              );

            const existing =
              connectedReceivers.get(
                receiverId
              );

            if (
              existing &&
              existing !== socket
            ) {
              console.warn(
                `Receiver ${receiverId} already connected.`
              );

              // Do not immediately kill the existing connection.
              // The latest connection becomes the active connection.
            }

            connectedReceivers.set(
              receiverId,
              socket
            );

            console.log(
              `Receiver identified: ${receiverId}`
            );
          }

          // ---------------------------------------------------------------
          // Process message
          // ---------------------------------------------------------------

          processReceiverMessage(
            incoming
          );
        } catch (error) {
          stats.invalidMessages++;

          console.error(
            "Invalid receiver message:",
            error.message
          );
        }
      }
    );

    socket.on(
      "close",
      () => {
        if (
          receiverId &&
          connectedReceivers.get(
            receiverId
          ) === socket
        ) {
          connectedReceivers.delete(
            receiverId
          );
        }

        console.log(
          `Receiver disconnected: ${
            receiverId ||
            remoteAddress
          }`
        );
      }
    );

    socket.on(
      "error",
      (error) => {
        console.error(
          `Receiver WebSocket error: ${error.message}`
        );
      }
    );
  }
);

// =============================================================================
// PYTHON WEBSOCKET
// =============================================================================
//
// Python connects here:
//
// ws://SERVER:9000/ws/python?token=...
//
// It receives raw AIS NMEA from receivers 002-005.
//
// =============================================================================

pythonWSS.on(
  "connection",
  (socket, request) => {
    const remoteAddress =
      request.socket.remoteAddress;

    console.log(
      `Python processor connected: ${remoteAddress}`
    );

    pythonClients.add(socket);

    socket.send(
      JSON.stringify({
        type: "connection_ack",
        message:
          "OceanHelm raw AIS processing stream connected",
      })
    );

    socket.on(
      "close",
      () => {
        pythonClients.delete(socket);

        console.log(
          "Python processor disconnected"
        );
      }
    );

    socket.on(
      "error",
      (error) => {
        console.error(
          `Python WebSocket error: ${error.message}`
        );
      }
    );
  }
);

// =============================================================================
// PUBLIC STREAM WEBSOCKET
// =============================================================================
//
// Client connects:
//
// wss://stream.oceanhelmtech.com/ws/stream
//
// Then sends:
//
// {
//   "action": "subscribe",
//   "bbox": {
//     "minLat": 4,
//     "maxLat": 7,
//     "minLon": 3,
//     "maxLon": 8
//   },
//   "message_types": [
//     "PositionReport"
//   ]
// }
//
// =============================================================================

streamWSS.on(
  "connection",
  (socket, request) => {
    const remoteAddress =
      request.socket.remoteAddress;

    socket.subscription = null;

    console.log(
      `AIS stream client connected: ${remoteAddress}`
    );

    socket.send(
      JSON.stringify({
        type: "connection_ack",
        message:
          "OceanHelm AIS stream connected",
      })
    );

    socket.send(
      JSON.stringify({
        type: "subscription_status",
        status: "not_subscribed",
      })
    );

    // -------------------------------------------------------------------------
    // Client messages
    // -------------------------------------------------------------------------

    socket.on(
      "message",
      (data) => {
        try {
          const message =
            JSON.parse(
              data.toString()
            );

          // ---------------------------------------------------------------
          // Subscribe
          // ---------------------------------------------------------------

          if (
            message.action ===
            "subscribe"
          ) {
            socket.subscription = {
              bbox:
                message.bbox ||
                null,

              message_types:
                Array.isArray(
                  message.message_types
                )
                  ? message.message_types
                  : [],

              mmsi:
                Array.isArray(
                  message.mmsi
                )
                  ? message.mmsi.map(
                      Number
                    )
                  : [],
            };

            socket.send(
              JSON.stringify({
                type:
                  "subscription_status",

                status:
                  "subscribed",

                subscription:
                  socket.subscription,
              })
            );

            return;
          }

          // ---------------------------------------------------------------
          // Unsubscribe
          // ---------------------------------------------------------------

          if (
            message.action ===
            "unsubscribe"
          ) {
            socket.subscription =
              null;

            socket.send(
              JSON.stringify({
                type:
                  "subscription_status",

                status:
                  "unsubscribed",
              })
            );

            return;
          }

          // ---------------------------------------------------------------
          // Ping
          // ---------------------------------------------------------------

          if (
            message.action ===
            "ping"
          ) {
            socket.send(
              JSON.stringify({
                type: "pong",
                timestamp:
                  new Date().toISOString(),
              })
            );

            return;
          }
        } catch (error) {
          socket.send(
            JSON.stringify({
              type: "error",
              message:
                "Invalid stream message",
            })
          );
        }
      }
    );

    socket.on(
      "close",
      () => {
        streamClients.delete(
          socket
        );

        console.log(
          `AIS stream client disconnected: ${remoteAddress}`
        );
      }
    );

    socket.on(
      "error",
      (error) => {
        console.error(
          `AIS stream client error: ${error.message}`
        );
      }
    );

    streamClients.add(socket);
  }
);

// =============================================================================
// WEBSOCKET AUTHENTICATION
// =============================================================================

function getQuery(request) {
  return new URL(
    request.url,
    `http://${request.headers.host}`
  ).searchParams;
}

function authorizeReceiver(request) {
  if (!RECEIVER_TOKEN) {
    return true;
  }

  const query =
    getQuery(request);

  return (
    query.get("token") ===
    RECEIVER_TOKEN
  );
}

function authorizePython(request) {
  if (!PYTHON_STREAM_TOKEN) {
    return true;
  }

  const query =
    getQuery(request);

  return (
    query.get("token") ===
    PYTHON_STREAM_TOKEN
  );
}

function authorizeStream(request) {
  if (
    STREAM_API_KEYS.size === 0
  ) {
    return true;
  }

  const query =
    getQuery(request);

  const token =
    query.get("api_key") ||
    query.get("token");

  return (
    token &&
    STREAM_API_KEYS.has(token)
  );
}

// =============================================================================
// HTTP UPGRADE ROUTER
// =============================================================================
//
// One HTTP server can route different WebSocket paths to separate WSS
// instances using ws's noServer/handleUpgrade pattern.
// =============================================================================

server.on(
  "upgrade",
  (request, socket, head) => {
    const pathname =
      new URL(
        request.url,
        `http://${request.headers.host}`
      ).pathname;

    // -------------------------------------------------------------------------
    // RECEIVERS
    // -------------------------------------------------------------------------

    if (
      pathname ===
      "/ws/receiver"
    ) {
      if (
        !authorizeReceiver(
          request
        )
      ) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      receiverWSS.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {
          receiverWSS.emit(
            "connection",
            ws,
            request
          );
        }
      );

      return;
    }

    // -------------------------------------------------------------------------
    // PYTHON
    // -------------------------------------------------------------------------

    if (
      pathname ===
      "/ws/python"
    ) {
      if (
        !authorizePython(
          request
        )
      ) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      pythonWSS.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {
          pythonWSS.emit(
            "connection",
            ws,
            request
          );
        }
      );

      return;
    }

    // -------------------------------------------------------------------------
    // PUBLIC AIS STREAM
    // -------------------------------------------------------------------------

    if (
      pathname ===
      "/ws/stream"
    ) {
      if (
        !authorizeStream(
          request
        )
      ) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      streamWSS.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {
          streamWSS.emit(
            "connection",
            ws,
            request
          );
        }
      );

      return;
    }

    // -------------------------------------------------------------------------
    // Unknown path
    // -------------------------------------------------------------------------

    socket.destroy();
  }
);

// =============================================================================
// HEARTBEAT
// =============================================================================
//
// Detect dead WebSocket connections.
//
// =============================================================================

function heartbeat() {
  this.isAlive = true;
}

function setupHeartbeat(wss) {
  wss.on(
    "connection",
    (socket) => {
      socket.isAlive = true;

      socket.on(
        "pong",
        heartbeat
      );
    }
  );

  const interval =
    setInterval(() => {
      for (
        const socket
        of wss.clients
      ) {
        if (
          socket.isAlive === false
        ) {
          socket.terminate();

          continue;
        }

        socket.isAlive = false;

        socket.ping();
      }
    }, 30000);

  wss.on(
    "close",
    () => {
      clearInterval(interval);
    }
  );
}

setupHeartbeat(
  receiverWSS
);

setupHeartbeat(
  pythonWSS
);

setupHeartbeat(
  streamWSS
);

// =============================================================================
// SERVER START
// =============================================================================

server.listen(
  PORT,
  HOST,
  () => {
    console.log("");
    console.log(
      "=============================================="
    );
    console.log(
      "       OceanHelm AIS Gateway"
    );
    console.log(
      "=============================================="
    );

    console.log(
      `Listening: ${HOST}:${PORT}`
    );

    console.log("");

    console.log(
      "Receiver endpoint:"
    );

    console.log(
      `  ws://SERVER:${PORT}/ws/receiver`
    );

    console.log("");

    console.log(
      "Python endpoint:"
    );

    console.log(
      `  ws://SERVER:${PORT}/ws/python`
    );

    console.log("");

    console.log(
      "Public AIS stream:"
    );

    console.log(
      `  ws://SERVER:${PORT}/ws/stream`
    );

    console.log("");

    console.log(
      "Health:"
    );

    console.log(
      `  http://SERVER:${PORT}/health`
    );

    console.log("");

    console.log(
      "Receiver configuration:"
    );

    for (
      const [receiverId, config]
      of Object.entries(
        RECEIVERS
      )
    ) {
      console.log(
        `  ${receiverId}: ` +
        `external=${config.externalFeed}, ` +
        `python=${config.python}, ` +
        `stream=${config.publicStream}`
      );
    }

    console.log("");

    if (
      AISHUB_HOST &&
      AISHUB_PORT
    ) {
      console.log(
        `AISHub: ${AISHUB_HOST}:${AISHUB_PORT}`
      );
    } else {
      console.log(
        "AISHub: not configured"
      );
    }

    console.log("");

    console.log(
      `Connected receivers: ${connectedReceivers.size}`
    );

    console.log(
      `Python clients: ${pythonClients.size}`
    );

    console.log(
      `Stream clients: ${streamClients.size}`
    );

    console.log("");
  }
);

// =============================================================================
// PERIODIC STATUS
// =============================================================================

setInterval(() => {
  console.log(
    `[STATUS] ` +
    `AIS=${stats.aisMessages.toLocaleString()} ` +
    `decoded=${stats.aisDecoded.toLocaleString()} ` +
    `GPS=${stats.gpsMessages.toLocaleString()} ` +
    `decodeErrors=${stats.aisDecodeErrors.toLocaleString()} ` +
    `python=${pythonClients.size} ` +
    `stream=${streamClients.size} ` +
    `externalQueue=${externalFeedQueue.length}`
  );
}, 60000);

// =============================================================================
// HELPERS
// =============================================================================

function sleep(ms) {
  return new Promise(
    (resolve) => {
      setTimeout(
        resolve,
        ms
      );
    }
  );
}

// =============================================================================
// SHUTDOWN
// =============================================================================

async function shutdown(
  signal
) {
  console.log("");
  console.log(
    `Received ${signal}. Shutting down...`
  );

  // ---------------------------------------------------------------------------
  // Stop accepting new external messages.
  // ---------------------------------------------------------------------------

  try {
    aishubSocket.close();
  } catch {}

  // ---------------------------------------------------------------------------
  // Close receiver connections
  // ---------------------------------------------------------------------------

  for (
    const socket
    of connectedReceivers.values()
  ) {
    try {
      socket.close();
    } catch {}
  }

  // ---------------------------------------------------------------------------
  // Close Python connections
  // ---------------------------------------------------------------------------

  for (
    const socket
    of pythonClients
  ) {
    try {
      socket.close();
    } catch {}
  }

  // ---------------------------------------------------------------------------
  // Close public stream clients
  // ---------------------------------------------------------------------------

  for (
    const socket
    of streamClients
  ) {
    try {
      socket.close();
    } catch {}
  }

  // ---------------------------------------------------------------------------
  // Close HTTP server
  // ---------------------------------------------------------------------------

  server.close(
    () => {
      console.log(
        "OceanHelm AIS Gateway stopped."
      );

      process.exit(0);
    }
  );

  // Safety timeout
  setTimeout(
    () => {
      process.exit(0);
    },
    5000
  );
}

process.on(
  "SIGINT",
  () =>
    shutdown("SIGINT")
);

process.on(
  "SIGTERM",
  () =>
    shutdown("SIGTERM")
);
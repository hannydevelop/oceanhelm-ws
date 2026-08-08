const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const HOST = '0.0.0.0';
const PORT = 9000;

const LOG_FILE = path.join(__dirname, 'test.log');

const wss = new WebSocket.Server({
  host: HOST,
  port: PORT
});

console.log(`OceanHelm WebSocket server listening on ${HOST}:${PORT}`);
console.log(`Writing AIS data to: ${LOG_FILE}`);

wss.on('connection', (socket, request) => {
  const remoteAddress = request.socket.remoteAddress;

  console.log(`Agent connected from ${remoteAddress}`);

  socket.send(JSON.stringify({
    type: 'connection_ack',
    message: 'OceanHelm WebSocket connection established'
  }));

  socket.on('message', (data) => {
    try {
      const incoming = JSON.parse(data.toString());

      /*
       * The Go agent currently sends:
       *
       * {
       *   receiver_id: "...",
       *   timestamp: "...",
       *   nmea: "..."
       * }
       */

      const record = {
        receiver_id: incoming.receiver_id || null,
        timestamp: incoming.timestamp || new Date().toISOString(),
        nmea: incoming.nmea || null
      };

      /*
       * JSON.stringify creates a proper JSON record.
       * One record per line makes test.log easy to process later.
       */
      const jsonLine = JSON.stringify(record) + '\n';

      fs.appendFile(
        LOG_FILE,
        jsonLine,
        (error) => {
          if (error) {
            console.error(
              'Failed to write AIS message:',
              error
            );
            return;
          }

          console.log('AIS message written to test.log');
        }
      );

    } catch (error) {
      console.error(
        'Invalid WebSocket message:',
        error.message
      );
    }
  });

  socket.on('close', () => {
    console.log(`Agent disconnected: ${remoteAddress}`);
  });

  socket.on('error', (error) => {
    console.error(
      `WebSocket error: ${error.message}`
    );
  });
});

wss.on('error', (error) => {
  console.error(
    'WebSocket server error:',
    error
  );
});
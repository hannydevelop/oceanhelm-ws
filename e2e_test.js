const WebSocket = require('ws');

const consumer = new WebSocket('ws://127.0.0.1:9000/ws/stream');

consumer.on('open', () => {
  consumer.send(JSON.stringify({
    action: 'subscribe',
    bbox: [-35, -20, 55, 37],
    message_types: [1,2,3,4,5,9,18,19,21,24]
  }));
});

consumer.on('message', (msg) => {
  console.log('[consumer received]', msg.toString());
});

setTimeout(() => {
  const agent = new WebSocket('ws://127.0.0.1:9000/');

  agent.on('open', () => {
    agent.send(JSON.stringify({
      receiver_id: 'receiver-002',
      timestamp: new Date().toISOString(),
      nmea: '!AIVDM,1,1,,A,133REv0P00P=K?TMDH6P0?vN289>,0*46'
    }));
  });
}, 500);

setTimeout(() => process.exit(0), 3000);

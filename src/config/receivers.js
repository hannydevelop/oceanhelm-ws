const receivers = {
  "receiver-001": {
    id: "receiver-001",
    type: "external",
    enabled: Boolean(process.env.RECEIVER_001_HOST),
    host: process.env.RECEIVER_001_HOST,
    port: Number(process.env.RECEIVER_001_PORT || 0),
    aishub: true
  },

  "receiver-002": {
    id: "receiver-002",
    type: "local",
    enabled: Boolean(process.env.AIS_002_HOST && process.env.AIS_002_PORT),
    host: process.env.AIS_002_HOST,
    port: Number(process.env.AIS_002_PORT || 0),
    aishub: false
  },

  "receiver-003": {
    id: "receiver-003",
    type: "local",
    enabled: Boolean(process.env.AIS_003_HOST && process.env.AIS_003_PORT),
    host: process.env.AIS_003_HOST,
    port: Number(process.env.AIS_003_PORT || 0),
    aishub: false
  },

  "receiver-004": {
    id: "receiver-004",
    type: "local",
    enabled: Boolean(process.env.AIS_004_HOST && process.env.AIS_004_PORT),
    host: process.env.AIS_004_HOST,
    port: Number(process.env.AIS_004_PORT || 0),
    aishub: false
  },

  "receiver-005": {
    id: "receiver-005",
    type: "local",
    enabled: Boolean(process.env.AIS_005_HOST && process.env.AIS_005_PORT),
    host: process.env.AIS_005_HOST,
    port: Number(process.env.AIS_005_PORT || 0),
    aishub: false
  }
};

module.exports = receivers;

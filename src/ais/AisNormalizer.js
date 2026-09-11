function first(...values) {
  return values.find(value => value !== undefined && value !== null);
}

function normalizeAisMessage(message) {
  const lat = first(message.lat, message.latitude);
  const lon = first(message.lon, message.longitude);
  const mmsi = first(message.mmsi, message.MMSI);

  const normalized = {
    type: "ais",
    message_type: first(
      message.message_type,
      message.messageType,
      message.type
    ),
    mmsi: mmsi == null ? null : Number(mmsi),
    timestamp: new Date().toISOString(),

    position: null,

    navigation: {
      speed: first(
        message.sog,
        message.speed,
        message.speedOverGround
      ),
      course: first(
        message.cog,
        message.course,
        message.courseOverGround
      ),
      heading: first(
        message.heading,
        message.true_heading
      )
    },

    vessel: {
      name: first(
        message.name,
        message.shipname,
        message.ship_name
      ),
      imo: first(message.imo, message.IMO),
      callsign: first(
        message.callsign,
        message.callSign
      ),
      vessel_type: first(
        message.vessel_type,
        message.vesselType,
        message.ship_type,
        message.typeAndCargo
      )
    }
  };

  if (
    lat != null &&
    lon != null &&
    Number.isFinite(Number(lat)) &&
    Number.isFinite(Number(lon))
  ) {
    normalized.position = {
      lat: Number(lat),
      lon: Number(lon)
    };
  }

  return normalized;
}

module.exports = { normalizeAisMessage };

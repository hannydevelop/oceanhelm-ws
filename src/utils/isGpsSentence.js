function isGpsSentence(sentence) {
  const value = String(sentence).trim().toUpperCase();

  return (
    value.startsWith("$GPGGA") ||
    value.startsWith("$GNGGA") ||
    value.startsWith("$GPRMC") ||
    value.startsWith("$GNRMC") ||
    value.startsWith("$GPGLL") ||
    value.startsWith("$GNGLL") ||
    value.startsWith("$GPVTG") ||
    value.startsWith("$GNVTG") ||
    value.startsWith("$GPZDA") ||
    value.startsWith("$GNZDA")
  );
}

module.exports = { isGpsSentence };

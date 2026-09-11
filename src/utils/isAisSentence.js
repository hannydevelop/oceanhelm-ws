function isAisSentence(sentence) {
  const value = String(sentence).trim().toUpperCase();

  return value.startsWith("!AIVDM") || value.startsWith("!AIVDO");
}

module.exports = { isAisSentence };

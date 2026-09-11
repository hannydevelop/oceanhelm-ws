const { AisDecoder } = require("../src/ais/AisDecoder");

const decoder = new AisDecoder("test");

decoder.write(
  "!AIVDM,1,1,,B,133i;RPP1DPEbcDMV@1r:Ow:2>`<,0*41",
  (error, message) => {
    if (error) {
      console.error(error);
      process.exitCode = 1;
      return;
    }

    console.log(JSON.stringify(message, null, 2));
  }
);

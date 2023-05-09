import { connect } from "../../src/mod.ts";

const nc = await connect();

// create a JSM
const jsm = await nc.jetstreamManager();
// add a stream
await jsm.streams.add({ name: "A", subjects: ["a", "a.>"] });

// publish some messages that match the subjects
nc.publish("a");
nc.publish("a.b");
nc.publish("a.b.c");

await nc.drain();

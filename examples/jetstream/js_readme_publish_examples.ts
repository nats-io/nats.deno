import { connect, Empty } from "../../src/mod.ts";

const nc = await connect();
const jsm = await nc.jetstreamManager();
await jsm.streams.add({ name: "a", subjects: ["a.b"] });

// create a jetstream client:
const js = nc.jetstream();

// to publish messages to a stream:
const pa = await js.publish("a.b");
// the jetstream returns an acknowledgement with the
// stream that captured the message, it's assigned sequence
// and whether the message is a duplicate.
const stream = pa.stream;
const seq = pa.seq;
const duplicate = pa.duplicate;

// More interesting is the ability to prevent duplicates
// on messages that are stored in the server. If
// you assign a message ID, the server will keep looking
// for the same ID for a configured amount of time, and
// reject messages that sport the same ID:
await js.publish("a.b", Empty, { msgID: "a" });

// you can also specify constraints that should be satisfied.
// For example, you can request the message to have as its
// last sequence before accepting the new message:
await js.publish("a.b", Empty, { expect: { lastMsgID: "a" } });
await js.publish("a.b", Empty, { expect: { lastSequence: 3 } });
await js.publish("a.b", Empty, { expect: { streamName: "a" } });
// you can also mix the above combinations

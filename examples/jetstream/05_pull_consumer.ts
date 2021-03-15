import { AckPolicy, connect, ErrorCode } from "../../src/mod.ts";

const nc = await connect();
const jsm = await nc.jetstreamManager();

// a pull consumer doesn't have a `deliver_to`
// set. The subject of the subscription to receive
// the messages is specified by during the pull
// request
await jsm.consumers.add("A", {
  durable_name: "b",
  ack_policy: AckPolicy.Explicit,
});

const js = nc.jetstream();
let m = await js.pull("A", "b");
console.log(m.subject);
m.ack();
m = await js.pull("A", "b");
console.log(m.subject);
m.ack();
m = await js.pull("A", "b");
console.log(m.subject);
m.ack();

try {
  await js.pull("A", "b");
} catch (err) {
  if (err.code === ErrorCode.JetStream404NoMessages) {
    console.log("no messages!");
  }
}

await jsm.consumers.delete("A", "b");
await nc.close();

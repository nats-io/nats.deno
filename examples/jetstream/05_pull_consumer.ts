import { connect } from "../src/nats_deno.ts";
import { AckPolicy } from "../src/types.ts";
import { JetStreamManager } from "../src/jsm.ts";

const nc = await connect();

const jsm = await JetStreamManager(nc);
await jsm.consumers.add("A", {
  durable_name: "b",
  ack_policy: AckPolicy.Explicit,
});

let m = await jsm.consumers.pull("A", "b");
console.log(m.subject);
m.ack();
m = await jsm.consumers.pull("A", "b");
console.log(m.subject);
m.ack();
m = await jsm.consumers.pull("A", "b");
console.log(m.subject);
m.ack();

try {
  await jsm.consumers.pull("A", "b");
} catch (err) {
  if (err.message === "404 No Messages") {
    console.log("no messages!");
  }
}

await jsm.consumers.delete("A", "b");
await nc.close();

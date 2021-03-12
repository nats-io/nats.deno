import { connect, Empty } from "../src/nats_deno.ts";
import { JetStreamManager } from "../src/jetstream.ts";

const nc = await connect();

const jsm = await JetStreamManager(nc);
await jsm.streams.add({ name: "B", subjects: ["b.a"] });

const c = await nc.jetstream();
let pa = await c.publish("b.a", Empty, {
  msgID: "a",
  expect: { streamName: "B" },
});
console.log(pa.duplicate, pa.seq, pa.stream);

pa = await c.publish(
  "b.a",
  Empty,
  { msgID: "a", expect: { lastSequence: 1 } },
);
console.log(pa.duplicate, pa.seq, pa.stream);

await jsm.streams.delete("B");

await nc.drain();

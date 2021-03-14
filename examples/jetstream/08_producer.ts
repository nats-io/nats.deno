import { connect, Empty } from "../../src/mod.ts";

const nc = await connect();

const jsm = await nc.jetstreamManager();
await jsm.streams.add({ name: "B", subjects: ["b.a"] });

const js = await nc.jetstream();

let pa = await js.publish("b.a", Empty, {
  msgID: "a",
  expect: { streamName: "B" },
});
console.log(`${pa.stream}[${pa.seq}]: duplicate? ${pa.duplicate}`);

pa = await js.publish("b.a", Empty, {
  msgID: "a",
  expect: { lastSequence: 1 },
});
console.log(`${pa.stream}[${pa.seq}]: duplicate? ${pa.duplicate}`);

await jsm.streams.delete("B");
await nc.drain();

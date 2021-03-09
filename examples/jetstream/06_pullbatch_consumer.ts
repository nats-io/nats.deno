import { connect, StringCodec } from "../src/nats_deno.ts";
import { nuid } from "../src/nbc_mod.ts";
import { JetStream, JetStreamManager } from "../src/jetstream.ts";
import { AckPolicy } from "../src/types.ts";

const nc = await connect();
const stream = nuid.next();
const subj = nuid.next();
// add a stream
const jsm = await JetStreamManager(nc);
await jsm.streams.add(
  { name: stream, subjects: [subj] },
);

// add 3 messages
const sc = StringCodec();
const c = await nc.jetstream();
for (const v of "abc") {
  await c.publish(subj, sc.encode(v));
}

// add a consumer
await jsm.consumers.add(stream, {
  durable_name: "me",
  ack_policy: AckPolicy.Explicit,
});

// ask for 25 messages
const batch = jsm.consumers.pullBatch(stream, "me", { batch: 25 });
await (async () => {
  for await (const m of batch) {
    console.log(m.info);
    m.ack();
  }
})();

console.log("iterator done", batch.processed, "/", batch.received);
const info = await jsm.consumers.info(stream, "me");
console.log(info);
await nc.close();

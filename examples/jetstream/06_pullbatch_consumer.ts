import {
  AckPolicy,
  connect,
  DeliverPolicy,
  StringCodec,
} from "../../src/mod.ts";
import { nuid } from "../../nats-base-client/nuid.ts";

const nc = await connect();
const stream = nuid.next();
const subj = nuid.next();
// add a stream
const jsm = await nc.jetstreamManager();
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
  deliver_policy: DeliverPolicy.All,
});

// ask for 25 messages
const js = nc.jetstream();

const batch = js.pullBatch(stream, "me", { batch: 25, no_wait: true });
await (async () => {
  for await (const m of batch) {
    console.log(m.seq, sc.decode(m.data));
    m.ack();
  }
})();

console.log("iterator done", batch.getProcessed(), "/", batch.getReceived());
await nc.close();

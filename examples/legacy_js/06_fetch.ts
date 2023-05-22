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

const js = nc.jetstream();

// similar to `pull()`, `fetch()` requests one or more messages.
// if `expire` is set, the returned iterator will wait for the specified
// number of messages or expire at the specified time. The `no_wait`d option
// returns an empty result if no messages are available.
const batch = js.fetch(stream, "me", { batch: 25, expires: 1000 });
await (async () => {
  for await (const m of batch) {
    console.log(m.seq, sc.decode(m.data));
    m.ack();
  }
})();

console.log("iterator done", batch.getProcessed(), "/", batch.getReceived());
await nc.close();

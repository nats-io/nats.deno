import {
  cleanup,
  initStream,
  jetstreamServerConf,
  setup,
} from "../../tests/jstest_util.ts";
import {
  AckPolicy,
  deferred,
  JsMsg,
  StringCodec,
} from "../../nats-base-client/mod.ts";
import { QueuedIterator } from "../../nats-base-client/queued_iterator.ts";
import { JetStreamReader } from "../../nats-base-client/types.ts";

const sc = StringCodec();

const { ns, nc } = await setup(jetstreamServerConf({}, true));
const { stream, subj } = await initStream(nc);
const js = nc.jetstream();
const jsm = await nc.jetstreamManager();

const buf = [];
for (let i = 0; i < 100; i++) {
  buf.push(js.publish(subj, sc.encode(`${i}`)));
}
await Promise.all(buf);

// create an ephemeral consumer, note that while we typically
// bind to a consumer, some constructs notably ordered consumer
// needs additional flexibility as the consumer must be
// recreated.
const e = await js.consumer(stream);

// read the first message
let m = await e.next();
console.log(m.seq, sc.decode(m.data));
m.ack();

// read all remaining messages
let iter = await e.read() as QueuedIterator<JsMsg>;
for await (const m of iter) {
  console.log(m.seq, sc.decode(m.data));
  m.ack();

  if (m.seq === 100) {
    break;
  }
}

// create a durable consumer
await jsm.consumers.add(stream, {
  durable_name: "A",
  ack_policy: AckPolicy.Explicit,
});

// get the consumer
const a = await js.consumer(stream, "A");
// get info on the consumer
console.log(await a.info());
iter = await a.read({
  inflight_limit: { max_bytes: 1024 * 1024 },
}) as QueuedIterator<JsMsg>;
for await (const m of iter) {
  m.ack();
  if (m.seq === 100) {
    break;
  }
}
console.log(await a.info());

// callbacks are possible too
let d = deferred();
const b = await js.consumer(stream);
const sub = await b.read({
  callback: (m) => {
    m.ack();
    if (m.seq === 100) {
      d.resolve();
    }
  },
}) as JetStreamReader;

await d;
await sub.stop();
console.log(await b.info());

await cleanup(ns, nc);

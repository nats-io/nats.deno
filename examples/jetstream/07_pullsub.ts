import { AckPolicy, connect } from "../../src/mod.ts";
import { nuid } from "../../nats-base-client/nuid.ts";
import { delay } from "../../nats-base-client/util.ts";

const nc = await connect();

const stream = nuid.next();
const subj = nuid.next();

const jsm = await nc.jetstreamManager();
await jsm.streams.add(
  { name: stream, subjects: [subj] },
);

await jsm.consumers.add(stream, {
  durable_name: "c",
  ack_policy: AckPolicy.Explicit,
});

const js = nc.jetstream();
await js.publish(subj);
await js.publish(subj);
await js.publish(subj);
await js.publish(subj);

// this is similar to fetch, but the consumer is created
// behind the scenes. To pull messages, you call `pull()` on
// the PullSubscription.
const psub = await js.pullSubscribe(subj, {
  config: { durable_name: "c" },
});
const done = (async () => {
  for await (const m of psub) {
    console.log(`${m.info.stream}[${m.seq}]`);
    m.ack();
  }
})();
psub.unsubscribe(4);
psub.pull({ batch: 2 });

await delay(500);
console.log(`pull more`);
psub.pull({ batch: 2 });
await done;
console.log("done");
await nc.close();

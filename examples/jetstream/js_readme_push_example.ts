import { connect, consumerOpts, createInbox } from "../../src/mod.ts";
import { nuid } from "../../nats-base-client/nuid.ts";

// HIDE THIS
const nc = await connect();
const jsm = await nc.jetstreamManager();
const stream = nuid.next();
const subj = `${stream}.b`;
await jsm.streams.add({ name: stream, subjects: [subj] });
const js = nc.jetstream();
await js.publish(subj);
// END HIDE THIS

const opts = consumerOpts();
opts.durable("me");
opts.manualAck();
opts.ackExplicit();
opts.deliverTo(createInbox());

const sub = await js.subscribe(subj, opts);
const done = (async () => {
  for await (const m of sub) {
    m.ack();
  }
})();

// HIDE THIS
sub.unsubscribe();
await done;
// END HIDE THIS

// when done (by some logic not shown here), you can delete
// the consumer by simply calling `destroy()`. Destroying
// a consumer removes all its state information.
await sub.destroy();

// HIDE THIS
await nc.close();
// END HIDE THIS

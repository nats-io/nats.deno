import { connect, consumerOpts, nuid } from "../../src/mod.ts";

const nc = await connect();
const jsm = await nc.jetstreamManager();

const stream = nuid.next();
const subj = nuid.next();
await jsm.streams.add({ name: stream, subjects: [`${subj}.>`] });

const js = nc.jetstream();
let opts = consumerOpts()
  .deliverTo("push")
  .manualAck()
  .ackExplicit()
  .idleHeartbeat(500)
  .durable("iter-dur");
const iter = await js.subscribe(`${subj}.>`, opts);
// if 2 heartbeats are missed, the iterator will end with an error
// simply re-do the js.subscribe() and attempt again
const done = (async () => {
  for await (const m of iter) {
    m.ack();
  }
})();
done.catch((err) => {
  console.log(`iterator closed: ${err}`);
});

opts = consumerOpts()
  .deliverTo("push")
  .manualAck()
  .ackExplicit()
  .idleHeartbeat(500)
  .durable("callback-dur")
  .callback((err, m) => {
    if (err) {
      // the callback will also report a heartbeat error, however because the
      // callback can receive errors, it continues active. If the server returns
      // the client will automatically resume receiving messages
      console.log(err);
    } else {
      m?.ack();
    }
  });

const sub = await js.subscribe(`${subj}.>`, opts);
await sub.closed.then(() => console.log("sub closed"));

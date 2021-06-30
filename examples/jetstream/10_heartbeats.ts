import {
  AckPolicy,
  connect,
  isHeartbeatMsg,
  JsHeaders,
  nanos,
  nuid,
  toJsMsg,
} from "../../src/mod.ts";

const nc = await connect();
const jsm = await nc.jetstreamManager();

const stream = nuid.next();
const subj = nuid.next();
await jsm.streams.add({ name: stream, subjects: [`${subj}.>`] });

// create a regular subscription (this is an ephemeral consumer, so start the sub)
let missed = 0;
const sub = nc.subscribe("my.messages", {
  callback: (_err, msg) => {
    missed = 0;
    // simply checking if has headers and code === 100, with no reply
    // subject set. if it has a reply it would be a flow control message
    // which will get acknowledged at the end.
    if (isHeartbeatMsg(msg)) {
      // the heartbeat has additional information:
      const lastSeq = msg.headers?.get(JsHeaders.LastStreamSeqHdr);
      const consSeq = msg.headers?.get(JsHeaders.LastConsumerSeqHdr);
      console.log(
        `alive - last stream seq: ${lastSeq} - last consumer seq: ${consSeq}`,
      );
      return;
    }
    // do something with the message
    const m = toJsMsg(msg);
    m.ack();
  },
});

setInterval(() => {
  missed++;
  if (missed > 3) {
    console.error("JetStream stopped sending heartbeats!");
  }
}, 30000);

// create a consumer that delivers to the subscription
await jsm.consumers.add(stream, {
  ack_policy: AckPolicy.Explicit,
  deliver_subject: "my.messages",
  idle_heartbeat: nanos(10000),
});

await sub.closed;

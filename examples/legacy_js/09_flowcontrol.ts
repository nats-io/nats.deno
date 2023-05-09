import {
  AckPolicy,
  connect,
  isFlowControlMsg,
  nuid,
  toJsMsg,
} from "../../src/mod.ts";

const nc = await connect();
const jsm = await nc.jetstreamManager();

const N = 10000;
const stream = nuid.next();
const subj = nuid.next();
await jsm.streams.add({ name: stream, subjects: [`${subj}.>`] });

// create a regular subscription (this is an ephemeral consumer, so start the sub)
let data = 0;
let fc = 0;
const sub = nc.subscribe("my.messages", {
  callback: (_err, msg) => {
    // simply checking if has headers and code === 100
    if (isFlowControlMsg(msg)) {
      fc++;
      msg.respond();
      return;
    }
    // do something with the message
    data++;
    const m = toJsMsg(msg);
    m.ack();
    if (data === N) {
      console.log(`processed ${data} msgs and ${fc} flow control messages`);
      sub.drain();
    }
  },
});

// create a consumer that delivers to the subscription
await jsm.consumers.add(stream, {
  ack_policy: AckPolicy.Explicit,
  deliver_subject: "my.messages",
  flow_control: true,
});

// publish some old nats messages
for (let i = 0; i < N; i++) {
  nc.publish(`${subj}.${i}`);
}

await sub.closed;
await nc.close();

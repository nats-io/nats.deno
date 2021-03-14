import { AckPolicy, connect, toJsMsg } from "../../src/mod.ts";

const nc = await connect();

// create a subscription
const sub = nc.subscribe("my.messages", { max: 3 });
const done = (async () => {
  for await (const m of sub) {
    const jm = toJsMsg(m);
    console.log(
      `${jm.info.stream}[${jm.seq}] - redelivered? ${jm.redelivered}`,
    );
    if (jm.redelivered) {
      console.log("seen this before");
    }
    jm.ack();
  }
})();

// create an ephemeral consumer - the subscription must exist
const jsm = await nc.jetstreamManager();
await jsm.consumers.add("A", {
  ack_policy: AckPolicy.Explicit,
  deliver_subject: "my.messages",
});

await done;
await nc.close();

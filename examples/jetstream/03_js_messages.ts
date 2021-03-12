import { connect } from "../src/nats_deno.ts";
import { toJsMsg } from "../src/jsmsg.ts";
import { AckPolicy } from "../src/types.ts";
import { JetStreamManager } from "../src/jsm.ts";

const nc = await connect();

// create a subscription
const sub = nc.subscribe("my.messages", { max: 3 });
const done = (async () => {
  for await (const m of sub) {
    const jm = toJsMsg(m);
    console.log(`stream msg# ${jm.info.streamSequence}`);
    if (jm.redelivered) {
      console.log("seen this before");
    }
    jm.ack();
  }
})();

// create an ephemeral consumer - the subscription must exist
const jsm = await JetStreamManager(nc);
await jsm.consumers.add("A", {
  ack_policy: AckPolicy.Explicit,
  deliver_subject: "my.messages",
});

await done;
await nc.close();

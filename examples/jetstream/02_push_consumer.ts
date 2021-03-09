import { connect } from "../../src/nats_deno.ts";
import { AckPolicy } from "../../nats-base-client/types.ts";

const nc = await connect();

// create a subscription
const sub = nc.subscribe("my.messages", { max: 3 });
const done = (async () => {
  for await (const m of sub) {
    console.log(m.subject);
    m.respond();
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

import { connect } from "../src/nats_deno.ts";
import { JetStreamManager } from "../src/jsm.ts";

const nc = await connect();

const jsm = await JetStreamManager(nc);
const sub = await jsm.consumers.ephemeral("A", {}, {
  manualAcks: false, // auto ack messages
  max: 3,
});
const done = (async () => {
  for await (const m of sub) {
    console.log(m.subject);
  }
})();

await done;
await nc.drain();

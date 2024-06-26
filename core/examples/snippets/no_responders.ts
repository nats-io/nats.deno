import { connect, ErrorCode } from "jsr:@nats-io/nats-transport-deno@3.0.0-5";
import type { NatsError } from "jsr:@nats-io/nats-transport-deno@3.0.0-5";

const nc = await connect(
  {
    servers: `demo.nats.io`,
  },
);

try {
  const m = await nc.request("hello.world");
  console.log(m.data);
} catch (err) {
  const nerr = err as NatsError;
  switch (nerr.code) {
    case ErrorCode.NoResponders:
      console.log("no one is listening to 'hello.world'");
      break;
    case ErrorCode.Timeout:
      console.log("someone is listening but didn't respond");
      break;
    default:
      console.log("request failed", err);
  }
}

await nc.close();

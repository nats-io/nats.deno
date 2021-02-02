import { connect, ErrorCode, NatsError } from "../../src/mod.ts";

const nc = await connect(
  {
    servers: `demo.nats.io`,
    noResponders: true,
    headers: true,
  },
);

try {
  const m = await nc.request("hello.world");
  console.log(m.data);
} catch (err) {
  const nerr = err as NatsError;
  switch (nerr.code) {
    case ErrorCode.NO_RESPONDERS:
      console.log("no one is listening to 'hello.world'");
      break;
    case ErrorCode.TIMEOUT:
      console.log("someone is listening but didn't respond");
      break;
    default:
      console.log("request failed", err);
  }
}

await nc.close();

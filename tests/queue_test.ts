import {
  createInbox,
  Msg,
  NatsError,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import { NatsServer } from "./helpers/mod.ts";
import { connect } from "../src/connect.ts";
import { Lock } from "./helpers/mod.ts";

Deno.test("queues - validate preference", async () => {
  const sc = StringCodec();
  const servers = await NatsServer.cluster(2);

  const a = await connect({
    port: servers[0].port,
    name: "A",
  });

  const b = await connect({
    port: servers[0].port,
    name: "B",
  });

  const c = await connect({
    port: servers[1].port,
    name: "C",
  });

  // const d = await connect({
  //   port: servers[2].port,
  //   name: "D",
  // });

  const subj = createInbox();

  const lock = Lock(36);
  function mh(n: string): (err: NatsError | null, msg: Msg) => void {
    return function (): void {
      // console.log(`${n}: ${sc.decode(msg.data)}`);
      lock.unlock();
    };
  }

  a.subscribe(subj, { queue: "q", callback: mh("A") });
  b.subscribe(subj, { queue: "q", callback: mh("B") });
  c.subscribe(subj, { queue: "q", callback: mh("C") });

  await Promise.all([a.flush(), b.flush(), c.flush()]);

  for (let i = 0; i < 12; i++) {
    a.publish(subj, sc.encode("a"));
    b.publish(subj, sc.encode("b"));
    c.publish(subj, sc.encode("c"));
    // d.publish(subj, sc.encode("d"));
  }

  await lock;
  await a.close();
  await b.close();
  await c.close();
  // await d.close();

  await NatsServer.stopAll(servers);
});

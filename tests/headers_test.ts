import { connect } from "../src/connect.ts";
import { NatsServer } from "./helpers/launcher.ts";
import { Lock } from "./helpers/lock.ts";
import {
  assertEquals,
  assert,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";

Deno.test("headers - option", async () => {
  const srv = await NatsServer.start({});
  const nc = await connect(
    {
      url: `nats://127.0.0.1:${srv.port}`,
      headers: true,
    },
  );

  const headers = new Headers();
  headers.set("hello", "world");
  headers.set("one", "two");

  const lock = Lock();
  const sub = nc.subscribe("foo");
  (async () => {
    for await (const m of sub) {
      assertEquals("bar", m.data);
      assert(m.headers);
      for (const [k, v] of m.headers) {
        assert(k);
        assert(v);
        const vv = headers.get(k);
        assertEquals(v, vv);
      }
      lock.unlock();
    }
  })().then();

  console.table(nc.protocol.info);
  nc.publish("foo", "bar", { headers: headers });
  await nc.flush();
  await lock;
  await nc.close();
  await srv.stop();
});

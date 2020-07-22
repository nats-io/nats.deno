import { connect } from "../src/connect.ts";
import { NatsServer } from "./helpers/launcher.ts";
import { Lock } from "./helpers/lock.ts";
import {
  assertEquals,
  assert,
  fail,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";
import { assertErrorCode } from "./helpers/mod.ts";
import { ErrorCode } from "../src/mod.ts";

Deno.test("headers - option", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      url: `nats://127.0.0.1:${srv.port}`,
      headers: true,
    },
  );

  const headers = new Headers();
  headers.set("a", "aa");
  headers.set("b", "bb");

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

  nc.publish("foo", "bar", { headers: headers });
  await nc.flush();
  await lock;
  await nc.close();
  await srv.stop();
});

Deno.test("headers - pub throws if not enabled", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      url: `nats://127.0.0.1:${srv.port}`,
    },
  );

  const headers = new Headers();
  headers.set("a", "a");

  try {
    nc.publish("foo", "", { headers: headers });
    fail("shouldn't have been able to publish");
  } catch (err) {
    assertErrorCode(err, ErrorCode.SERVER_OPTION_NA);
  }

  await nc.close();
  await srv.stop();
});

Deno.test("headers - client fails to connect if headers not available", async () => {
  const srv = await NatsServer.start({ no_header_support: true });
  const lock = Lock();
  await connect(
    {
      url: `nats://127.0.0.1:${srv.port}`,
      headers: true,
    },
  ).catch((err) => {
    assertErrorCode(err, ErrorCode.SERVER_OPTION_NA);
    lock.unlock();
  });

  await lock;
  await srv.stop();
});

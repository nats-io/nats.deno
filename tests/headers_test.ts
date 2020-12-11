import {
  connect,
  createInbox,
  Empty,
  ErrorCode,
  headers,
  RequestOptions,
  StringCodec,
} from "../src/mod.ts";
import { NatsServer } from "./helpers/launcher.ts";
import { assertErrorCode, Lock } from "./helpers/mod.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
  fail,
} from "https://deno.land/std@0.80.0/testing/asserts.ts";
import { MsgHdrsImpl } from "../nats-base-client/internal_mod.ts";

Deno.test("headers - option", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      servers: `127.0.0.1:${srv.port}`,
      headers: true,
    },
  );

  const h = headers();
  h.set("a", "aa");
  h.set("b", "bb");

  const sc = StringCodec();

  const lock = Lock();
  const sub = nc.subscribe("foo");
  (async () => {
    for await (const m of sub) {
      assertEquals("bar", sc.decode(m.data));
      assert(m.headers);
      for (const [k, v] of m.headers) {
        assert(k);
        assert(v);
        const vv = h.values(k);
        assertArrayIncludes(v, vv);
      }
      lock.unlock();
    }
  })().then();

  nc.publish("foo", sc.encode("bar"), { headers: h });
  await nc.flush();
  await lock;
  await nc.close();
  await srv.stop();
});

Deno.test("headers - pub throws if not enabled", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      servers: `127.0.0.1:${srv.port}`,
    },
  );

  const h = headers();
  h.set("a", "a");

  try {
    nc.publish("foo", Empty, { headers: h });
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
      servers: `127.0.0.1:${srv.port}`,
      headers: true,
    },
  ).catch((err) => {
    assertErrorCode(err, ErrorCode.SERVER_OPTION_NA);
    lock.unlock();
  });

  await lock;
  await srv.stop();
});

Deno.test("headers - request headers", async () => {
  const sc = StringCodec();
  const srv = await NatsServer.start();
  const nc = await connect({
    servers: `nats://127.0.0.1:${srv.port}`,
    headers: true,
  });
  const s = createInbox();
  const sub = nc.subscribe(s);
  (async () => {
    for await (const m of sub) {
      m.respond(sc.encode("foo"), { headers: m.headers });
    }
  })().then();
  const opts = {} as RequestOptions;
  opts.headers = headers();
  opts.headers.set("x", s);
  const msg = await nc.request(s, Empty, opts);
  await nc.close();
  await srv.stop();
  assertEquals(sc.decode(msg.data), "foo");
  assertEquals(msg.headers?.get("x"), s);
});

function status(code: number, description: string): Uint8Array {
  const status = code
    ? `${MsgHdrsImpl.HEADER} ${code.toString()} ${description}`.trim()
    : MsgHdrsImpl.HEADER;
  const line = `${status}\r\n\r\n\r\n`;
  return StringCodec().encode(line);
}

function checkStatus(code = 200, description = "") {
  const h = MsgHdrsImpl.decode(status(code, description));
  const isErrorCode = code > 0 && (code < 200 || code >= 300);
  assertEquals(h.hasError, isErrorCode);

  if (code > 0) {
    assertEquals(h.code, code);
    assertEquals(h.description, description);
    assertEquals(h.status, `${code} ${description}`.trim());
    assertEquals(h.get("status"), code.toString());
  }
  assertEquals(h.get("description"), description);
}

Deno.test("headers - status", () => {
  checkStatus(0, "");
  checkStatus(200, "");
  checkStatus(200, "OK");
  checkStatus(503, "No Responders");
  checkStatus(404, "No Messages");
});

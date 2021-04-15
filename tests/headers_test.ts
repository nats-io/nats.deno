/*
 * Copyright 2020-2021 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  connect,
  createInbox,
  Empty,
  headers,
  RequestOptions,
  StringCodec,
} from "../src/mod.ts";
import { NatsServer } from "./helpers/launcher.ts";
import { Lock } from "./helpers/mod.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
} from "https://deno.land/std@0.90.0/testing/asserts.ts";
import { MsgHdrsImpl } from "../nats-base-client/internal_mod.ts";

Deno.test("headers - option", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      servers: `127.0.0.1:${srv.port}`,
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

Deno.test("headers - request headers", async () => {
  const sc = StringCodec();
  const srv = await NatsServer.start();
  const nc = await connect({
    servers: `nats://127.0.0.1:${srv.port}`,
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
    ? `NATS/1.0 ${code.toString()} ${description}`.trim()
    : "NATS/1.0";
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

Deno.test("headers - non MIME", () => {
  const h = headers();
  h.set("a", "aa");
  assertEquals(h.get("A"), "aa");
  assertEquals(h.get("A", false), "aa");
  assertEquals(h.get("a", false), "");

  h.set("b", "bb", false);
  assertEquals(h.get("B"), "");
  assertEquals(h.get("b"), "");
  assertEquals(h.get("b", false), "bb");

  h.append("c", "cc");
  assertEquals(h.values("c"), ["cc"]);
  assertEquals(h.values("C"), ["cc"]);
  assertEquals(h.values("c", false), []);

  h.append("d", "cc", false);
  assertEquals(h.values("d"), []);
  assertEquals(h.values("D"), []);
  assertEquals(h.values("d", false), ["cc"]);

  h.delete("c", false);
  assertEquals(h.values("c"), ["cc"]);
  assertEquals(h.values("C"), ["cc"]);

  h.delete("c");
  assertEquals(h.values("c"), []);
  assertEquals(h.values("C"), []);
});

Deno.test("headers - values should be arrays", () => {
  const h = headers();
  h.set("a", "aa");
  h.append("a", "bb");
  assertEquals(h.get("a"), "aa");
  assertEquals(h.values("a"), ["aa", "bb"]);
});

Deno.test("headers - equality", () => {
  const a = headers() as MsgHdrsImpl;
  const b = headers() as MsgHdrsImpl;
  assert(a.equals(b));

  a.set("a", "b");
  b.set("a", "b");
  assert(a.equals(b));

  b.append("a", "bb");
  assert(!a.equals(b));

  a.append("a", "cc");
  assert(!a.equals(b));
});

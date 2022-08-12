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
  canonicalMIMEHeaderKey,
  connect,
  createInbox,
  Empty,
  headers,
  JsHeaders,
  Match,
  NatsError,
  RequestOptions,
  StringCodec,
} from "../src/mod.ts";
import { NatsServer } from "./helpers/launcher.ts";
import {
  assert,
  assertEquals,
  assertThrows,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import {
  MsgHdrsImpl,
  MsgImpl,
  Parser,
} from "../nats-base-client/internal_mod.ts";
import { Publisher } from "../nats-base-client/protocol.ts";
import { TestDispatcher } from "./parser_test.ts";

Deno.test("headers - illegal key", () => {
  const h = headers();
  ["bad:", "bad ", String.fromCharCode(127)].forEach((v) => {
    assertThrows(() => {
      h.set(v, "aaa");
    }, NatsError);
  });

  ["\r", "\n"].forEach((v) => {
    assertThrows(() => {
      h.set("a", v);
    }, NatsError);
  });
});

Deno.test("headers - case sensitive", () => {
  const h = headers() as MsgHdrsImpl;
  h.set("a", "a");
  assert(h.has("a"));
  assert(!h.has("A"));

  h.set("A", "A");
  assert(h.has("A"));

  assertEquals(h.size(), 2);
  assertEquals(h.get("a"), "a");
  assertEquals(h.values("a"), ["a"]);
  assertEquals(h.get("A"), "A");
  assertEquals(h.values("A"), ["A"]);

  h.append("a", "aa");
  h.append("A", "AA");
  assertEquals(h.size(), 2);
  assertEquals(h.values("a"), ["a", "aa"]);
  assertEquals(h.values("A"), ["A", "AA"]);

  h.delete("a");
  assert(!h.has("a"));
  assert(h.has("A"));

  h.set("A", "AAA");
  assertEquals(h.values("A"), ["AAA"]);
});

Deno.test("headers - case insensitive", () => {
  const h = headers() as MsgHdrsImpl;
  h.set("a", "a", Match.IgnoreCase);
  // set replaces
  h.set("A", "A", Match.IgnoreCase);
  assertEquals(h.size(), 1);
  assert(h.has("a", Match.IgnoreCase));
  assert(h.has("A", Match.IgnoreCase));
  assertEquals(h.values("a", Match.IgnoreCase), ["A"]);
  assertEquals(h.values("A", Match.IgnoreCase), ["A"]);

  h.append("a", "aa");
  assertEquals(h.size(), 2);
  const v = h.values("a", Match.IgnoreCase);
  v.sort();
  assertEquals(v, ["A", "aa"]);

  h.delete("a", Match.IgnoreCase);
  assertEquals(h.size(), 0);
});

Deno.test("headers - mime", () => {
  const h = headers() as MsgHdrsImpl;
  h.set("ab", "ab", Match.CanonicalMIME);
  assert(!h.has("ab"));
  assert(h.has("Ab"));

  // set replaces
  h.set("aB", "A", Match.CanonicalMIME);
  assertEquals(h.size(), 1);
  assert(h.has("Ab"));

  h.append("ab", "aa", Match.CanonicalMIME);
  assertEquals(h.size(), 1);
  const v = h.values("ab", Match.CanonicalMIME);
  v.sort();
  assertEquals(v, ["A", "aa"]);

  h.delete("ab", Match.CanonicalMIME);
  assertEquals(h.size(), 0);
});

Deno.test("headers - publish has headers", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      port: srv.port,
    },
  );

  const h = headers();
  h.set("a", "aa");
  h.set("b", "bb");

  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 1 });
  const done = (async () => {
    for await (const m of sub) {
      assert(m.headers);
      const mh = m.headers as MsgHdrsImpl;
      assertEquals(mh.size(), 2);
      assert(mh.has("a"));
      assert(mh.has("b"));
    }
  })();

  nc.publish(subj, Empty, { headers: h });
  await done;
  await nc.close();
  await srv.stop();
});

Deno.test("headers - request has headers", async () => {
  const srv = await NatsServer.start();
  const nc = await connect({
    port: srv.port,
  });
  const s = createInbox();
  const sub = nc.subscribe(s, { max: 1 });
  const done = (async () => {
    for await (const m of sub) {
      m.respond(Empty, { headers: m.headers });
    }
  })();

  const opts = {} as RequestOptions;
  opts.headers = headers();
  opts.headers.set("x", "X");
  const msg = await nc.request(s, Empty, opts);
  assert(msg.headers);
  const mh = msg.headers;
  assert(mh.has("x"));
  await done;
  await nc.close();
  await srv.stop();
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
  }
  assertEquals(h.description, description);
}

Deno.test("headers - status", () => {
  checkStatus(0, "");
  checkStatus(200, "");
  checkStatus(200, "OK");
  checkStatus(503, "No Responders");
  checkStatus(404, "No Messages");
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

Deno.test("headers - canonical", () => {
  assertEquals(canonicalMIMEHeaderKey("foo"), "Foo");
  assertEquals(canonicalMIMEHeaderKey("foo-bar"), "Foo-Bar");
  assertEquals(canonicalMIMEHeaderKey("foo-bar-baz"), "Foo-Bar-Baz");
});

Deno.test("headers - append ignore case", () => {
  const h = headers() as MsgHdrsImpl;
  h.set("a", "a");
  h.append("A", "b", Match.IgnoreCase);
  assertEquals(h.size(), 1);
  assertEquals(h.values("a"), ["a", "b"]);
});

Deno.test("headers - append exact case", () => {
  const h = headers() as MsgHdrsImpl;
  h.set("a", "a");
  h.append("A", "b");
  assertEquals(h.size(), 2);
  assertEquals(h.values("a"), ["a"]);
  assertEquals(h.values("A"), ["b"]);
});

Deno.test("headers - append canonical", () => {
  const h = headers() as MsgHdrsImpl;
  h.set("a", "a");
  h.append("A", "b", Match.CanonicalMIME);
  assertEquals(h.size(), 2);
  assertEquals(h.values("a"), ["a"]);
  assertEquals(h.values("A"), ["b"]);
});

Deno.test("headers - malformed header are ignored", () => {
  const te = new TextEncoder();
  const d = new TestDispatcher();
  const p = new Parser(d);
  p.parse(
    te.encode(`HMSG SUBJECT 1 REPLY 17 17\r\nNATS/1.0\r\nBAD\r\n\r\n\r\n`),
  );
  assertEquals(d.errs.length, 0);
  assertEquals(d.msgs.length, 1);
  const e = d.msgs[0];
  const m = new MsgImpl(e.msg!, e.data!, {} as Publisher);
  const hi = m.headers as MsgHdrsImpl;
  assertEquals(hi.size(), 0);
});

Deno.test("headers - handles no space", () => {
  const te = new TextEncoder();
  const d = new TestDispatcher();
  const p = new Parser(d);
  p.parse(
    te.encode(`HMSG SUBJECT 1 REPLY 17 17\r\nNATS/1.0\r\nA:A\r\n\r\n\r\n`),
  );
  assertEquals(d.errs.length, 0);
  assertEquals(d.msgs.length, 1);
  const e = d.msgs[0];
  const m = new MsgImpl(e.msg!, e.data!, {} as Publisher);
  assert(m.headers);
  assertEquals(m.headers.get("A"), "A");
});

Deno.test("headers - trims values", () => {
  const te = new TextEncoder();
  const d = new TestDispatcher();
  const p = new Parser(d);
  p.parse(
    te.encode(
      `HMSG SUBJECT 1 REPLY 23 23\r\nNATS/1.0\r\nA:   A   \r\n\r\n\r\n`,
    ),
  );
  assertEquals(d.errs.length, 0);
  assertEquals(d.msgs.length, 1);
  const e = d.msgs[0];
  const m = new MsgImpl(e.msg!, e.data!, {} as Publisher);
  assert(m.headers);
  assertEquals(m.headers.get("A"), "A");
});

Deno.test("headers - error headers may have other entries", () => {
  const te = new TextEncoder();

  const d = new TestDispatcher();
  const p = new Parser(d);
  p.parse(
    te.encode(
      `HMSG _INBOX.DJ2IU18AMXPOZMG5R7NJVI 2  75 75\r\nNATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 1\r\nNats-Last-Stream: 1\r\n\r\n\r\n`,
    ),
  );
  assertEquals(d.msgs.length, 1);

  const e = d.msgs[0];
  const m = new MsgImpl(e.msg!, e.data!, {} as Publisher);
  assert(m.headers);
  assertEquals(m.headers.get(JsHeaders.LastConsumerSeqHdr), "1");
  assertEquals(m.headers.get(JsHeaders.LastStreamSeqHdr), "1");
});

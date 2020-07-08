/*
 * Copyright 2018-2020 The NATS Authors
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
  ConnectionOptions,
  ProtocolHandler,
  defaultReq,
  Msg,
  Subscription,
} from "../nats-base-client/mod.ts";

import { Lock } from "./helpers/mod.ts";

import {
  assertEquals,
  equal,
} from "https://deno.land/std/testing/asserts.ts";
import {
  MuxSubscription,
  Subscriptions,
} from "../nats-base-client/protocol.ts";

Deno.test("protocol - partial messages correctly", async () => {
  let lock = Lock(1, 3);
  let protocol = new ProtocolHandler(
    {} as ConnectionOptions,
    { publish: (subject, data1, reply) => {} },
  );
  protocol.infoReceived = true;
  // feed the inbound with arrays of 1 byte at a time
  let data =
    "MSG test.foo 1 11\r\nHello World\r\nMSG test.bar 1 11\r\nHello World\r\nMSG test.baz 1 11\r\nHello World\r\nPONG\r\n";
  let chunks: Uint8Array[] = [];
  let te = new TextEncoder();
  for (let i = 0; i < data.length; i++) {
    chunks.push(te.encode(data.charAt(i)));
  }

  let s = {} as Subscription;
  s.sid = 1;
  s.subject = "test.*";
  s.callback = ((_, msg) => {
    assertEquals(msg.data, "Hello World");
    lock.unlock();
  });

  protocol.subscriptions.add(s);

  function f(i: number) {
    setTimeout(() => {
      protocol.inbound.fill(chunks[i]);
      protocol.processInbound();
    });
  }

  for (let i = 0; i < chunks.length; i++) {
    f(i);
  }

  await lock;
});

Deno.test("protocol - mux subscription unknown return null", () => {
  let mux = new MuxSubscription();
  mux.init();

  let r = defaultReq();
  r.token = "alberto";
  mux.add(r);
  assertEquals(mux.size(), 1);
  assertEquals(mux.get("alberto"), r);
  assertEquals(mux.getToken({ subject: "" } as Msg), null);
  mux.cancel(r);
  assertEquals(mux.size(), 0);
});

Deno.test("protocol - bad dispatch is noop", () => {
  let mux = new MuxSubscription();
  mux.init();
  mux.dispatcher()(null, { subject: "foo" } as Msg);
});

Deno.test("protocol - dispatch without max", async () => {
  let lock = Lock();
  let mux = new MuxSubscription();
  mux.init();
  let r = defaultReq();
  r.token = "foo";
  // max in requests is supposed to be 1 - this just for coverage
  r.max = 2;
  r.callback = () => {
    assertEquals(mux.size(), 1);
    lock.unlock();
  };
  mux.add(r);

  let m = {} as Msg;
  m.subject = mux.baseInbox + "foo";
  let f = mux.dispatcher();
  f(null, m);
  await lock;
});

Deno.test("protocol - subs all", () => {
  const subs = new Subscriptions();
  const s = new Subscription({} as ProtocolHandler, "hello");
  s.timeout = 1;
  s.received = 0;
  subs.add(s);
  assertEquals(subs.size(), 1);
  assertEquals(s.sid, 1);
  assertEquals(subs.sidCounter, 1);
  equal(subs.get(0), s);
  const a = subs.all();
  assertEquals(a.length, 1);
  subs.cancel(a[0]);
  assertEquals(subs.size(), 0);
});

Deno.test("protocol - cancel unknown sub", () => {
  const subs = new Subscriptions();
  const s = new Subscription({} as ProtocolHandler, "hello");
  assertEquals(subs.size(), 0);
  subs.add(s);
  assertEquals(subs.size(), 1);
  subs.cancel(s);
  assertEquals(subs.size(), 0);
});

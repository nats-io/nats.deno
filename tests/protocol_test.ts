/*
 * Copyright 2018-2021 The NATS Authors
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
  Empty,
  ErrorCode,
  extractProtocolMessage,
  Msg,
  MuxSubscription,
  ProtocolHandler,
  RequestOne,
  SubscriptionImpl,
  Subscriptions,
} from "../nats-base-client/internal_mod.ts";
import { assertErrorCode } from "./helpers/mod.ts";
import {
  assertEquals,
  equal,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { protoLen } from "../nats-base-client/util.ts";

Deno.test("protocol - mux subscription unknown return null", async () => {
  const mux = new MuxSubscription();
  mux.init();

  const r = new RequestOne(mux, "");
  r.token = "alberto";
  mux.add(r);
  assertEquals(mux.size(), 1);
  assertEquals(mux.get("alberto"), r);
  assertEquals(mux.getToken({ subject: "" } as Msg), null);

  const p = Promise.race([r.deferred, r.timer])
    .catch((err) => {
      assertErrorCode(err, ErrorCode.Cancelled);
    });

  r.cancel();
  await p;
  assertEquals(mux.size(), 0);
});

Deno.test("protocol - bad dispatch is noop", () => {
  const mux = new MuxSubscription();
  mux.init();
  mux.dispatcher()(null, { subject: "foo" } as Msg);
});

Deno.test("protocol - subs all", () => {
  const subs = new Subscriptions();

  const s = new SubscriptionImpl({
    unsubscribe(_s: SubscriptionImpl, _max?: number) {},
  } as ProtocolHandler, "hello");
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
  const s = new SubscriptionImpl({
    unsubscribe(_s: SubscriptionImpl, _max?: number) {},
  } as ProtocolHandler, "hello");
  assertEquals(subs.size(), 0);
  subs.add(s);
  assertEquals(subs.size(), 1);
  subs.cancel(s);
  assertEquals(subs.size(), 0);
});

Deno.test("protocol - protolen -1 on empty", () => {
  assertEquals(protoLen(Empty), 0);
  assertEquals(extractProtocolMessage(Empty), "");
});

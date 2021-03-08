/*
 * Copyright 2021 The NATS Authors
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
import { assertEquals } from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { connect } from "../src/mod.ts";
import {
  createInbox,
  deferred,
  ErrorCode,
  Msg,
  NatsConnectionImpl,
  NatsError,
  StringCodec,
  TypedSubscription,
  TypedSubscriptionOptions,
} from "../nats-base-client/internal_mod.ts";
import { assertThrowsErrorCode } from "./helpers/asserts.ts";

const u = "demo.nats.io:4222";

Deno.test("typedsub - rejects no adapter", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  assertThrowsErrorCode(() => {
    new TypedSubscription<string>(
      nc,
      "hello",
      {} as TypedSubscriptionOptions<string>,
    );
  }, ErrorCode.ApiError);
  await nc.close();
});

Deno.test("typedsub - iterator gets data", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const subj = createInbox();

  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const text = "the fox jumped over the fence";
  const out = text.split(" ");
  tso.max = out.length;

  const sa = new TypedSubscription<string>(nc, subj, tso);
  const d: string[] = [];
  const done = (async () => {
    for await (const s of sa) {
      d.push(s);
    }
  })();

  out.forEach((v) => nc.publish(subj, sc.encode(v)));
  await done;
  assertEquals(d, out);
  await nc.close();
});

Deno.test("typedsub - callback gets data", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const subj = createInbox();

  const sc = StringCodec();

  const tso = {} as TypedSubscriptionOptions<string>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const d: string[] = [];
  tso.callback = (err, data) => {
    d.push(data ?? "BAD");
  };

  new TypedSubscription<string>(nc, subj, tso);
  const text = "the fox jumped over the fence";
  const out = text.split(" ");
  out.forEach((v) => nc.publish(subj, sc.encode(v)));
  await nc.flush();
  assertEquals(d, out);
  await nc.close();
});

Deno.test("typedsub - dispatched", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const subj = createInbox();
  const tso = {} as TypedSubscriptionOptions<Msg>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, Msg | null] => {
    return [err, msg];
  };

  tso.dispatchedFn = (msg) => {
    if (msg && msg.reply) {
      msg.respond(msg.data);
    }
  };

  const sub = new TypedSubscription<Msg>(nc, subj, tso);
  (async () => {
    for await (const m of sub) {
      // nothing
    }
  })().catch();

  const d = StringCodec().encode("hello");
  const resp = await nc.request(subj, d);
  assertEquals(resp.data, d);
  await nc.close();
});

Deno.test("typedsub - cleanup on unsub", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const subj = createInbox();
  const tso = {} as TypedSubscriptionOptions<Msg>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, Msg | null] => {
    return [err, msg];
  };

  const d = deferred<void>();
  tso.cleanupFn = (sub, info) => {
    d.resolve();
  };

  const sub = new TypedSubscription<Msg>(nc, subj, tso);
  sub.unsubscribe();
  await d;
  await nc.close();
});

Deno.test("typedsub - cleanup on close", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const subj = createInbox();
  const tso = {} as TypedSubscriptionOptions<Msg>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, Msg | null] => {
    return [err, msg];
  };

  tso.callback = (err, m) => {};

  const d = deferred<void>();
  tso.cleanupFn = (sub, info) => {
    d.resolve();
  };

  new TypedSubscription<Msg>(nc, subj, tso);
  await nc.close();
  await d;
});

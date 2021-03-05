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
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { connect, createInbox, Msg } from "../src/mod.ts";
import {
  deferred,
  SubscriptionImpl,
} from "../nats-base-client/internal_mod.ts";

const u = "demo.nats.io:4222";

Deno.test("extensions - cleanup fn called at auto unsub", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {}, max: 1 });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = ((sub, info) => {
    const id = info as { data?: string };
    d.resolve(id.data ? id.data : "");
  });
  nc.publish(subj);
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await nc.close();
});

Deno.test("extensions - cleanup fn called at unsubscribe", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = (() => {
    d.resolve("hello");
  });
  sub.unsubscribe();
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await nc.close();
});

Deno.test("extensions - cleanup fn called at sub drain", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = (() => {
    d.resolve("hello");
  });
  await sub.drain();
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await nc.close();
});

Deno.test("extensions - cleanup fn called at conn drain", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = (() => {
    d.resolve("hello");
  });
  await nc.drain();
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await nc.close();
});

Deno.test("extensions - closed resolves on unsub", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const closed = (sub as SubscriptionImpl).closed;
  sub.unsubscribe();
  await closed;
  assert(sub.isClosed());
  await nc.close();
});

Deno.test("extensions - dispatched called on callback", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} }) as SubscriptionImpl;
  let count = 0;
  sub.setDispatchedFn((msg: Msg | null) => {
    if (msg) {
      count++;
    }
  });
  nc.publish(subj);
  await nc.flush();
  assertEquals(count, 1);
  await nc.close();
});

Deno.test("extensions - dispatched called on iterator", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 1 }) as SubscriptionImpl;
  let count = 0;
  sub.setDispatchedFn((msg: Msg | null) => {
    if (msg) {
      count++;
    }
  });
  const done = (async () => {
    for await (const m of sub) {
      // nothing
    }
  })();

  nc.publish(subj);
  await done;
  assertEquals(count, 1);
  await nc.close();
});

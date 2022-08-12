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
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { createInbox, Msg, StringCodec } from "../src/mod.ts";
import {
  deferred,
  SubscriptionImpl,
} from "../nats-base-client/internal_mod.ts";
import { cleanup, setup } from "./jstest_util.ts";

Deno.test("extensions - cleanup fn called at auto unsub", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {}, max: 1 });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = (_sub, info) => {
    const id = info as { data?: string };
    d.resolve(id.data ? id.data : "");
  };
  nc.publish(subj);
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await cleanup(ns, nc);
});

Deno.test("extensions - cleanup fn called at unsubscribe", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = () => {
    d.resolve("hello");
  };
  sub.unsubscribe();
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await cleanup(ns, nc);
});

Deno.test("extensions - cleanup fn called at sub drain", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = () => {
    d.resolve("hello");
  };
  await sub.drain();
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await cleanup(ns, nc);
});

Deno.test("extensions - cleanup fn called at conn drain", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const d = deferred<string>();
  const subimpl = sub as SubscriptionImpl;
  subimpl.info = { data: "hello" };
  subimpl.cleanupFn = () => {
    d.resolve("hello");
  };
  await nc.drain();
  assertEquals(await d, "hello");
  assert(sub.isClosed());
  await cleanup(ns, nc);
});

Deno.test("extensions - closed resolves on unsub", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} });
  const closed = (sub as SubscriptionImpl).closed;
  sub.unsubscribe();
  await closed;
  assert(sub.isClosed());
  await cleanup(ns, nc);
});

Deno.test("extensions - dispatched called on callback", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { callback: () => {} }) as SubscriptionImpl;
  let count = 0;

  sub.setPrePostHandlers({
    dispatchedFn: (msg: Msg | null) => {
      if (msg) {
        count++;
      }
    },
  });
  nc.publish(subj);
  await nc.flush();
  assertEquals(count, 1);
  await cleanup(ns, nc);
});

Deno.test("extensions - dispatched called on iterator", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 1 }) as SubscriptionImpl;
  let count = 0;
  sub.setPrePostHandlers({
    dispatchedFn: (msg: Msg | null) => {
      if (msg) {
        count++;
      }
    },
  });
  const done = (async () => {
    for await (const _m of sub) {
      // nothing
    }
  })();

  nc.publish(subj);
  await done;
  assertEquals(count, 1);
  await cleanup(ns, nc);
});

Deno.test("extensions - filter called on callback", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, {
    max: 3,
    callback: (_err, m) => {
      processed.push(sc.decode(m.data));
    },
  }) as SubscriptionImpl;
  const sc = StringCodec();
  const all: string[] = [];
  const processed: string[] = [];
  const dispatched: string[] = [];
  sub.setPrePostHandlers({
    protocolFilterFn: (msg: Msg | null): boolean => {
      if (msg) {
        const d = sc.decode(msg.data);
        all.push(d);
        return d.startsWith("A");
      }
      return false;
    },
    dispatchedFn: (msg: Msg | null): void => {
      dispatched.push(msg ? sc.decode(msg.data) : "");
    },
  });

  nc.publish(subj, sc.encode("A"));
  nc.publish(subj, sc.encode("B"));
  nc.publish(subj, sc.encode("C"));
  await sub.closed;

  assertEquals(processed, ["A"]);
  assertEquals(dispatched, ["A"]);
  assertEquals(all, ["A", "B", "C"]);

  await cleanup(ns, nc);
});

Deno.test("extensions - filter called on iterator", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 3 }) as SubscriptionImpl;
  const sc = StringCodec();
  const all: string[] = [];
  const processed: string[] = [];
  const dispatched: string[] = [];
  sub.setPrePostHandlers({
    protocolFilterFn: (msg: Msg | null): boolean => {
      if (msg) {
        const d = sc.decode(msg.data);
        all.push(d);
        return d.startsWith("A");
      }
      return false;
    },
    dispatchedFn: (msg: Msg | null): void => {
      dispatched.push(msg ? sc.decode(msg.data) : "");
    },
  });
  const done = (async () => {
    for await (const m of sub) {
      processed.push(sc.decode(m.data));
    }
  })();

  nc.publish(subj, sc.encode("A"));
  nc.publish(subj, sc.encode("B"));
  nc.publish(subj, sc.encode("C"));
  await done;

  assertEquals(processed, ["A"]);
  assertEquals(dispatched, ["A"]);
  assertEquals(all, ["A", "B", "C"]);

  await cleanup(ns, nc);
});

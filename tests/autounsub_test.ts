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
  assertEquals,
} from "https://deno.land/std@0.68.0/testing/asserts.ts";

import {
  ErrorCode,
  connect,
  Subscription,
  createInbox,
  Empty,
} from "../src/mod.ts";
import { Lock } from "./helpers/mod.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";

const u = "demo.nats.io:4222";

Deno.test("autounsub - max option", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 10 });
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(sub.getReceived(), 10);
  await nc.close();
});

Deno.test("autounsub - unsubscribe", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 10 });
  sub.unsubscribe(11);
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(sub.getReceived(), 11);
  await nc.close();
});

Deno.test("autounsub - can unsub from auto-unsubscribed", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 1 });
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(sub.getReceived(), 1);
  sub.unsubscribe();
  await nc.close();
});

Deno.test("autounsub - can break to unsub", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 20 });
  const iter = (async () => {
    for await (const m of sub) {
      break;
    }
  })();
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  await iter;
  assertEquals(sub.getProcessed(), 1);
  await nc.close();
});

Deno.test("autounsub - can change auto-unsub to a higher value", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 1 });
  sub.unsubscribe(10);
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(sub.getReceived(), 10);
  await nc.close();
});

Deno.test("autounsub - request receives expected count with multiple helpers", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();

  const fn = (async (sub: Subscription) => {
    for await (const m of sub) {
      m.respond();
    }
  });
  const subs: Subscription[] = [];
  for (let i = 0; i < 5; i++) {
    const sub = nc.subscribe(subj);
    fn(sub).then();
    subs.push(sub);
  }
  await nc.request(subj);
  await nc.drain();

  let counts = subs.map((s) => {
    return s.getReceived();
  });
  const count = counts.reduce((a, v) => a + v);
  assertEquals(count, 5);
});

Deno.test("autounsub - manual request receives expected count with multiple helpers", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const lock = Lock(5);

  const fn = (async (sub: Subscription) => {
    for await (const m of sub) {
      m.respond();
      lock.unlock();
    }
  });
  for (let i = 0; i < 5; i++) {
    const sub = nc.subscribe(subj);
    fn(sub).then();
  }
  const replySubj = createInbox();
  const sub = nc.subscribe(replySubj);
  nc.publish(subj, Empty, { reply: replySubj });
  await lock;
  await nc.drain();
  assertEquals(sub.getReceived(), 5);
});

Deno.test("autounsub - check subscription leaks", async () => {
  let nc = await connect({ servers: u }) as NatsConnectionImpl;
  let subj = createInbox();
  let sub = nc.subscribe(subj);
  sub.unsubscribe();
  assertEquals(nc.protocol.subscriptions.size(), 0);
  await nc.close();
});

Deno.test("autounsub - check request leaks", async () => {
  let nc = await connect({ servers: u }) as NatsConnectionImpl;
  let subj = createInbox();

  // should have no subscriptions
  assertEquals(nc.protocol.subscriptions.size(), 0);

  let sub = nc.subscribe(subj);
  (async () => {
    for await (const m of sub) {
      m.respond();
    }
  })().then();

  // should have one subscription
  assertEquals(nc.protocol.subscriptions.size(), 1);

  let msgs = [];
  msgs.push(nc.request(subj));
  msgs.push(nc.request(subj));

  // should have 2 mux subscriptions, and 2 subscriptions
  assertEquals(nc.protocol.subscriptions.size(), 2);
  assertEquals(nc.protocol.muxSubscriptions.size(), 2);

  await Promise.all(msgs);

  // mux subs should have pruned
  assertEquals(nc.protocol.muxSubscriptions.size(), 0);

  sub.unsubscribe();
  assertEquals(nc.protocol.subscriptions.size(), 1);
  await nc.close();
});

Deno.test("autounsub - check cancelled request leaks", async () => {
  let nc = await connect({ servers: u }) as NatsConnectionImpl;
  let subj = createInbox();

  // should have no subscriptions
  assertEquals(nc.protocol.subscriptions.size(), 0);

  let rp = nc.request(subj, Empty, { timeout: 100 });

  assertEquals(nc.protocol.subscriptions.size(), 1);
  assertEquals(nc.protocol.muxSubscriptions.size(), 1);

  // the rejection should be timeout
  const lock = Lock();
  rp.catch((rej) => {
    assertEquals(rej?.code, ErrorCode.TIMEOUT);
    lock.unlock();
  });

  await lock;
  // mux subs should have pruned
  assertEquals(nc.protocol.muxSubscriptions.size(), 0);
  await nc.close();
});

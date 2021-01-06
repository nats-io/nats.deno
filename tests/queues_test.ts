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

import { connect, createInbox, Subscription } from "../src/mod.ts";
import { assertEquals } from "https://deno.land/std@0.83.0/testing/asserts.ts";

const u = "demo.nats.io:4222";

Deno.test("queues - deliver to single queue", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const subs = [];
  for (let i = 0; i < 5; i++) {
    const s = nc.subscribe(subj, { queue: "a" });
    subs.push(s);
  }
  nc.publish(subj);
  await nc.flush();
  const received = subs.map((s) => s.getReceived());
  const sum = received.reduce((p, c) => p + c);
  assertEquals(sum, 1);
  await nc.close();
});

Deno.test("queues - deliver to multiple queues", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();

  const fn = (queue: string) => {
    const subs = [];
    for (let i = 0; i < 5; i++) {
      const s = nc.subscribe(subj, { queue: queue });
      subs.push(s);
    }
    return subs;
  };

  const subsa = fn("a");
  const subsb = fn("b");

  nc.publish(subj);
  await nc.flush();

  const mc = (subs: Subscription[]): number => {
    const received = subs.map((s) => s.getReceived());
    return received.reduce((p, c) => p + c);
  };

  assertEquals(mc(subsa), 1);
  assertEquals(mc(subsb), 1);
  await nc.close();
});

Deno.test("queues - queues and subs independent", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const subs = [];
  let queueCount = 0;
  for (let i = 0; i < 5; i++) {
    const s = nc.subscribe(subj, {
      callback: () => {
        queueCount++;
      },
      queue: "a",
    });
    subs.push(s);
  }

  let count = 0;
  subs.push(nc.subscribe(subj, {
    callback: () => {
      count++;
    },
  }));
  await Promise.all(subs);

  nc.publish(subj);
  await nc.flush();
  assertEquals(queueCount, 1);
  assertEquals(count, 1);
  await nc.close();
});

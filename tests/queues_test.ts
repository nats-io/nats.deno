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
  connect,
  Nuid,
} from "../src/mod.ts";
import {
  assertEquals,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";

const u = "demo.nats.io:4222";
const nuid = new Nuid();

Deno.test("queues - deliver to single queue", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const subs = [];

  let count = 0;
  for (let i = 0; i < 5; i++) {
    const s = nc.subscribe(subj, {
      callback: () => {
        count++;
      },
      queue: "a",
    });
    subs.push(s);
  }
  await Promise.all(subs);

  nc.publish(subj);
  await nc.flush();
  await nc.close();
});

Deno.test("queues - deliver to multiple queues", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const subs = [];
  let queue1 = 0;
  for (let i = 0; i < 5; i++) {
    let s = nc.subscribe(subj, {
      callback: () => {
        queue1++;
      },
      queue: "a",
    });
    subs.push(s);
  }

  let queue2 = 0;
  for (let i = 0; i < 5; i++) {
    let s = nc.subscribe(subj, {
      callback: () => {
        queue2++;
      },
      queue: "b",
    });
    subs.push(s);
  }
  await Promise.all(subs);

  nc.publish(subj);
  await nc.flush();
  assertEquals(queue1, 1);
  assertEquals(queue2, 1);
  await nc.close();
});

Deno.test("queues - queues and subs independent", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const subs = [];
  let queueCount = 0;
  for (let i = 0; i < 5; i++) {
    let s = nc.subscribe(subj, {
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

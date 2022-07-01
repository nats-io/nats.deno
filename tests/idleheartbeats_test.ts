/*
 * Copyright 2022 The NATS Authors
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

import { IdleHeartbeat } from "../nats-base-client/idleheartbeat.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.136.0/testing/asserts.ts";
import { deferred } from "../nats-base-client/util.ts";

Deno.test("idleheartbeat - basic", async () => {
  const d = deferred<number>();
  const h = new IdleHeartbeat(250, () => {
    d.reject(new Error("didn't expect to notify"));
    return true;
  });
  let count = 0;
  const timer = setInterval(() => {
    count++;
    h.work();
    if (count === 8) {
      clearInterval(timer);
      h.cancel();
      d.resolve();
    }
  }, 100);

  await d;
});

Deno.test("idleheartbeat - timeout", async () => {
  const d = deferred<number>();
  new IdleHeartbeat(250, (v: number): boolean => {
    d.resolve(v);
    return true;
  }, { maxOut: 1 });
  assertEquals(await d, 1);
});

Deno.test("idleheartbeat - timeout maxOut", async () => {
  const d = deferred<number>();
  new IdleHeartbeat(250, (v: number): boolean => {
    d.resolve(v);
    return true;
  }, { maxOut: 5 });
  assertEquals(await d, 5);
});

Deno.test("idleheartbeat - timeout recover", async () => {
  const d = deferred<void>();
  const h = new IdleHeartbeat(250, (_v: number): boolean => {
    d.reject(new Error("didn't expect to fail"));
    return true;
  }, { maxOut: 5 });

  const interval = setInterval(() => {
    h.work();
  }, 1000);

  setTimeout(() => {
    h.cancel();
    d.resolve();
    clearInterval(interval);
  }, 1650);

  await d;
  assertEquals(h.missed, 2);
});

Deno.test("idleheartbeat - timeout autocancel", async () => {
  const d = deferred();
  const h = new IdleHeartbeat(250, (_v: number): boolean => {
    d.reject(new Error("didn't expect to fail"));
    return true;
  }, { maxOut: 4, cancelAfter: 2000 });

  assert(h.autoCancelTimer);

  let t = 0;
  const timer = setInterval(() => {
    h.work();
    t++;
    if (t === 20) {
      clearInterval(timer);
      d.resolve();
    }
  }, 100);

  // we are not canceling the monitor, as the test will catch
  // and resource leaks for a timer if not cleared.

  await d;
  assert(h.count >= 7);
  assertEquals(h.cancelAfter, 2000);
  assertEquals(h.timer, 0);
  assertEquals(h.autoCancelTimer, 0);
});

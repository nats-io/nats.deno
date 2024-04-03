/*
 * Copyright 2020-2023 The NATS Authors
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
  fail,
} from "https://deno.land/std@0.221.0/assert/mod.ts";

import {
  DebugEvents,
  deferred,
  delay,
  Heartbeat,
  PH,
  Status,
} from "../nats-base-client/internal_mod.ts";

function pm(
  lag: number,
  disconnect: () => void,
  statusHandler: (s: Status) => void,
  skip?: number[],
): PH {
  let counter = 0;
  return {
    flush(): Promise<void> {
      counter++;
      const d = deferred<void>();
      if (skip && skip.indexOf(counter) !== -1) {
        return d;
      }
      delay(lag)
        .then(() => d.resolve());
      return d;
    },
    disconnect(): void {
      disconnect();
    },
    dispatchStatus(status: Status): void {
      statusHandler(status);
    },
  };
}
Deno.test("heartbeat - timers fire", async () => {
  const status: Status[] = [];
  const ph = pm(25, () => {
    fail("shouldn't have disconnected");
  }, (s: Status): void => {
    status.push(s);
  });

  const hb = new Heartbeat(ph, 100, 3);
  hb._schedule();
  await delay(400);
  assert(hb.timer);
  hb.cancel();
  // we can have a timer still running here - we need to wait for lag
  await delay(50);
  assertEquals(hb.timer, undefined);
  assert(status.length >= 2, `status ${status.length} >= 2`);
  assertEquals(status[0].type, DebugEvents.PingTimer);
});

Deno.test("heartbeat - errors fire on missed maxOut", async () => {
  const disconnect = deferred<void>();
  const status: Status[] = [];
  const ph = pm(25, () => {
    disconnect.resolve();
  }, (s: Status): void => {
    status.push(s);
  }, [4, 5, 6]);

  const hb = new Heartbeat(ph, 100, 3);
  hb._schedule();
  await disconnect;
  assertEquals(hb.timer, undefined);
  assert(status.length >= 7, `${status.length} >= 7`);
  assertEquals(status[0].type, DebugEvents.PingTimer);
});

Deno.test("heartbeat - recovers from missed", async () => {
  const status: Status[] = [];
  const ph = pm(25, () => {
    fail("shouldn't have disconnected");
  }, (s: Status): void => {
    status.push(s);
  }, [4, 5]);

  const hb = new Heartbeat(ph, 100, 3);
  hb._schedule();
  await delay(850);
  hb.cancel();
  assertEquals(hb.timer, undefined);
  const missed = status.map((s) => {
    return s.data as number;
  });
  // we expect to have reached a condition where a max of 3 heartbeats were in pending.
  for (const n of missed) {
    assert(n <= 3, `${n} <= 3`);
  }
  for (let i = 0; i < missed.length; i++) {
    if (missed[i] === 3) {
      // expect next one to be 1
      const next = missed.length > i + 1 ? missed[i + i] : -1;
      assert(next === 1, `${next} === 1`);
      break;
    }
  }
  // some resources in the test runner are not always cleaned unless we wait a bit
  await delay(500);
});

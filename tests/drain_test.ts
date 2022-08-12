/*
 * Copyright 2020-2021 The NATS Authors
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
 *
 */
import {
  assert,
  assertEquals,
  assertThrows,
  fail,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { assertThrowsAsyncErrorCode } from "./helpers/asserts.ts";
import {
  connect,
  createInbox,
  ErrorCode,
  Msg,
  StringCodec,
} from "../src/mod.ts";

import { assertErrorCode, assertThrowsErrorCode, Lock } from "./helpers/mod.ts";
import { cleanup, setup } from "./jstest_util.ts";

Deno.test("drain - connection drains when no subs", async () => {
  const { ns, nc } = await setup();
  await nc.drain();
  await cleanup(ns);
});

Deno.test("drain - connection drain", async () => {
  const { ns, nc } = await setup();
  const nc2 = await connect({ port: ns.port });

  const max = 1000;
  const lock = Lock(max);
  const subj = createInbox();

  let first = true;
  await nc.subscribe(subj, {
    callback: () => {
      lock.unlock();
      if (first) {
        first = false;
        nc.drain();
      }
    },
    queue: "q1",
  });

  let count = 0;
  await nc2.subscribe(subj, {
    callback: () => {
      lock.unlock();
      count++;
    },
    queue: "q1",
  });

  await nc.flush();
  await nc2.flush();

  for (let i = 0; i < max; i++) {
    nc2.publish(subj);
  }
  await nc2.drain();
  await lock;
  await nc.closed();
  assert(count > 0, "expected second connection to get some messages");

  await ns.stop();
});

Deno.test("drain - subscription drain", async () => {
  const { ns, nc } = await setup();
  const lock = Lock();
  const subj = createInbox();
  let c1 = 0;
  const s1 = nc.subscribe(subj, {
    callback: () => {
      c1++;
      if (!s1.isDraining()) {
        // resolve when done
        s1.drain()
          .then(() => {
            lock.unlock();
          });
      }
    },
    queue: "q1",
  });

  let c2 = 0;
  nc.subscribe(subj, {
    callback: () => {
      c2++;
    },
    queue: "q1",
  });

  for (let i = 0; i < 10000; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  await lock;

  assertEquals(c1 + c2, 10000);
  assert(c1 >= 1, "s1 got more than one message");
  assert(c2 >= 1, "s2 got more than one message");
  assert(s1.isClosed());
  await cleanup(ns, nc);
});

Deno.test("drain - publish after drain fails", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  nc.subscribe(subj);
  await nc.drain();

  assertThrows(() => {
    nc.publish(subj);
  }, (err: Error) => {
    assertErrorCode(
      err,
      ErrorCode.ConnectionClosed,
      ErrorCode.ConnectionDraining,
    );
  });
  await ns.stop();
});

Deno.test("drain - reject reqrep during connection drain", async () => {
  const { ns, nc } = await setup();
  const nc2 = await connect({ port: ns.port });
  const lock = Lock();
  const subj = createInbox();
  const sc = StringCodec();
  // start a service for replies
  await nc.subscribe(subj, {
    callback: (_, msg: Msg) => {
      if (msg.reply) {
        msg.respond(sc.encode("ok"));
      }
    },
  });
  await nc.flush();

  let first = true;
  const done = Lock();
  await nc2.subscribe(subj, {
    callback: async () => {
      if (first) {
        first = false;
        nc2.drain()
          .then(() => {
            done.unlock();
          });
        try {
          // should fail
          await nc2.request(subj + "a");
          fail("shouldn't have been able to request");
          lock.unlock();
        } catch (err) {
          assertEquals(err.code, ErrorCode.ConnectionDraining);
          lock.unlock();
        }
      }
    },
  });
  // publish a trigger for the drain and requests
  nc2.publish(subj);
  await nc2.flush();
  await lock;
  await nc.close();
  await done;
  await ns.stop();
});

Deno.test("drain - reject drain on closed", async () => {
  const { ns, nc } = await setup();
  await nc.close();
  await assertThrowsAsyncErrorCode(async () => {
    await nc.drain();
  }, ErrorCode.ConnectionClosed);
  await ns.stop();
});

Deno.test("drain - reject drain on draining", async () => {
  const { ns, nc } = await setup();
  const done = nc.drain();
  await assertThrowsAsyncErrorCode(() => {
    return nc.drain();
  }, ErrorCode.ConnectionDraining);
  await done;
  await ns.stop();
});

Deno.test("drain - reject subscribe on draining", async () => {
  const { ns, nc } = await setup();
  const done = nc.drain();
  assertThrowsErrorCode(() => {
    return nc.subscribe("foo");
  }, ErrorCode.ConnectionDraining);
  await done;
  await ns.stop();
});

Deno.test("drain - reject subscription drain on closed sub", async () => {
  const { ns, nc } = await setup();
  const sub = nc.subscribe("foo");
  sub.unsubscribe();
  await assertThrowsAsyncErrorCode(() => {
    return sub.drain();
  }, ErrorCode.SubClosed);
  await nc.close();
  await ns.stop();
});

Deno.test("drain - connection is closed after drain", async () => {
  const { ns, nc } = await setup();
  nc.subscribe("foo");
  await nc.drain();
  assert(nc.isClosed());
  await ns.stop();
});

Deno.test("drain - reject subscription drain on closed", async () => {
  const { ns, nc } = await setup();
  const sub = nc.subscribe("foo");
  await nc.close();
  await assertThrowsAsyncErrorCode(() => {
    return sub.drain();
  }, ErrorCode.ConnectionClosed);
  await ns.stop();
});

Deno.test("drain - multiple sub drain returns same promise", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  const p1 = sub.drain();
  const p2 = sub.drain();
  assertEquals(p1, p2);
  nc.publish(subj);
  await nc.flush();
  await p1;
  await cleanup(ns, nc);
});

Deno.test("drain - publisher drain", async () => {
  const { ns, nc } = await setup();
  const nc1 = await connect({ port: ns.port });

  const subj = createInbox();
  const lock = Lock(10);

  nc.subscribe(subj, {
    callback: () => {
      lock.unlock();
    },
  });
  await nc.flush();

  for (let i = 0; i < 10; i++) {
    nc1.publish(subj);
  }
  await nc1.drain();
  await lock;
  await cleanup(ns, nc, nc1);
});

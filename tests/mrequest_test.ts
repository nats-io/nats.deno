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
import { cleanup, setup } from "./jstest_util.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";
import { createInbox } from "../nats-base-client/protocol.ts";
import { Lock } from "./helpers/lock.ts";
import { Empty, RequestStrategy } from "../nats-base-client/types.ts";

import {
  assert,
  assertEquals,
  fail,
} from "https://deno.land/std@0.138.0/testing/asserts.ts";
import { StringCodec } from "../nats-base-client/codec.ts";
import { assertErrorCode } from "./helpers/mod.ts";
import { deferred, ErrorCode } from "../nats-base-client/mod.ts";
import { delay } from "../nats-base-client/util.ts";

Deno.test("mreq - request many count", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 5; i++) {
        msg.respond();
      }
    },
  });

  const lock = Lock(5, 2000);

  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Count,
    maxWait: 2000,
    maxMessages: 5,
    noMux: true,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    lock.unlock();
  }
  await lock;

  assertEquals(nci.protocol.subscriptions.size(), 1);
  await cleanup(ns, nc);
});

Deno.test("mreq - nomux request many jitter", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 10; i++) {
        msg.respond();
      }
    },
  });

  let count = 0;
  const start = Date.now();

  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.JitterTimer,
    maxWait: 5000,
    noMux: true,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(500 > time);
  assertEquals(count, 10);
  await cleanup(ns, nc);
});

Deno.test("mreq - nomux request many sentinel", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  const sc = StringCodec();
  const payload = sc.encode("hello");
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 10; i++) {
        msg.respond(payload);
      }
      msg.respond();
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.SentinelMsg,
    maxWait: 2000,
    noMux: true,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(500 > time);
  assertEquals(count, 11);
  await cleanup(ns, nc);
});

Deno.test("mreq - nomux request many sentinel - partial response", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  const sc = StringCodec();
  const payload = sc.encode("hello");
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 10; i++) {
        msg.respond(payload);
      }
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.SentinelMsg,
    maxWait: 2000,
    noMux: true,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(time >= 2000);
  assertEquals(count, 10);
  await cleanup(ns, nc);
});

Deno.test("mreq - nomux request many wait for timer - no respone", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: () => {
      // ignore it
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Timer,
    maxWait: 2000,
    noMux: true,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(time >= 2000);
  assertEquals(count, 0);
  await cleanup(ns, nc);
});

Deno.test("mreq - nomux request many waits for timer late response", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: async (_err, msg) => {
      await delay(1750);
      msg.respond();
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Timer,
    maxWait: 2000,
    noMux: true,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(time >= 2000);
  assertEquals(count, 1);
  await cleanup(ns, nc);
});

Deno.test("mreq - nomux request many stops on error", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();

  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Timer,
    maxWait: 2000,
    noMux: true,
  });
  const d = deferred<Error>();
  for await (const mer of iter) {
    if (mer instanceof Error) {
      d.resolve(mer);
    }
  }
  const err = await d;
  assertErrorCode(err, ErrorCode.NoResponders);
  await cleanup(ns, nc);
});

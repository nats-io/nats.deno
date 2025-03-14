/*
 * Copyright 2022-2024 The NATS Authors
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
  cleanup,
  jetstreamServerConf,
  setup,
} from "../../tests/helpers/mod.ts";
import { initStream } from "./jstest_util.ts";
import { AckPolicy, DeliverPolicy } from "../jsapi_types.ts";
import {
  assertEquals,
  assertExists,
  assertRejects,
} from "https://deno.land/std@0.221.0/assert/mod.ts";
import { Empty } from "../../nats-base-client/encoders.ts";
import { StringCodec } from "../../nats-base-client/codec.ts";
import {
  deadline,
  deferred,
  delay,
  nanos,
} from "../../nats-base-client/util.ts";
import { NatsConnectionImpl } from "../../nats-base-client/nats.ts";
import { syncIterator } from "../../nats-base-client/core.ts";
import { PullConsumerMessagesImpl } from "../consumer.ts";

Deno.test("fetch - no messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "b",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const consumer = await js.consumers.get(stream, "b");
  const iter = await consumer.fetch({
    max_messages: 100,
    expires: 1000,
  });
  for await (const m of iter) {
    m.ack();
  }
  assertEquals(iter.getReceived(), 0);
  assertEquals(iter.getProcessed(), 0);

  await cleanup(ns, nc);
});

Deno.test("fetch - less messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const { stream, subj } = await initStream(nc);
  const js = nc.jetstream();
  await js.publish(subj, Empty);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "b",
    ack_policy: AckPolicy.Explicit,
  });

  const consumer = await js.consumers.get(stream, "b");
  assertEquals((await consumer.info(true)).num_pending, 1);
  const iter = await consumer.fetch({ expires: 1000, max_messages: 10 });
  for await (const m of iter) {
    m.ack();
  }
  assertEquals(iter.getReceived(), 1);
  assertEquals(iter.getProcessed(), 1);

  await cleanup(ns, nc);
});

Deno.test("fetch - exactly messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const { stream, subj } = await initStream(nc);
  const sc = StringCodec();
  const js = nc.jetstream();
  await Promise.all(
    new Array(200).fill("a").map((_, idx) => {
      return js.publish(subj, sc.encode(`${idx}`));
    }),
  );

  const jsm = await nc.jetstreamManager();

  await jsm.consumers.add(stream, {
    durable_name: "b",
    ack_policy: AckPolicy.Explicit,
  });

  const consumer = await js.consumers.get(stream, "b");
  assertEquals((await consumer.info(true)).num_pending, 200);

  const iter = await consumer.fetch({ expires: 5000, max_messages: 100 });
  for await (const m of iter) {
    m.ack();
  }
  assertEquals(iter.getReceived(), 100);
  assertEquals(iter.getProcessed(), 100);

  await cleanup(ns, nc);
});

Deno.test("fetch - consumer not found", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "A", subjects: ["hello"] });

  await jsm.consumers.add("A", {
    durable_name: "a",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get("A", "a");

  await c.delete();

  const iter = await c.fetch({
    expires: 3000,
  });

  const exited = assertRejects(
    async () => {
      for await (const _ of iter) {
        // nothing
      }
    },
    Error,
    "no responders",
  );

  await exited;
  await cleanup(ns, nc);
});

Deno.test("fetch - deleted consumer", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "A", subjects: ["a"] });

  await jsm.consumers.add("A", {
    durable_name: "a",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get("A", "a");

  const iter = await c.fetch({
    expires: 3000,
  });

  const exited = assertRejects(
    async () => {
      for await (const _ of iter) {
        // nothing
      }
    },
    Error,
    "consumer deleted",
  );

  await delay(1000);
  await c.delete();

  await exited;
  await cleanup(ns, nc);
});

Deno.test("fetch - stream not found", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "A", subjects: ["hello"] });

  await jsm.consumers.add("A", {
    durable_name: "a",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get("A", "a");
  const iter = await c.fetch({
    expires: 3000,
  });
  await jsm.streams.delete("A");

  await assertRejects(
    async () => {
      for await (const _ of iter) {
        // nothing
      }
    },
    Error,
    "no responders",
  );

  await cleanup(ns, nc);
});

Deno.test("fetch - listener leaks", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "messages", subjects: ["hello"] });

  const js = nc.jetstream();
  await js.publish("hello");

  await jsm.consumers.add("messages", {
    durable_name: "myconsumer",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
    ack_wait: nanos(3000),
    max_waiting: 500,
  });

  const nci = nc as NatsConnectionImpl;
  const base = nci.protocol.listeners.length;

  const consumer = await js.consumers.get("messages", "myconsumer");

  let done = false;
  while (!done) {
    const iter = await consumer.fetch({
      max_messages: 1,
    }) as PullConsumerMessagesImpl;
    for await (const m of iter) {
      assertEquals(nci.protocol.listeners.length, base);
      m?.nak();
      if (m.info.deliveryCount > 100) {
        done = true;
      }
    }
  }

  assertEquals(nci.protocol.listeners.length, base);

  await cleanup(ns, nc);
});

Deno.test("fetch - sync", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "messages", subjects: ["hello"] });

  const js = nc.jetstream();
  await js.publish("hello");
  await js.publish("hello");

  await jsm.consumers.add("messages", {
    durable_name: "c",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
    ack_wait: nanos(3000),
    max_waiting: 500,
  });

  const consumer = await js.consumers.get("messages", "c");
  const iter = await consumer.fetch({ max_messages: 2 });
  const sync = syncIterator(iter);
  assertExists(await sync.next());
  assertExists(await sync.next());
  assertEquals(await sync.next(), null);
  await cleanup(ns, nc);
});

Deno.test("fetch - consumer bind", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "A", subjects: ["a"] });

  await jsm.consumers.add("A", {
    durable_name: "a",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  await js.publish("a");

  const c = await js.consumers.get("A", "a");
  await c.delete();

  const cisub = nc.subscribe("$JS.API.CONSUMER.INFO.A.a", {
    callback: () => {},
  });

  const iter = await c.fetch({
    expires: 1000,
    bind: true,
  });

  await assertRejects(
    () => {
      return (async () => {
        for await (const _ of iter) {
          // nothing
        }
      })();
    },
    Error,
    "no responders",
  );

  assertEquals(cisub.getProcessed(), 0);
  await cleanup(ns, nc);
});

Deno.test("fetch - no responders - stream deleted", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();

  await jsm.streams.add({ name: "messages", subjects: ["hello"] });
  await jsm.consumers.add("messages", {
    name: "c",
    deliver_policy: DeliverPolicy.All,
  });

  // stream is deleted
  const c = await jsm.jetstream().consumers.get("messages", "c");
  await jsm.streams.delete("messages");

  await assertRejects(
    async () => {
      const iter = await c.fetch({ expires: 10_000 });
      for await (const _ of iter) {
        // nothing
      }
    },
    Error,
    "no responders",
  );

  await jsm.streams.add({ name: "messages", subjects: ["hello"] });
  await jsm.consumers.add("messages", {
    name: "c",
    deliver_policy: DeliverPolicy.All,
  });
  await nc.jetstream().publish("hello");

  const mP = deferred<void>();
  const iter = await c.fetch({ expires: 10_000 });
  for await (const _ of iter) {
    mP.resolve();
    break;
  }
  await deadline(mP, 5_000);
  await cleanup(ns, nc);
});

Deno.test("fetch - no responders - consumer deleted", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();

  await jsm.streams.add({ name: "messages", subjects: ["hello"] });
  await jsm.consumers.add("messages", {
    name: "c",
    deliver_policy: DeliverPolicy.All,
  });

  // stream is deleted
  const c = await jsm.jetstream().consumers.get("messages", "c");
  await jsm.consumers.delete("messages", "c");

  await assertRejects(
    async () => {
      const iter = await c.fetch({ expires: 10_000 });
      for await (const _ of iter) {
        // nothing
      }
    },
    Error,
    "no responders",
  );

  await jsm.consumers.add("messages", {
    name: "c",
    deliver_policy: DeliverPolicy.All,
  });
  await nc.jetstream().publish("hello");

  const mP = deferred<void>();
  const iter = await c.fetch({ expires: 10_000 });
  for await (const _ of iter) {
    mP.resolve();
    break;
  }
  await deadline(mP, 5_000);
  await cleanup(ns, nc);
});

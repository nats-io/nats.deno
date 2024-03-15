/*
 * Copyright 2022-2023 The NATS Authors
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
import { setupStreamAndConsumer } from "../../examples/jetstream/util.ts";
import { assertEquals } from "https://deno.land/std@0.200.0/assert/assert_equals.ts";
import { assertRejects } from "https://deno.land/std@0.200.0/assert/assert_rejects.ts";
import { consumerHbTest } from "./consumers_test.ts";
import { initStream } from "./jstest_util.ts";
import { AckPolicy, DeliverPolicy } from "../jsapi_types.ts";
import { deadline, deferred } from "../../nats-base-client/util.ts";
import { nanos } from "../jsutil.ts";
import { ConsumerEvents, PullConsumerMessagesImpl } from "../consumer.ts";
import { syncIterator } from "../../nats-base-client/core.ts";
import { assertExists } from "https://deno.land/std@0.200.0/assert/assert_exists.ts";

Deno.test("consumers - consume", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const count = 1000;
  const { stream, consumer } = await setupStreamAndConsumer(nc, count);

  const js = nc.jetstream({ timeout: 30_000 });
  const c = await js.consumers.get(stream, consumer);
  const ci = await c.info();
  assertEquals(ci.num_pending, count);
  const start = Date.now();
  const iter = await c.consume({ expires: 2_000, max_messages: 10 });
  for await (const m of iter) {
    m.ack();
    if (m.info.pending === 0) {
      const millis = Date.now() - start;
      console.log(
        `consumer: ${millis}ms - ${count / (millis / 1000)} msgs/sec`,
      );
      break;
    }
  }
  assertEquals(iter.getReceived(), count);
  assertEquals(iter.getProcessed(), count);
  assertEquals((await c.info()).num_pending, 0);
  await cleanup(ns, nc);
});

Deno.test("consumers - consume callback rejects iter", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream, consumer } = await setupStreamAndConsumer(nc, 0);
  const js = nc.jetstream();
  const c = await js.consumers.get(stream, consumer);
  const iter = await c.consume({
    expires: 5_000,
    max_messages: 10_000,
    callback: (m) => {
      m.ack();
    },
  });

  await assertRejects(
    async () => {
      for await (const _o of iter) {
        // should fail
      }
    },
    Error,
    "unsupported iterator",
  );
  iter.stop();

  await cleanup(ns, nc);
});

Deno.test("consumers - consume heartbeats", async () => {
  await consumerHbTest(false);
});

Deno.test("consumers - consume deleted consumer", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get(stream, "a");
  const iter = await c.consume({
    expires: 3000,
  });

  const deleted = deferred();
  let notFound = 0;
  const done = deferred<number>();
  (async () => {
    const status = await iter.status();
    for await (const s of status) {
      if (s.type === ConsumerEvents.ConsumerDeleted) {
        deleted.resolve();
      }
      if (s.type === ConsumerEvents.ConsumerNotFound) {
        notFound++;
        if (notFound > 1) {
          done.resolve();
        }
      }
    }
  })().then();

  (async () => {
    for await (const _m of iter) {
      // nothing
    }
  })().then();

  setTimeout(() => {
    jsm.consumers.delete(stream, "a");
  }, 1000);

  await deleted;
  await done;
  await iter.close();

  await cleanup(ns, nc);
});

Deno.test("consumers - sub leaks consume()", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: stream,
    ack_policy: AckPolicy.Explicit,
  });
  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  const js = nc.jetstream();
  const c = await js.consumers.get(stream, stream);
  const iter = await c.consume({ expires: 30000 });
  const done = (async () => {
    for await (const _m of iter) {
      // nothing
    }
  })().then();
  setTimeout(() => {
    iter.close();
  }, 1000);

  await done;
  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  await cleanup(ns, nc);
});

Deno.test("consumers - consume drain", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: stream,
    ack_policy: AckPolicy.Explicit,
  });
  //@ts-ignore: test
  const js = nc.jetstream();
  const c = await js.consumers.get(stream, stream);
  const iter = await c.consume({ expires: 30000 });
  setTimeout(() => {
    nc.drain();
  }, 100);
  const done = (async () => {
    for await (const _m of iter) {
      // nothing
    }
  })().then();

  await deadline(done, 1000);

  await cleanup(ns, nc);
});

Deno.test("consumers - consume sync", async () => {
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
  const iter = await consumer.consume() as PullConsumerMessagesImpl;
  const sync = syncIterator(iter);
  assertExists(await sync.next());
  assertExists(await sync.next());
  iter.stop();
  assertEquals(await sync.next(), null);
  await cleanup(ns, nc);
});

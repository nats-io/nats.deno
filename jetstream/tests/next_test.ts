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
  assertRejects,
} from "@std/assert";
import { NatsConnectionImpl } from "../../nats-base-client/nats.ts";
import { delay, nanos } from "../../nats-base-client/util.ts";

Deno.test("next - basics", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: stream,
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get(stream, stream);
  let ci = await c.info(true);
  assertEquals(ci.num_pending, 0);

  let m = await c.next({ expires: 1000 });
  assertEquals(m, null);

  await Promise.all([js.publish(subj), js.publish(subj)]);
  ci = await c.info();
  assertEquals(ci.num_pending, 2);

  m = await c.next();
  assertEquals(m?.seq, 1);
  m?.ack();
  await nc.flush();

  ci = await c.info();
  assertEquals(ci?.num_pending, 1);
  m = await c.next();
  assertEquals(m?.seq, 2);
  m?.ack();

  await cleanup(ns, nc);
});

Deno.test("next - sub leaks", async () => {
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
  await c.next({ expires: 1000 });
  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  await cleanup(ns, nc);
});

Deno.test("next - listener leaks", async () => {
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

  while (true) {
    const m = await consumer.next();
    if (m) {
      m.nak();
      if (m.info?.redeliveryCount > 100) {
        break;
      }
    }
  }
  assertEquals(nci.protocol.listeners.length, base);

  await cleanup(ns, nc);
});

Deno.test("next - consumer not found", async () => {
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
  await delay(1000);

  const exited = assertRejects(
    async () => {
      await c.next({ expires: 1000 });
    },
    Error,
    "consumer not found",
  );

  await exited;
  await cleanup(ns, nc);
});

Deno.test("next - deleted consumer", async () => {
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

  (nc as NatsConnectionImpl).options.debug = true;
  const exited = assertRejects(
    () => {
      return c.next({ expires: 4000 });
    },
    Error,
    "consumer deleted",
  );
  await delay(1000);
  await c.delete();

  await exited;

  await cleanup(ns, nc);
});

Deno.test("next - stream not found", async () => {
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

  (nc as NatsConnectionImpl).options.debug = true;
  await jsm.streams.delete("A");
  await delay(1000);

  await assertRejects(
    () => {
      return c.next({ expires: 4000 });
    },
    Error,
    "stream not found",
  );

  await cleanup(ns, nc);
});

Deno.test("next - consumer bind", async () => {
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

  const msg = await c.next({
    expires: 1000,
    bind: true,
  });

  assertEquals(msg, null);
  assertEquals(cisub.getProcessed(), 0);

  await cleanup(ns, nc);
});

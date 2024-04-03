/*
 * Copyright 2023 The NATS Authors
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

import { NatsServer, notCompatible } from "../../tests/helpers/mod.ts";
import { AckPolicy, connect, JSONCodec } from "../../src/mod.ts";
import {
  assertArrayIncludes,
  assertEquals,
  assertExists,
  assertRejects,
} from "https://deno.land/std@0.221.0/assert/mod.ts";
import {
  cleanup,
  jetstreamServerConf,
  setup,
} from "../../tests/helpers/mod.ts";
import { initStream } from "./jstest_util.ts";
import { NatsConnectionImpl } from "../../nats-base-client/nats.ts";

Deno.test("streams - get", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream();

  await assertRejects(
    async () => {
      await js.streams.get("another");
    },
    Error,
    "stream not found",
  );

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({
    name: "another",
    subjects: ["a.>"],
  });

  const s = await js.streams.get("another");
  assertExists(s);
  assertEquals(s.name, "another");

  await jsm.streams.delete("another");
  await assertRejects(
    async () => {
      await s.info();
    },
    Error,
    "stream not found",
  );

  await cleanup(ns, nc);
});

Deno.test("streams - mirrors", async () => {
  const cluster = await NatsServer.jetstreamCluster(3);
  const nc = await connect({ port: cluster[0].port });
  const jsm = await nc.jetstreamManager();

  // create a stream in a different server in the cluster
  await jsm.streams.add({
    name: "src",
    subjects: ["src.*"],
    placement: {
      cluster: cluster[1].config.cluster.name,
      tags: cluster[1].config.server_tags,
    },
  });

  // create a mirror in the server we connected
  await jsm.streams.add({
    name: "mirror",
    placement: {
      cluster: cluster[2].config.cluster.name,
      tags: cluster[2].config.server_tags,
    },
    mirror: {
      name: "src",
    },
  });

  const js = nc.jetstream();
  const s = await js.streams.get("src");
  assertExists(s);
  assertEquals(s.name, "src");

  const alternates = await s.alternates();
  assertEquals(2, alternates.length);
  assertArrayIncludes(alternates.map((a) => a.name), ["src", "mirror"]);

  await assertRejects(
    async () => {
      await js.streams.get("another");
    },
    Error,
    "stream not found",
  );

  const s2 = await s.best();
  const selected = (await s.info(true)).alternates?.[0]?.name ?? "";
  assertEquals(s2.name, selected);

  await nc.close();
  await NatsServer.stopAll(cluster);
});

Deno.test("streams - consumers", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream();

  // add a stream and a message
  const { stream, subj } = await initStream(nc);
  await js.publish(subj, JSONCodec().encode({ hello: "world" }));

  // retrieve the stream
  const s = await js.streams.get(stream);
  assertExists(s);
  assertEquals(s.name, stream);

  // get a message
  const sm = await s.getMessage({ seq: 1 });
  let d = sm.json<{ hello: string }>();
  assertEquals(d.hello, "world");

  // attempt to get a named consumer
  await assertRejects(
    async () => {
      await s.getConsumer("a");
    },
    Error,
    "consumer not found",
  );

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(s.name, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
  });
  const c = await s.getConsumer("a");
  const jm = await c.next();
  assertExists(jm);
  d = jm?.json<{ hello: string }>();
  assertEquals(d.hello, "world");

  await cleanup(ns, nc);
});

Deno.test("streams - delete message", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream();

  // add a stream and a message
  const { stream, subj } = await initStream(nc);
  await Promise.all([js.publish(subj), js.publish(subj), js.publish(subj)]);

  // retrieve the stream
  const s = await js.streams.get(stream);
  assertExists(s);
  assertEquals(s.name, stream);

  // get a message
  const sm = await s.getMessage({ seq: 2 });
  assertExists(sm);

  assertEquals(await s.deleteMessage(2, true), true);
  await assertRejects(
    async () => {
      await s.getMessage({ seq: 2 });
    },
    Error,
    "no message found",
  );

  const si = await s.info(false, { deleted_details: true });
  assertEquals(si.state.deleted, [2]);

  await cleanup(ns, nc);
});

Deno.test("streams - first_seq", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.add({
    name: "test",
    first_seq: 50,
    subjects: ["foo"],
  });
  assertEquals(si.config.first_seq, 50);

  const pa = await nc.jetstream().publish("foo");
  assertEquals(pa.seq, 50);

  await cleanup(ns, nc);
});

Deno.test("streams - first_seq fails if wrong server", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const nci = nc as NatsConnectionImpl;
  nci.features.update("2.9.2");

  const jsm = await nc.jetstreamManager();
  await assertRejects(
    async () => {
      await jsm.streams.add({
        name: "test",
        first_seq: 50,
        subjects: ["foo"],
      });
    },
    Error,
    "stream 'first_seq' requires server 2.10.0",
  );

  await cleanup(ns, nc);
});

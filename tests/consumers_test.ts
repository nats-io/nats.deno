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
  initStream,
  jetstreamServerConf,
  setup,
} from "./jstest_util.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";
import {
  assertEquals,
  assertExists,
} from "https://deno.land/std@0.75.0/testing/asserts.ts";
import {
  AckPolicy,
  Consumer,
  ConsumerMessages,
  Empty,
  NatsConnection,
  PubAck,
  StorageType,
} from "../nats-base-client/types.ts";
import { StringCodec } from "../nats-base-client/codec.ts";
import { deferred, nuid } from "../nats-base-client/mod.ts";
import { fail } from "https://deno.land/std@0.179.0/testing/asserts.ts";
import { NatsServer } from "./helpers/launcher.ts";
import { connect } from "../src/connect.ts";
import { PullConsumerMessagesImpl } from "../nats-base-client/consumermessages.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";

Deno.test("consumers - min supported server", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  (nc as NatsConnectionImpl).features.update("2.9.2");
  const js = nc.jetstream();
  await assertRejects(
    async () => {
      await js.consumers.get(stream, "a");
    },
    Error,
    "jetstream simplification is only supported on servers",
  );

  await assertRejects(
    async () => {
      await js.consumers.ordered(stream);
    },
    Error,
    "jetstream simplification is only supported on servers",
  );

  await cleanup(ns, nc);
});

Deno.test("consumers - get", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();
  await assertRejects(
    async () => {
      await js.consumers.get("a", "b");
    },
    Error,
    "stream not found",
  );

  const jsm = await nc.jetstreamManager();
  const { stream } = await initStream(nc);

  await assertRejects(
    async () => {
      await js.consumers.get(stream, "b");
    },
    Error,
    "consumer not found",
  );
  await jsm.consumers.add(stream, { durable_name: "b" });

  const consumer = await js.consumers.get(stream, "b");
  assertExists(consumer);

  await cleanup(ns, nc);
});

Deno.test("consumers - delete", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();

  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();

  await jsm.consumers.add(stream, { durable_name: "b" });

  const c = await js.consumers.get(stream, "b");
  await c.delete();

  await assertRejects(
    async () => {
      await js.consumers.get(stream, "b");
    },
    Error,
    "consumer not found",
  );

  await cleanup(ns, nc);
});

Deno.test("consumers - info", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const js = nc.jetstream();

  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, { durable_name: "b" });
  const c = await js.consumers.get(stream, "b");
  assertEquals(await jsm.consumers.info(stream, "b"), await c.info());
  await cleanup(ns, nc);
});

Deno.test("consumers - push consumer not supported", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();

  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "b",
    deliver_subject: "foo",
  });
  await assertRejects(
    async () => {
      await js.consumers.get(stream, "b");
    },
    Error,
    "push consumer not supported",
  );

  await cleanup(ns, nc);
});

Deno.test("consumers - fetch no messages", async () => {
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
  for await (const o of iter) {
    if (o.isError) {
      console.error(o.error);
      continue;
    }
    console.log(o.value.seq);
    o.value?.ack();
  }
  assertEquals(iter.getReceived(), 0);
  assertEquals(iter.getProcessed(), 0);

  await cleanup(ns, nc);
});

Deno.test("consumers - fetch less messages", async () => {
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
  for await (const o of iter) {
    if (o.isError) {
      console.error(o.error);
      continue;
    }
    o.value.ack();
  }
  assertEquals(iter.getReceived(), 1);
  assertEquals(iter.getProcessed(), 1);

  await cleanup(ns, nc);
});

Deno.test("consumers - fetch exactly messages", async () => {
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
  for await (const o of iter) {
    if (o.isError) {
      fail(`failed with ${o.error}`);
    } else {
      o.value.ack();
    }
  }
  assertEquals(iter.getReceived(), 100);
  assertEquals(iter.getProcessed(), 100);

  await cleanup(ns, nc);
});

Deno.test("consumers - consume", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    jetstream: {
      max_memory_store: 1024 * 1024 * 1024,
    },
  }));

  const count = 50_000;
  const conf = await memStream(nc, count);
  console.log(`seeded server: ${conf.millis}ms`);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(conf.stream, {
    durable_name: "b",
    ack_policy: AckPolicy.Explicit,
  });

  const start = Date.now();
  const js = nc.jetstream();
  const consumer = await js.consumers.get(conf.stream, "b");
  assertEquals((await consumer.info(true)).num_pending, count);
  const iter = await consumer.consume({
    expires: 10_000,
    max_messages: 50_000,
  });
  for await (const o of iter) {
    if (o.isError) {
      fail(`failed with ${o.error}`);
    } else {
      o.value.ack();
      if (o.value.seq === count) {
        const millis = Date.now() - start;
        console.log(
          `consumer: ${millis}ms - ${count / (millis / 1000)} msgs/sec`,
        );
        break;
      }
    }
  }
  assertEquals(iter.getReceived(), count);
  assertEquals(iter.getProcessed(), count);
  assertEquals((await consumer.info()).num_pending, 0);
  await cleanup(ns, nc);
});

Deno.test("consumers - consume callback rejects iter", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    jetstream: {
      max_memory_store: 1024 * 1024 * 1024,
    },
  }));

  const conf = await memStream(nc, 0);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(conf.stream, {
    durable_name: "b",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const consumer = await js.consumers.get(conf.stream, "b");
  const iter = await consumer.consume({
    expires: 10_000,
    max_messages: 50_000,
    callback: (r) => {
      if (r.isError) {
        fail(`failed with ${r.error}`);
      } else {
        r.value.ack();
      }
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

Deno.test("consumers - fetch heartbeats", async () => {
  await consumerHbTest(true);
});

async function memStream(
  nc: NatsConnection,
  msgs = 1000,
  size = 0,
  batch = 10000,
): Promise<{ millis: number; stream: string; subj: string }> {
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  const subj = nuid.next();
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
    storage: StorageType.Memory,
  });
  const payload = new Uint8Array(size);

  const js = nc.jetstream();
  const start = Date.now();
  const buf: Promise<PubAck>[] = [];
  for (let i = 0; i < msgs; i++) {
    buf.push(js.publish(subj, payload));
    if (buf.length === batch) {
      await Promise.all(buf);
      buf.length = 0;
    }
  }
  if (buf.length) {
    await Promise.all(buf);
    buf.length = 0;
  }
  return { millis: Date.now() - start, subj, stream };
}

/**
 * Setup a cluster that has N nodes with the first node being just a connection
 * server - rest are JetStream - min number of servers is 3
 * @param count
 * @param debug
 */
async function setupDataConnCluster(
  count = 3,
  debug = false,
): Promise<NatsServer[]> {
  if (count < 3) {
    return Promise.reject(new Error("min cluster is 3"));
  }
  let servers = await NatsServer.jetstreamCluster(count, {}, debug);
  await NatsServer.stopAll(servers);

  servers[0].config.jetstream = "disabled";
  await Deno.rename(
    servers[1].config.jetstream.store_dir,
    `${servers[1].config.jetstream.store_dir}_old`,
  );
  await Deno.rename(
    servers[2].config.jetstream.store_dir,
    `${servers[2].config.jetstream.store_dir}_old`,
  );
  const proms = servers.map((s) => {
    return s.restart();
  });
  servers = await Promise.all(proms);
  await NatsServer.dataClusterFormed(proms.slice(1));
  return servers;
}

async function consumerHbTest(fetch: boolean) {
  const servers = await setupDataConnCluster(3);

  const nc = await connect({ port: servers[0].port });
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get(stream, "a");
  let iter: ConsumerMessages;
  if (fetch) {
    iter = await c.fetch({
      max_messages: 100,
      idle_heartbeat: 1000,
      expires: 30000,
    });
  } else {
    iter = await c.consume({
      max_messages: 100,
      idle_heartbeat: 1000,
      expires: 30000,
    });
  }
  // stop the data serverss
  setTimeout(() => {
    servers[1].stop();
    servers[2].stop();
  }, 1000);
  await (async () => {
    for await (const r of iter) {
      if (r.isError && r.error.message === "idle heartbeats missed") {
        iter.stop();
      }
    }
  })();
  await nc.close();
  await NatsServer.stopAll(servers);
}

Deno.test("consumers - fetch deleted consumer", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get(stream, "a");
  const iter = await c.fetch({
    expires: 30000,
  });
  const dr = deferred();
  setTimeout(() => {
    jsm.consumers.delete(stream, "a")
      .then(() => {
        dr.resolve();
      });
  }, 1000);
  await assertRejects(
    async () => {
      for await (const _m of iter) {
        // nothing
      }
    },
    Error,
    "consumer deleted",
  );
  await dr;
  await cleanup(ns, nc);
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
    expires: 30000,
  });
  const dr = deferred();
  setTimeout(() => {
    jsm.consumers.delete(stream, "a").then(() => dr.resolve());
  }, 1000);

  await assertRejects(
    async () => {
      for await (const _m of iter) {
        // nothing
      }
    },
    Error,
    "consumer deleted",
  );

  await dr;
  await cleanup(ns, nc);
});

Deno.test("consumers - bad options", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get(stream, "a");
  await assertRejects(
    async () => {
      await c.consume({ max_messages: 100, max_bytes: 100 });
    },
    Error,
    "only specify one of max_messages or max_bytes",
  );

  await assertRejects(
    async () => {
      await c.consume({ expires: 500 });
    },
    Error,
    "expires should be at least 1000ms",
  );

  await cleanup(ns, nc);
});

Deno.test("consumers - cleanup handler", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get(stream, "a");
  let iter = await c.consume({
    expires: 30 * 1000,
  }) as PullConsumerMessagesImpl;
  // need consume messages or close will stall
  (async () => {
    for await (const _r of iter) {
      // ignore
    }
  })().then();
  let called = false;
  iter.setCleanupHandler(() => {
    called = true;
    throw new Error("testing");
  });
  await iter.close();
  assertEquals(called, true);

  called = false;
  iter = await c.consume({
    expires: 30 * 1000,
    callback: (_r) => {},
  }) as PullConsumerMessagesImpl;
  iter.setCleanupHandler(() => {
    called = true;
    throw new Error("testing");
  });
  await iter.close();
  assertEquals(called, true);

  await cleanup(ns, nc);
});

Deno.test("consumers - should be able to consume and pull", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const c = await js.consumers.get(stream, "a");

  async function consume(c: Consumer): Promise<void> {
    const iter = await c.consume({ expires: 1500 });
    setTimeout(() => {
      iter.stop();
    }, 1600);
    return (async () => {
      for await (const _r of iter) {
        // ignore
      }
    })().then();
  }

  async function fetch(c: Consumer): Promise<void> {
    const iter = await c.fetch({ expires: 1500 });
    return (async () => {
      for await (const _r of iter) {
        // ignore
      }
    })().then();
  }

  await Promise.all([consume(c), consume(c), fetch(c), fetch(c)]);

  await cleanup(ns, nc);
});

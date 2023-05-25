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

import { initStream } from "./jstest_util.ts";
import {
  assertEquals,
  assertExists,
  assertRejects,
} from "https://deno.land/std@0.125.0/testing/asserts.ts";
import { DeliverPolicy, JsMsg } from "../mod.ts";
import { OrderedPullConsumerImpl } from "../consumer.ts";
import { deferred } from "../../nats-base-client/mod.ts";
import {
  cleanup,
  jetstreamServerConf,
  notCompatible,
  setup,
} from "../../tests/helpers/mod.ts";
import { delay } from "../../nats-base-client/util.ts";

Deno.test("ordered - get", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();

  await assertRejects(
    async () => {
      await js.consumers.get("a");
    },
    Error,
    "stream not found",
  );

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test"] });
  await js.publish("test");

  const oc = await js.consumers.get("test") as OrderedPullConsumerImpl;
  assertExists(oc);

  const ci = await oc.info();
  assertEquals(ci.name, `${oc.namePrefix}_${oc.serial}`);
  assertEquals(ci.num_pending, 1);

  await cleanup(ns, nc);
});

Deno.test("ordered - fetch", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });
  await js.publish("test.a");
  await js.publish("test.b");
  await js.publish("test.c");

  const oc = await js.consumers.get("test") as OrderedPullConsumerImpl;
  assertExists(oc);

  let iter = await oc.fetch({ max_messages: 1 });
  for await (const m of iter) {
    assertEquals(m.subject, "test.a");
    assertEquals(m.seq, 1);
  }

  iter = await oc.fetch({ max_messages: 1 });
  for await (const m of iter) {
    assertEquals(m.subject, "test.b");
    assertEquals(m.seq, 2);
  }

  await cleanup(ns, nc);
});

Deno.test("ordered - fetch reset", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });
  await js.publish("test.a");
  await js.publish("test.b");
  await js.publish("test.c");

  const oc = await js.consumers.get("test") as OrderedPullConsumerImpl;
  assertExists(oc);

  const seen: number[] = new Array(3).fill(0);
  let done = deferred();

  const callback = (m: JsMsg) => {
    const idx = m.seq - 1;
    seen[idx]++;
    // mess with the internals so we see these again
    if (seen[idx] === 1) {
      oc.cursor.deliver_seq--;
      oc.cursor.stream_seq--;
    }
    iter.stop();
    done.resolve();
  };

  let iter = await oc.fetch({
    max_messages: 1,
    //@ts-ignore: callback not exposed
    callback,
  });
  await done;
  done = deferred();

  iter = await oc.fetch({
    max_messages: 1,
    //@ts-ignore: callback not exposed
    callback,
  });
  await done;
  done = deferred();

  iter = await oc.fetch({
    max_messages: 1,
    //@ts-ignore: callback not exposed
    callback,
  });
  await done;
  done = deferred();

  iter = await oc.fetch({
    max_messages: 1,
    //@ts-ignore: callback not exposed
    callback,
  });
  await done;
  done = deferred();

  iter = await oc.fetch({
    max_messages: 1,
    //@ts-ignore: callback not exposed
    callback,
  });
  await done;
  done = deferred();

  iter = await oc.fetch({
    max_messages: 1,
    //@ts-ignore: callback not exposed
    callback,
  });
  await done;

  assertEquals(seen, [2, 2, 2]);
  assertEquals(oc.serial, 6);

  await cleanup(ns, nc);
});

Deno.test("ordered - consume reset", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });
  await js.publish("test.a");
  await js.publish("test.b");
  await js.publish("test.c");

  const oc = await js.consumers.get("test") as OrderedPullConsumerImpl;
  assertExists(oc);

  const seen: number[] = new Array(3).fill(0);
  const done = deferred();

  const callback = (r: JsMsg) => {
    const idx = r.seq - 1;
    seen[idx]++;
    // mess with the internals so we see these again
    if (seen[idx] === 1) {
      oc.cursor.deliver_seq--;
      oc.cursor.stream_seq--;
    }
    if (r.info.pending === 0) {
      iter.stop();
      done.resolve();
    }
  };

  const iter = await oc.consume({
    max_messages: 1,
    callback,
  });
  await done;

  assertEquals(seen, [2, 2, 1]);
  assertEquals(oc.serial, 3);

  await cleanup(ns, nc);
});

Deno.test("ordered - consume", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });
  await js.publish("test.a");
  await js.publish("test.b");
  await js.publish("test.c");

  const oc = await js.consumers.get("test") as OrderedPullConsumerImpl;
  assertExists(oc);

  const iter = await oc.consume({ max_messages: 1 });
  for await (const m of iter) {
    if (m.info.pending === 0) {
      break;
    }
  }

  await cleanup(ns, nc);
});

Deno.test("ordered - filters consume", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  await js.publish("test.a");
  await js.publish("test.b");
  await js.publish("test.c");

  const oc = await js.consumers.get("test", { filterSubjects: ["test.b"] });
  assertExists(oc);

  const iter = await oc.consume();
  for await (const m of iter) {
    assertEquals("test.b", m.subject);
    if (m.info.pending === 0) {
      break;
    }
  }

  assertEquals(iter.getProcessed(), 1);

  await cleanup(ns, nc);
});

Deno.test("ordered - filters fetch", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  await js.publish("test.a");
  await js.publish("test.b");
  await js.publish("test.c");

  const oc = await js.consumers.get("test", { filterSubjects: ["test.b"] });
  assertExists(oc);

  const iter = await oc.fetch({ expires: 1000 });
  for await (const m of iter) {
    assertEquals("test.b", m.subject);
  }
  assertEquals(iter.getProcessed(), 1);

  await cleanup(ns, nc);
});

Deno.test("ordered - fetch reject consumer type change or concurrency", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  const oc = await js.consumers.get("test");
  const iter = await oc.fetch({ expires: 3000 });
  (async () => {
    for await (const _r of iter) {
      // nothing
    }
  })().then();

  await assertRejects(
    async () => {
      await oc.fetch();
    },
    Error,
    "ordered consumer doesn't support concurrent fetch",
  );

  await assertRejects(
    async () => {
      await oc.consume();
    },
    Error,
    "ordered consumer initialized as fetch",
  );

  await iter.stop();

  await cleanup(ns, nc);
});

Deno.test("ordered - consume reject consumer type change or concurrency", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  const oc = await js.consumers.get("test");
  const iter = await oc.consume({ expires: 3000 });
  (async () => {
    for await (const _r of iter) {
      // nothing
    }
  })().then();

  await assertRejects(
    async () => {
      await oc.consume();
    },
    Error,
    "ordered consumer doesn't support concurrent consume",
  );

  await assertRejects(
    async () => {
      await oc.fetch();
    },
    Error,
    "ordered consumer already initialized as consume",
  );

  await iter.stop();

  await cleanup(ns, nc);
});

Deno.test("ordered - last per subject", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  await Promise.all([
    js.publish("test.a"),
    js.publish("test.a"),
  ]);

  let oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.LastPerSubject,
  });
  let iter = await oc.fetch({ max_messages: 1 });
  await (async () => {
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 2);
    }
  })();

  oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.LastPerSubject,
  });
  iter = await oc.consume({ max_messages: 1 });
  await (async () => {
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 2);
      break;
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("ordered - start sequence", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  await Promise.all([
    js.publish("test.a"),
    js.publish("test.b"),
  ]);

  let oc = await js.consumers.get("test", {
    opt_start_seq: 2,
  });

  let iter = await oc.fetch({ max_messages: 1 });
  await (async () => {
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 2);
    }
  })();

  oc = await js.consumers.get("test", {
    opt_start_seq: 2,
  });
  iter = await oc.consume({ max_messages: 1 });
  await (async () => {
    for await (const r of iter) {
      assertEquals(r.info.streamSequence, 2);
      assertEquals(r.subject, "test.b");
      break;
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("ordered - last", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  await Promise.all([
    js.publish("test.a"),
    js.publish("test.b"),
  ]);

  let oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.Last,
  });

  let iter = await oc.fetch({ max_messages: 1 });
  await (async () => {
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 2);
      assertEquals(m.subject, "test.b");
    }
  })();

  oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.Last,
  });
  iter = await oc.consume({ max_messages: 1 });
  await (async () => {
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 2);
      assertEquals(m.subject, "test.b");
      break;
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("ordered - new", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  await Promise.all([
    js.publish("test.a"),
    js.publish("test.b"),
  ]);

  let oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.New,
  });

  let iter = await oc.fetch({ max_messages: 1 });
  await (async () => {
    await js.publish("test.c");
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 3);
      assertEquals(m.subject, "test.c");
    }
  })();

  oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.New,
  });
  iter = await oc.consume({ max_messages: 1 });
  await (async () => {
    await js.publish("test.d");
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 4);
      assertEquals(m.subject, "test.d");
      break;
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("ordered - start time", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test.*"] });

  const js = nc.jetstream();
  await Promise.all([
    js.publish("test.a"),
    js.publish("test.b"),
  ]);

  await delay(500);
  const date = new Date().toISOString();

  let oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.StartTime,
    opt_start_time: date,
  });

  await js.publish("test.c");

  let iter = await oc.fetch({ max_messages: 1 });
  await (async () => {
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 3);
      assertEquals(m.subject, "test.c");
    }
  })();

  oc = await js.consumers.get("test", {
    deliver_policy: DeliverPolicy.StartTime,
    opt_start_time: date,
  });
  iter = await oc.consume({ max_messages: 1 });
  await (async () => {
    for await (const m of iter) {
      assertEquals(m.info.streamSequence, 3);
      assertEquals(m.subject, "test.c");
      break;
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("ordered - next", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "test", subjects: ["test"] });
  const js = nc.jetstream();

  const c = await js.consumers.get("test");
  let m = await c.next({ expires: 1000 });
  assertEquals(m, null);

  await Promise.all([
    js.publish("test"),
    js.publish("test"),
  ]);

  m = await c.next();
  assertEquals(m?.seq, 1);

  m = await c.next();
  assertEquals(m?.seq, 2);

  await cleanup(ns, nc);
});

Deno.test("ordered - sub leaks next()", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream } = await initStream(nc);

  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  const js = nc.jetstream();
  const c = await js.consumers.get(stream);
  await c.next({ expires: 1000 });
  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  await cleanup(ns, nc);
});

Deno.test("ordered - sub leaks fetch()", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream } = await initStream(nc);

  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  const js = nc.jetstream();
  const c = await js.consumers.get(stream);
  const iter = await c.fetch({ expires: 1000 });
  const done = (async () => {
    for await (const _m of iter) {
      // nothing
    }
  })().then();
  await done;
  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  await cleanup(ns, nc);
});

Deno.test("ordered - sub leaks consume()", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream } = await initStream(nc);

  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.size(), 1);
  const js = nc.jetstream();
  const c = await js.consumers.get(stream);
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

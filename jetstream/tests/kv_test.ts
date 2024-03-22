/*
 * Copyright 2021-2023 The NATS Authors
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
  collect,
  compare,
  ConnectionOptions,
  deferred,
  delay,
  Empty,
  NatsConnection,
  NatsConnectionImpl,
  NatsError,
  nuid,
  parseSemVer,
  QueuedIterator,
  StringCodec,
  syncIterator,
} from "../../nats-base-client/internal_mod.ts";

import {
  DirectMsgHeaders,
  DiscardPolicy,
  KV,
  KvEntry,
  KvOptions,
  nanos,
  StorageType,
  StreamSource,
  SubjectTransformConfig,
} from "../mod.ts";

import {
  assert,
  assertArrayIncludes,
  assertEquals,
  assertExists,
  assertRejects,
  assertThrows,
} from "https://deno.land/std@0.200.0/assert/mod.ts";

import {
  Base64KeyCodec,
  Bucket,
  NoopKvCodecs,
  validateBucket,
  validateKey,
} from "../kv.ts";
import {
  cleanup,
  jetstreamServerConf,
  Lock,
  NatsServer,
  notCompatible,
  setup,
} from "../../tests/helpers/mod.ts";
import { QueuedIteratorImpl } from "../../nats-base-client/queued_iterator.ts";
import { connect } from "../../src/mod.ts";
import { JSONCodec } from "https://deno.land/x/nats@v1.10.2/nats-base-client/codec.ts";
import { JetStreamOptions } from "../../nats-base-client/core.ts";
import {
  JetStreamSubscriptionInfoable,
  kvPrefix,
  KvWatchInclude,
} from "../types.ts";

Deno.test("kv - key validation", () => {
  const bad = [
    " x y",
    "x ",
    "x!",
    "xx$",
    "*",
    ">",
    "x.>",
    "x.*",
    ".",
    ".x",
    ".x.",
    "x.",
  ];
  for (const v of bad) {
    assertThrows(
      () => {
        validateKey(v);
      },
      Error,
      "invalid key",
      `expected '${v}' to be invalid key`,
    );
  }

  const good = [
    "foo",
    "_foo",
    "-foo",
    "_kv_foo",
    "foo123",
    "123",
    "a/b/c",
    "a.b.c",
  ];
  for (const v of good) {
    try {
      validateKey(v);
    } catch (err) {
      throw new Error(
        `expected '${v}' to be a valid key, but was rejected: ${err.message}`,
      );
    }
  }
});

Deno.test("kv - bucket name validation", () => {
  const bad = [" B", "!", "x/y", "x>", "x.x", "x.*", "x.>", "x*", "*", ">"];
  for (const v of bad) {
    assertThrows(
      () => {
        validateBucket(v);
      },
      Error,
      "invalid bucket name",
      `expected '${v}' to be invalid bucket name`,
    );
  }

  const good = [
    "B",
    "b",
    "123",
    "1_2_3",
    "1-2-3",
  ];
  for (const v of good) {
    try {
      validateBucket(v);
    } catch (err) {
      throw new Error(
        `expected '${v}' to be a valid bucket name, but was rejected: ${err.message}`,
      );
    }
  }
});

Deno.test("kv - init creates stream", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const js = nc.jetstream();
  await js.views.kv(n);

  streams = await jsm.streams.list().next();
  assertEquals(streams.length, 1);
  assertEquals(streams[0].config.name, `KV_${n}`);

  await cleanup(ns, nc);
});

Deno.test("kv - bind to existing KV", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const js = nc.jetstream();
  await js.views.kv(n, { history: 10 }) as Bucket;

  streams = await jsm.streams.list().next();
  assertEquals(streams.length, 1);
  assertEquals(streams[0].config.name, `KV_${n}`);

  const kv = await js.views.kv(n, { bindOnly: true }) as Bucket;
  const status = await kv.status();
  assertEquals(status.bucket, `${n}`);
  await crud(kv);
  await cleanup(ns, nc);
});

async function crud(bucket: Bucket): Promise<void> {
  const sc = StringCodec();

  const status = await bucket.status();
  assertEquals(status.values, 0);
  assertEquals(status.history, 10);
  assertEquals(status.bucket, bucket.bucket);
  assertEquals(status.ttl, 0);
  assertEquals(status.streamInfo.config.name, `${kvPrefix}${bucket.bucket}`);

  await bucket.put("k", sc.encode("hello"));
  let r = await bucket.get("k");
  assertEquals(sc.decode(r!.value), "hello");

  await bucket.put("k", sc.encode("bye"));
  r = await bucket.get("k");
  assertEquals(sc.decode(r!.value), "bye");

  await bucket.delete("k");
  r = await bucket.get("k");
  assert(r);
  assertEquals(r.operation, "DEL");

  const buf: string[] = [];
  const values = await bucket.history();
  for await (const r of values) {
    buf.push(sc.decode(r.value));
  }
  assertEquals(values.getProcessed(), 3);
  assertEquals(buf.length, 3);
  assertEquals(buf[0], "hello");
  assertEquals(buf[1], "bye");
  assertEquals(buf[2], "");

  const pr = await bucket.purgeBucket();
  assertEquals(pr.purged, 3);
  assert(pr.success);

  const ok = await bucket.destroy();
  assert(ok);

  const streams = await bucket.jsm.streams.list().next();
  assertEquals(streams.length, 0);
}

Deno.test("kv - crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const n = nuid.next();
  const js = nc.jetstream();
  const bucket = await js.views.kv(n, { history: 10 }) as Bucket;
  await crud(bucket);
  await cleanup(ns, nc);
});

Deno.test("kv - codec crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const js = nc.jetstream();
  const bucket = await js.views.kv(n, {
    history: 10,
    codec: {
      key: Base64KeyCodec(),
      value: NoopKvCodecs().value,
    },
  }) as Bucket;
  await crud(bucket);
  await cleanup(ns, nc);
});

Deno.test("kv - history", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const n = nuid.next();
  const js = nc.jetstream();
  const bucket = await js.views.kv(n, { history: 2 });
  let status = await bucket.status();
  assertEquals(status.values, 0);
  assertEquals(status.history, 2);

  await bucket.put("A", Empty);
  await bucket.put("A", Empty);
  await bucket.put("A", Empty);
  await bucket.put("A", Empty);

  status = await bucket.status();
  assertEquals(status.values, 2);
  await cleanup(ns, nc);
});

Deno.test("kv - cleanups/empty", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const n = nuid.next();
  const js = nc.jetstream();
  const bucket = await js.views.kv(n);
  assertEquals(await bucket.get("x"), null);

  const h = await bucket.history();
  assertEquals(h.getReceived(), 0);

  const keys = await collect(await bucket.keys());
  assertEquals(keys.length, 0);

  const nci = nc as NatsConnectionImpl;
  // mux should be created
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

Deno.test("kv - history cleanup", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const n = nuid.next();
  const js = nc.jetstream();
  const bucket = await js.views.kv(n);
  await bucket.put("a", Empty);
  await bucket.put("b", Empty);
  await bucket.put("c", Empty);

  const h = await bucket.history();
  const done = (async () => {
    for await (const _e of h) {
      break;
    }
  })();

  await done;
  const nci = nc as NatsConnectionImpl;
  // mux should be created
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

Deno.test("kv - bucket watch", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const sc = StringCodec();
  const n = nuid.next();
  const js = nc.jetstream();
  const b = await js.views.kv(n, { history: 10 });
  const m: Map<string, string> = new Map();
  const iter = await b.watch();
  const done = (async () => {
    for await (const r of iter) {
      if (r.operation === "DEL") {
        m.delete(r.key);
      } else {
        m.set(r.key, sc.decode(r.value));
      }
      if (r.key === "x") {
        break;
      }
    }
  })();

  await b.put("a", sc.encode("1"));
  await b.put("b", sc.encode("2"));
  await b.put("c", sc.encode("3"));
  await b.put("a", sc.encode("2"));
  await b.put("b", sc.encode("3"));
  await b.delete("c");
  await b.put("x", Empty);

  await done;

  assertEquals(iter.getProcessed(), 7);
  assertEquals(m.get("a"), "2");
  assertEquals(m.get("b"), "3");
  assert(!m.has("c"));
  assertEquals(m.get("x"), "");

  const nci = nc as NatsConnectionImpl;
  // mux should be created
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

async function keyWatch(bucket: Bucket): Promise<void> {
  const sc = StringCodec();
  const m: Map<string, string> = new Map();

  const iter = await bucket.watch({ key: "a.>" });
  const done = (async () => {
    for await (const r of iter) {
      if (r.operation === "DEL") {
        m.delete(r.key);
      } else {
        m.set(r.key, sc.decode(r.value));
      }
      if (r.key === "a.x") {
        break;
      }
    }
  })();

  await bucket.put("a.b", sc.encode("1"));
  await bucket.put("b.b", sc.encode("2"));
  await bucket.put("c.b", sc.encode("3"));
  await bucket.put("a.b", sc.encode("2"));
  await bucket.put("b.b", sc.encode("3"));
  await bucket.delete("c.b");
  await bucket.put("a.x", Empty);
  await done;

  assertEquals(iter.getProcessed(), 3);
  assertEquals(m.get("a.b"), "2");
  assertEquals(m.get("a.x"), "");
}

Deno.test("kv - key watch", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const bucket = await js.views.kv(nuid.next()) as Bucket;
  await keyWatch(bucket);

  const nci = nc as NatsConnectionImpl;
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

Deno.test("kv - codec key watch", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const bucket = await js.views.kv(nuid.next(), {
    history: 10,
    codec: {
      key: Base64KeyCodec(),
      value: NoopKvCodecs().value,
    },
  }) as Bucket;
  await keyWatch(bucket);

  const nci = nc as NatsConnectionImpl;
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

async function keys(b: Bucket): Promise<void> {
  const sc = StringCodec();

  await b.put("a", sc.encode("1"));
  await b.put("b", sc.encode("2"));
  await b.put("c.c.c", sc.encode("3"));
  await b.put("a", sc.encode("2"));
  await b.put("b", sc.encode("3"));
  await b.delete("c.c.c");
  await b.put("x", Empty);

  const keys = await collect(await b.keys());
  assertEquals(keys.length, 3);
  assertArrayIncludes(keys, ["a", "b", "x"]);
}

Deno.test("kv - keys", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next());
  await keys(b as Bucket);

  const nci = nc as NatsConnectionImpl;
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

Deno.test("kv - codec keys", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next(), {
    history: 10,
    codec: {
      key: Base64KeyCodec(),
      value: NoopKvCodecs().value,
    },
  });
  await keys(b as Bucket);

  const nci = nc as NatsConnectionImpl;
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

Deno.test("kv - ttl", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const sc = StringCodec();
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next(), { ttl: 1000 }) as Bucket;

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info(b.stream);
  assertEquals(si.config.max_age, nanos(1000));

  assertEquals(await b.get("x"), null);
  await b.put("x", sc.encode("hello"));
  const e = await b.get("x");
  assert(e);
  assertEquals(sc.decode(e.value), "hello");
  await delay(2000);
  assertEquals(await b.get("x"), null);

  await cleanup(ns, nc);
});

Deno.test("kv - no ttl", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const sc = StringCodec();
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next()) as Bucket;

  await b.put("x", sc.encode("hello"));
  let e = await b.get("x");
  assert(e);
  assertEquals(sc.decode(e.value), "hello");

  await delay(1500);
  e = await b.get("x");
  assert(e);
  assertEquals(sc.decode(e.value), "hello");

  await cleanup(ns, nc);
});

Deno.test("kv - complex key", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const sc = StringCodec();
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next()) as Bucket;

  await b.put("x.y.z", sc.encode("hello"));
  const e = await b.get("x.y.z");
  assertEquals(e?.value, sc.encode("hello"));

  const d = deferred<KvEntry>();
  let iter = await b.watch({ key: "x.y.>" });
  await (async () => {
    for await (const r of iter) {
      d.resolve(r);
      break;
    }
  })();

  const vv = await d;
  assertEquals(vv.value, sc.encode("hello"));

  const dd = deferred<KvEntry>();
  iter = await b.history({ key: "x.y.>" });
  await (async () => {
    for await (const r of iter) {
      dd.resolve(r);
      break;
    }
  })();

  const vvv = await dd;
  assertEquals(vvv.value, sc.encode("hello"));

  const nci = nc as NatsConnectionImpl;
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

Deno.test("kv - remove key", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const sc = StringCodec();
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next()) as Bucket;

  await b.put("a.b", sc.encode("ab"));
  let v = await b.get("a.b");
  assert(v);
  assertEquals(sc.decode(v.value), "ab");

  await b.purge("a.b");
  v = await b.get("a.b");
  assert(v);
  assertEquals(v.operation, "PURGE");

  const status = await b.status();
  // the purged value
  assertEquals(status.values, 1);

  await cleanup(ns, nc);
});

Deno.test("kv - remove subkey", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next()) as Bucket;
  await b.put("a", Empty);
  await b.put("a.b", Empty);
  await b.put("a.c", Empty);

  let keys = await collect(await b.keys());
  assertEquals(keys.length, 3);
  assertArrayIncludes(keys, ["a", "a.b", "a.c"]);

  await b.delete("a.*");
  keys = await collect(await b.keys());
  assertEquals(keys.length, 1);
  assertArrayIncludes(keys, ["a"]);

  await cleanup(ns, nc);
});

Deno.test("kv - create key", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next()) as Bucket;
  const sc = StringCodec();
  await b.create("a", Empty);
  await assertRejects(
    async () => {
      await b.create("a", sc.encode("a"));
    },
    Error,
    "wrong last sequence: 1",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("kv - update key", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next()) as Bucket;
  const sc = StringCodec();
  const seq = await b.create("a", Empty);
  await assertRejects(
    async () => {
      await b.update("a", sc.encode("a"), 100);
    },
    Error,
    "wrong last sequence: 1",
    undefined,
  );

  await b.update("a", sc.encode("b"), seq);

  await cleanup(ns, nc);
});

Deno.test("kv - internal consumer", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  async function getCount(name: string): Promise<number> {
    const js = nc.jetstream();
    const b = await js.views.kv(name) as Bucket;
    const watch = await b.watch() as QueuedIteratorImpl<unknown>;
    const sub = watch._data as JetStreamSubscriptionInfoable;
    return sub?.info?.last?.num_pending || 0;
  }

  const name = nuid.next();
  const js = nc.jetstream();
  const b = await js.views.kv(name) as Bucket;
  assertEquals(await getCount(name), 0);

  await b.put("a", Empty);
  assertEquals(await getCount(name), 1);

  await cleanup(ns, nc);
});

Deno.test("kv - is wildcard delete implemented", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const name = nuid.next();
  const js = nc.jetstream();
  const b = await js.views.kv(name, { history: 10 }) as Bucket;
  await b.put("a", Empty);
  await b.put("a.a", Empty);
  await b.put("a.b", Empty);
  await b.put("a.b.c", Empty);

  let keys = await collect(await b.keys());
  assertEquals(keys.length, 4);

  await b.delete("a.*");
  keys = await collect(await b.keys());
  assertEquals(keys.length, 2);

  // this was a manual delete, so we should have tombstones
  // for all the deleted entries
  let deleted = 0;
  const w = await b.watch();
  await (async () => {
    for await (const e of w) {
      if (e.operation === "DEL") {
        deleted++;
      }
      if (e.delta === 0) {
        break;
      }
    }
  })();
  assertEquals(deleted, 2);

  await nc.close();
  await ns.stop();
});

Deno.test("kv - delta", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const name = nuid.next();
  const js = nc.jetstream();
  const b = await js.views.kv(name) as Bucket;
  await b.put("a", Empty);
  await b.put("a.a", Empty);
  await b.put("a.b", Empty);
  await b.put("a.b.c", Empty);

  const w = await b.history();
  await (async () => {
    let i = 0;
    let delta = 4;
    for await (const e of w) {
      assertEquals(e.revision, ++i);
      assertEquals(e.delta, --delta);
      if (e.delta === 0) {
        break;
      }
    }
  })();

  await nc.close();
  await ns.stop();
});

Deno.test("kv - watch and history headers only", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();
  const b = await js.views.kv("bucket") as Bucket;
  const sc = StringCodec();
  await b.put("key1", sc.encode("aaa"));

  async function getEntry(
    qip: Promise<QueuedIterator<KvEntry>>,
  ): Promise<KvEntry> {
    const iter = await qip;
    const p = deferred<KvEntry>();
    (async () => {
      for await (const e of iter) {
        p.resolve(e);
        break;
      }
    })().then();

    return p;
  }

  async function check(pe: Promise<KvEntry>): Promise<void> {
    const e = await pe;
    assertEquals(e.key, "key1");
    assertEquals(e.value, Empty);
    assertEquals(e.length, 3);
  }

  await check(getEntry(b.watch({ key: "key1", headers_only: true })));
  await check(getEntry(b.history({ key: "key1", headers_only: true })));
  await cleanup(ns, nc);
});

Deno.test("kv - mem and file", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();
  const d = await js.views.kv("default") as Bucket;
  assertEquals((await d.status()).backingStore, StorageType.File);
  assertEquals((await d.status()).storage, StorageType.File);

  const f = await js.views.kv("file", {
    storage: StorageType.File,
  }) as Bucket;
  assertEquals((await f.status()).backingStore, StorageType.File);
  assertEquals((await f.status()).storage, StorageType.File);

  const m = await js.views.kv("mem", {
    storage: StorageType.Memory,
  }) as Bucket;
  assertEquals((await m.status()).backingStore, StorageType.Memory);
  assertEquals((await m.status()).storage, StorageType.Memory);

  await cleanup(ns, nc);
});

Deno.test("kv - example", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream();
  const sc = StringCodec();

  const kv = await js.views.kv("testing", { history: 5 });

  // create an entry - this is similar to a put, but will fail if the
  // key exists
  await kv.create("hello.world", sc.encode("hi"));

  // Values in KV are stored as KvEntries:
  // {
  //   bucket: string,
  //   key: string,
  //   value: Uint8Array,
  //   created: Date,
  //   revision: number,
  //   delta?: number,
  //   operation: "PUT"|"DEL"|"PURGE"
  // }
  // The operation property specifies whether the value was
  // updated (PUT), deleted (DEL) or purged (PURGE).

  // you can monitor values modification in a KV by watching.
  // You can watch specific key subset or everything.
  // Watches start with the latest value for each key in the
  // set of keys being watched - in this case all keys
  const watch = await kv.watch();
  (async () => {
    for await (const _e of watch) {
      // do something with the change
    }
  })().then();

  // update the entry
  await kv.put("hello.world", sc.encode("world"));
  // retrieve the KvEntry storing the value
  // returns null if the value is not found
  const e = await kv.get("hello.world");
  assert(e);
  // initial value of "hi" was overwritten above
  assertEquals(sc.decode(e.value), "world");

  const buf: string[] = [];
  const keys = await kv.keys();
  await (async () => {
    for await (const k of keys) {
      buf.push(k);
    }
  })();
  assertEquals(buf.length, 1);
  assertEquals(buf[0], "hello.world");

  const h = await kv.history({ key: "hello.world" });
  await (async () => {
    for await (const _e of h) {
      // do something with the historical value
      // you can test e.operation for "PUT", "DEL", or "PURGE"
      // to know if the entry is a marker for a value set
      // or for a deletion or purge.
    }
  })();

  // deletes the key - the delete is recorded
  await kv.delete("hello.world");

  // purge is like delete, but all history values
  // are dropped and only the purge remains.
  await kv.purge("hello.world");

  // stop the watch operation above
  watch.stop();

  // danger: destroys all values in the KV!
  await kv.destroy();

  await cleanup(ns, nc);
});

function setupCrossAccount(): Promise<NatsServer> {
  const conf = {
    accounts: {
      A: {
        jetstream: true,
        users: [{ user: "a", password: "a" }],
        exports: [
          { service: "$JS.API.>" },
          { service: "$KV.>" },
          { stream: "forb.>" },
        ],
      },
      B: {
        users: [{ user: "b", password: "b" }],
        imports: [
          { service: { subject: "$KV.>", account: "A" }, to: "froma.$KV.>" },
          { service: { subject: "$JS.API.>", account: "A" }, to: "froma.>" },
          { stream: { subject: "forb.>", account: "A" } },
        ],
      },
    },
  };
  return NatsServer.start(jetstreamServerConf(conf, true));
}

async function makeKvAndClient(
  opts: ConnectionOptions,
  jsopts: Partial<JetStreamOptions> = {},
): Promise<{ nc: NatsConnection; kv: KV }> {
  const nc = await connect(opts);
  const js = nc.jetstream(jsopts);
  const kv = await js.views.kv("a");
  return { nc, kv };
}

Deno.test("kv - cross account history", async () => {
  const ns = await setupCrossAccount();

  async function checkHistory(kv: KV, trace?: string): Promise<void> {
    const ap = deferred();
    const bp = deferred();
    const cp = deferred();
    const ita = await kv.history();
    const done = (async () => {
      for await (const e of ita) {
        if (trace) {
          console.log(`${trace}: ${e.key}`, e);
        }
        switch (e.key) {
          case "A":
            ap.resolve();
            break;
          case "B":
            bp.resolve();
            break;
          case "C":
            cp.resolve();
            break;
          default:
            // nothing
        }
      }
    })();

    await Promise.all([ap, bp, cp]);
    ita.stop();
    await done;
  }
  const { nc: nca, kv: kva } = await makeKvAndClient({
    port: ns.port,
    user: "a",
    pass: "a",
  });
  const sc = StringCodec();
  await kva.put("A", sc.encode("A"));
  await kva.put("B", sc.encode("B"));
  await kva.delete("B");

  const { nc: ncb, kv: kvb } = await makeKvAndClient({
    port: ns.port,
    user: "b",
    pass: "b",
    inboxPrefix: "forb",
  }, { apiPrefix: "froma" });
  await kvb.put("C", sc.encode("C"));

  await Promise.all([checkHistory(kva), checkHistory(kvb)]);

  await cleanup(ns, nca, ncb);
});

Deno.test("kv - cross account watch", async () => {
  const ns = await setupCrossAccount();

  async function checkWatch(kv: KV, trace?: string): Promise<void> {
    const ap = deferred();
    const bp = deferred();
    const cp = deferred();
    const ita = await kv.watch();
    const done = (async () => {
      for await (const e of ita) {
        if (trace) {
          console.log(`${trace}: ${e.key}`, e);
        }
        switch (e.key) {
          case "A":
            ap.resolve();
            break;
          case "B":
            bp.resolve();
            break;
          case "C":
            cp.resolve();
            break;
          default:
            // nothing
        }
      }
    })();

    await Promise.all([ap, bp, cp]);
    ita.stop();
    await done;
  }

  const { nc: nca, kv: kva } = await makeKvAndClient({
    port: ns.port,
    user: "a",
    pass: "a",
  });
  const { nc: ncb, kv: kvb } = await makeKvAndClient({
    port: ns.port,
    user: "b",
    pass: "b",
    inboxPrefix: "forb",
  }, { apiPrefix: "froma" });

  const proms = [checkWatch(kva), checkWatch(kvb)];
  await Promise.all([nca.flush(), ncb.flush()]);

  const sc = StringCodec();
  await kva.put("A", sc.encode("A"));
  await kva.put("B", sc.encode("B"));
  await kvb.put("C", sc.encode("C"));
  await Promise.all(proms);

  await cleanup(ns, nca, ncb);
});

Deno.test("kv - watch iter stops", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();
  const b = await js.views.kv("a") as Bucket;
  const watch = await b.watch();
  const done = (async () => {
    for await (const _e of watch) {
      // do nothing
    }
  })();

  watch.stop();
  await done;
  await cleanup(ns, nc);
});

Deno.test("kv - defaults to discard new - if server 2.7.2", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();
  const b = await js.views.kv("a") as Bucket;
  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info(b.stream);
  const v272 = parseSemVer("2.7.2");
  const serv = (nc as NatsConnectionImpl).getServerVersion();
  assert(serv !== undefined, "should have a server version");
  const v = compare(serv, v272);
  const discard = v >= 0 ? DiscardPolicy.New : DiscardPolicy.Old;
  assertEquals(si.config.discard, discard);
  await cleanup(ns, nc);
});

Deno.test("kv - initialized watch empty", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();

  const b = await js.views.kv("a") as Bucket;
  const d = deferred();
  await b.watch({
    initializedFn: () => {
      d.resolve();
    },
  });

  await d;
  await cleanup(ns, nc);
});

Deno.test("kv - initialized watch with messages", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();

  const b = await js.views.kv("a") as Bucket;
  await b.put("A", Empty);
  await b.put("B", Empty);
  await b.put("C", Empty);
  const d = deferred<number>();
  const iter = await b.watch({
    initializedFn: () => {
      d.resolve();
    },
  });

  (async () => {
    for await (const _e of iter) {
      // ignore
    }
  })().then();
  await d;
  await cleanup(ns, nc);
});

Deno.test("kv - initialized watch with modifications", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();

  const b = await js.views.kv("a") as Bucket;

  await b.put("A", Empty);
  await b.put("B", Empty);
  await b.put("C", Empty);
  const d = deferred<number>();
  setTimeout(async () => {
    for (let i = 0; i < 100; i++) {
      await b.put(i.toString(), Empty);
    }
  });
  const iter = await b.watch({
    initializedFn: () => {
      d.resolve(iter.getProcessed());
    },
  });

  // we are expecting 103
  const lock = Lock(103);
  (async () => {
    for await (const _e of iter) {
      lock.unlock();
    }
  })().then();
  const when = await d;
  // we don't really know when this happened
  assert(103 > when);
  await lock;

  //@ts-ignore: testing
  const sub = iter._data as JetStreamSubscriptionImpl;
  const ci = await sub.consumerInfo();
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.consumer_seq, 103);

  await cleanup(ns, nc);
});

Deno.test("kv - watch init callback exceptions terminate the iterator", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();

  const b = await js.views.kv("a") as Bucket;
  for (let i = 0; i < 10; i++) {
    await b.put(i.toString(), Empty);
  }
  const iter = await b.watch({
    initializedFn: () => {
      throw new Error("crash");
    },
  });

  const d = deferred<Error>();
  try {
    await (async () => {
      for await (const _e of iter) {
        // awaiting the iterator
      }
    })();
  } catch (err) {
    d.resolve(err);
  }
  const err = await d;
  assertEquals(err.message, "crash");
  await cleanup(ns, nc);
});

Deno.test("kv - get revision", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();
  const sc = StringCodec();

  const b = await js.views.kv(nuid.next(), { history: 3 }) as Bucket;

  async function check(key: string, value: string | null, revision = 0) {
    const e = await b.get(key, { revision });
    if (value === null) {
      assertEquals(e, null);
    } else {
      assertEquals(sc.decode(e!.value), value);
    }
  }

  await b.put("A", sc.encode("a"));
  await b.put("A", sc.encode("b"));
  await b.put("A", sc.encode("c"));

  // expect null, as sequence 1, holds "A"
  await check("B", null, 1);

  await check("A", "c");
  await check("A", "a", 1);
  await check("A", "b", 2);

  await b.put("A", sc.encode("d"));
  await check("A", "d");
  await check("A", null, 1);

  await cleanup(ns, nc);
});

Deno.test("kv - purge deletes", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();

  const b = await js.views.kv("a") as Bucket;

  // keep the marker if delete is younger
  await b.put("a", Empty);
  await b.put("b", Empty);
  await b.put("c", Empty);
  await b.delete("a");
  await b.delete("c");
  await delay(1000);
  await b.delete("b");

  const pr = await b.purgeDeletes(700);
  assertEquals(pr.purged, 2);
  assertEquals(await b.get("a"), null);
  assertEquals(await b.get("c"), null);

  const e = await b.get("b");
  assertEquals(e?.operation, "DEL");

  await cleanup(ns, nc);
});

Deno.test("kv - replicas", async () => {
  const servers = await NatsServer.jetstreamCluster(3);
  const nc = await connect({ port: servers[0].port });
  const js = nc.jetstream();

  const b = await js.views.kv("a", { replicas: 3 });
  const status = await b.status();

  const jsm = await nc.jetstreamManager();
  let si = await jsm.streams.info(status.streamInfo.config.name);
  assertEquals(si.config.num_replicas, 3);

  si = await jsm.streams.update(status.streamInfo.config.name, {
    num_replicas: 1,
  });
  assertEquals(si.config.num_replicas, 1);

  await nc.close();
  await NatsServer.stopAll(servers);
});

Deno.test("kv - allow direct", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );

  if (await notCompatible(ns, nc, "2.9.0")) {
    return;
  }
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();

  async function test(
    name: string,
    opts: Partial<KvOptions>,
    direct: boolean,
  ): Promise<void> {
    const kv = await js.views.kv(name, opts) as Bucket;
    assertEquals(kv.direct, direct);
    const si = await jsm.streams.info(kv.bucketName());
    assertEquals(si.config.allow_direct, direct);
  }

  // default is not specified but allowed by the server
  await test(nuid.next(), { history: 1 }, true);
  // user opted to no direct
  await test(nuid.next(), { history: 1, allow_direct: false }, false);
  // user opted for direct
  await test(nuid.next(), { history: 1, allow_direct: true }, true);

  // now we create a kv that enables it
  const xkv = await js.views.kv("X") as Bucket;
  assertEquals(xkv.direct, true);

  // but the client opts-out of the direct
  const xc = await js.views.kv("X", { allow_direct: false }) as Bucket;
  assertEquals(xc.direct, false);

  // now the creator disables it, but the client wants it
  const ykv = await js.views.kv("Y", { allow_direct: false }) as Bucket;
  assertEquals(ykv.direct, false);
  const yc = await js.views.kv("Y", { allow_direct: true }) as Bucket;
  assertEquals(yc.direct, false);

  // now let's pretend we got a server that doesn't support it
  const nci = nc as NatsConnectionImpl;
  nci.features.update("2.8.0");
  nci.info!.version = "2.8.0";

  await assertRejects(
    async () => {
      await test(nuid.next(), { history: 1, allow_direct: true }, false);
    },
    Error,
    "allow_direct is not available on server version",
  );

  await cleanup(ns, nc);
});

Deno.test("kv - direct message", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );

  if (await notCompatible(ns, nc, "2.9.0")) {
    return;
  }

  const js = nc.jetstream();
  const sc = StringCodec();

  const kv = await js.views.kv("a", { allow_direct: true, history: 3 });
  assertEquals(await kv.get("a"), null);

  await kv.put("a", sc.encode("hello"));

  const m = await kv.get("a");
  assert(m !== null);
  assertEquals(m.key, "a");
  assertEquals(m.delta, 0);
  assertEquals(m.revision, 1);
  assertEquals(m.operation, "PUT");
  assertEquals(m.bucket, "a");

  await kv.delete("a");

  const d = await kv.get("a");
  assert(d !== null);
  assertEquals(d.key, "a");
  assertEquals(d.delta, 0);
  assertEquals(d.revision, 2);
  assertEquals(d.operation, "DEL");
  assertEquals(d.bucket, "a");

  await kv.put("c", sc.encode("hi"));
  await kv.put("c", sc.encode("hello"));

  // should not fail
  await kv.get("c");

  const o = await kv.get("c", { revision: 3 });
  assert(o !== null);
  assertEquals(o.revision, 3);

  await cleanup(ns, nc);
});

Deno.test("kv - republish", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.9.0")) {
    return;
  }

  const js = nc.jetstream();
  const kv = await js.views.kv("test", {
    republish: {
      src: ">",
      dest: "republished-kv.>",
    },
  }) as Bucket;

  const sc = StringCodec();

  const sub = nc.subscribe("republished-kv.>", { max: 1 });
  (async () => {
    for await (const m of sub) {
      assertEquals(m.subject, `republished-kv.${kv.subjectForKey("hello")}`);
      assertEquals(sc.decode(m.data), "world");
      assertEquals(m.headers?.get(DirectMsgHeaders.Stream), kv.bucketName());
    }
  })().then();

  await kv.put("hello", sc.encode("world"));
  await sub.closed;
  await cleanup(ns, nc);
});

Deno.test("kv - ttl is in nanos", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();

  const b = await js.views.kv("a", { ttl: 1000 });
  const status = await b.status();
  assertEquals(status.ttl, 1000);
  assertEquals(status.size, 0);

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info("KV_a");
  assertEquals(si.config.max_age, nanos(1000));
  await cleanup(ns, nc);
});

Deno.test("kv - size", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const js = nc.jetstream();

  const b = await js.views.kv("a", { ttl: 1000 });
  let status = await b.status();
  assertEquals(status.size, 0);
  assertEquals(status.size, status.streamInfo.state.bytes);

  const sc = StringCodec();
  await b.put("a", sc.encode("hello"));
  status = await b.status();
  assert(status.size > 0);
  assertEquals(status.size, status.streamInfo.state.bytes);
  await cleanup(ns, nc);
});

Deno.test("kv - mirror cross domain", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({
      server_name: "HUB",
      jetstream: { domain: "HUB" },
    }, true),
  );
  // the ports file doesn't report leaf node
  const varz = await ns.varz() as unknown;

  const { ns: lns, nc: lnc } = await setup(
    jetstreamServerConf({
      server_name: "LEAF",
      jetstream: { domain: "LEAF" },
      leafnodes: {
        remotes: [
          //@ts-ignore: direct query
          { url: `leaf://127.0.0.1:${varz.leaf.port}` },
        ],
      },
    }),
  );

  // setup a KV
  const js = nc.jetstream();
  const kv = await js.views.kv("TEST");
  const sc = StringCodec();
  const m = new Map<string, KvEntry[]>();

  // watch notifications on a on "name"
  async function watch(kv: KV, bucket: string, key: string) {
    const iter = await kv.watch({ key });
    const buf: KvEntry[] = [];
    m.set(bucket, buf);

    return (async () => {
      for await (const e of iter) {
        buf.push(e);
      }
    })().then();
  }

  watch(kv, "test", "name").then();

  await kv.put("name", sc.encode("derek"));
  await kv.put("age", sc.encode("22"));
  await kv.put("v", sc.encode("v"));
  await kv.delete("v");
  await nc.flush();
  let a = m.get("test");
  assert(a);
  assertEquals(a.length, 1);
  assertEquals(sc.decode(a[0].value), "derek");

  const ljs = await lnc.jetstream();
  await ljs.views.kv("MIRROR", {
    mirror: { name: "TEST", domain: "HUB" },
  });

  // setup a Mirror
  const ljsm = await lnc.jetstreamManager();
  let si = await ljsm.streams.info("KV_MIRROR");
  assertEquals(si.config.mirror_direct, true);

  for (let i = 0; i < 2000; i += 500) {
    si = await ljsm.streams.info("KV_MIRROR");
    if (si.state.messages === 3) {
      break;
    }
    await delay(500);
  }
  assertEquals(si.state.messages, 3);

  async function checkEntry(kv: KV, key: string, value: string, op: string) {
    const e = await kv.get(key);
    assert(e);
    assertEquals(e.operation, op);
    if (value !== "") {
      assertEquals(sc.decode(e.value), value);
    }
  }

  async function t(kv: KV, name: string, old?: string) {
    const histIter = await kv.history();
    const hist: string[] = [];
    for await (const e of histIter) {
      hist.push(e.key);
    }

    if (old) {
      await checkEntry(kv, "name", old, "PUT");
      assertEquals(hist.length, 3);
      assertArrayIncludes(hist, ["name", "age", "v"]);
    } else {
      assertEquals(hist.length, 0);
    }

    await kv.put("name", sc.encode(name));
    await checkEntry(kv, "name", name, "PUT");

    await kv.put("v", sc.encode("v"));
    await checkEntry(kv, "v", "v", "PUT");

    await kv.delete("v");
    await checkEntry(kv, "v", "", "DEL");

    const keysIter = await kv.keys();
    const keys: string[] = [];
    for await (const k of keysIter) {
      keys.push(k);
    }
    assertEquals(keys.length, 2);
    assertArrayIncludes(keys, ["name", "age"]);
  }

  const mkv = await ljs.views.kv("MIRROR");

  watch(mkv, "mirror", "name").then();
  await t(mkv, "rip", "derek");
  a = m.get("mirror");
  assert(a);
  assertEquals(a.length, 2);
  assertEquals(sc.decode(a[1].value), "rip");

  // access the origin kv via the leafnode
  const rjs = lnc.jetstream({ domain: "HUB" });
  const rkv = await rjs.views.kv("TEST") as Bucket;
  assertEquals(rkv.prefix, "$KV.TEST");
  watch(rkv, "origin", "name").then();
  await t(rkv, "ivan", "rip");
  await delay(1000);
  a = m.get("origin");
  assert(a);
  assertEquals(a.length, 2);
  assertEquals(sc.decode(a[1].value), "ivan");

  // shutdown the server
  await cleanup(ns, nc);
  await checkEntry(mkv, "name", "ivan", "PUT");

  await cleanup(lns, lnc);
});

Deno.test("kv - previous sequence", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const kv = await js.views.kv("K");

  assertEquals(await kv.put("A", Empty, { previousSeq: 0 }), 1);
  assertEquals(await kv.put("B", Empty, { previousSeq: 0 }), 2);
  assertEquals(await kv.put("A", Empty, { previousSeq: 1 }), 3);
  assertEquals(await kv.put("A", Empty, { previousSeq: 3 }), 4);
  await assertRejects(async () => {
    await kv.put("A", Empty, { previousSeq: 1 });
  });
  assertEquals(await kv.put("B", Empty, { previousSeq: 2 }), 5);

  await cleanup(ns, nc);
});

Deno.test("kv - encoded entry", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const kv = await js.views.kv("K");
  await kv.put("a", StringCodec().encode("hello"));
  await kv.put("b", JSONCodec().encode(5));
  await kv.put("c", JSONCodec().encode(["hello", 5]));

  assertEquals((await kv.get("a"))?.string(), "hello");
  assertEquals((await kv.get("b"))?.json(), 5);
  assertEquals((await kv.get("c"))?.json(), ["hello", 5]);

  await cleanup(ns, nc);
});

Deno.test("kv - create after delete", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const js = nc.jetstream();
  const kv = await js.views.kv("K");
  await kv.create("a", Empty);

  await assertRejects(async () => {
    await kv.create("a", Empty);
  });
  await kv.delete("a");
  await kv.create("a", Empty);
  await kv.purge("a");
  await kv.create("a", Empty);
  await cleanup(ns, nc);
});

Deno.test("kv - string payloads", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const js = nc.jetstream();
  const kv = await js.views.kv("K");
  await kv.create("a", "b");
  let entry = await kv.get("a");
  assertExists(entry);
  assertEquals(entry?.string(), "b");

  await kv.put("a", "c");
  entry = await kv.get("a");
  assertExists(entry);
  assertEquals(entry?.string(), "c");

  await kv.update("a", "d", entry!.revision);
  entry = await kv.get("a");
  assertExists(entry);
  assertEquals(entry?.string(), "d");

  await cleanup(ns, nc);
});

Deno.test("kv - metadata", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }

  const js = nc.jetstream();
  const kv = await js.views.kv("K", { metadata: { hello: "world" } });
  const status = await kv.status();
  assertEquals(status.metadata?.hello, "world");
  await cleanup(ns, nc);
});

Deno.test("kv - watch updates only", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const js = nc.jetstream();
  const kv = await js.views.kv("K");

  await kv.put("a", "a");
  await kv.put("b", "b");

  const d = deferred();
  const iter = await kv.watch({
    include: KvWatchInclude.UpdatesOnly,
    initializedFn: () => {
      d.resolve();
    },
  });

  const notifications: string[] = [];
  (async () => {
    for await (const e of iter) {
      notifications.push(e.key);
    }
  })().then();
  await d;
  await kv.put("c", "c");
  await delay(1000);

  assertEquals(notifications.length, 1);
  assertEquals(notifications[0], "c");

  await cleanup(ns, nc);
});

Deno.test("kv - watch history", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const js = nc.jetstream();
  const kv = await js.views.kv("K", { history: 10 });

  await kv.put("a", "a");
  await kv.put("a", "aa");
  await kv.put("a", "aaa");
  await kv.delete("a");

  const iter = await kv.watch({
    include: KvWatchInclude.AllHistory,
  });

  const notifications: string[] = [];
  (async () => {
    for await (const e of iter) {
      if (e.operation === "DEL") {
        notifications.push(`${e.key}=del`);
      } else {
        notifications.push(`${e.key}=${e.string()}`);
      }
    }
  })().then();
  await kv.put("c", "c");
  await delay(1000);

  assertEquals(notifications.length, 5);
  assertEquals(notifications[0], "a=a");
  assertEquals(notifications[1], "a=aa");
  assertEquals(notifications[2], "a=aaa");
  assertEquals(notifications[3], "a=del");
  assertEquals(notifications[4], "c=c");

  await cleanup(ns, nc);
});

Deno.test("kv - watch history no deletes", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const js = nc.jetstream();
  const kv = await js.views.kv("K", { history: 10 });

  await kv.put("a", "a");
  await kv.put("a", "aa");
  await kv.put("a", "aaa");
  await kv.delete("a");

  const iter = await kv.watch({
    include: KvWatchInclude.AllHistory,
    ignoreDeletes: true,
  });

  const notifications: string[] = [];
  (async () => {
    for await (const e of iter) {
      if (e.operation === "DEL") {
        notifications.push(`${e.key}=del`);
      } else {
        notifications.push(`${e.key}=${e.string()}`);
      }
    }
  })().then();
  await kv.put("c", "c");
  await kv.delete("c");
  await delay(1000);

  assertEquals(notifications.length, 4);
  assertEquals(notifications[0], "a=a");
  assertEquals(notifications[1], "a=aa");
  assertEquals(notifications[2], "a=aaa");
  assertEquals(notifications[3], "c=c");

  await cleanup(ns, nc);
});

Deno.test("kv - republish header handling", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const jsm = await nc.jetstreamManager();
  const n = nuid.next();
  await jsm.streams.add({
    name: n,
    subjects: ["A.>"],
    storage: StorageType.Memory,
    republish: {
      src: ">",
      dest: `$KV.${n}.>`,
    },
  });

  const js = nc.jetstream();
  const kv = await js.views.kv(n);

  nc.publish("A.orange", "hey");
  await js.publish("A.tomato", "hello");
  await kv.put("A.potato", "yo");

  async function check(allow_direct = false): Promise<void> {
    const B = await js.views.kv(n, { allow_direct });
    let e = await B.get("A.orange");
    assertExists(e);

    e = await B.get("A.tomato");
    assertExists(e);

    e = await B.get("A.potato");
    assertExists(e);
  }
  await check();
  await check(true);

  await cleanup(ns, nc);
});

Deno.test("kv - compression", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();
  const s2 = await js.views.kv("compressed", {
    compression: true,
  });
  let status = await s2.status();
  assertEquals(status.compression, true);

  const none = await js.views.kv("none");
  status = await none.status();
  assertEquals(status.compression, false);
  await cleanup(ns, nc);
});

Deno.test("kv - watch start at", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream();
  const kv = await js.views.kv("a");
  await kv.put("a", "1");
  await kv.put("b", "2");
  await kv.put("c", "3");

  const iter = await kv.watch({ resumeFromRevision: 2 });
  await (async () => {
    for await (const o of iter) {
      // expect first key to be "b"
      assertEquals(o.key, "b");
      assertEquals(o.revision, 2);
      break;
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("kv - delete key if revision", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next());
  const seq = await b.create("a", Empty);
  await assertRejects(
    async () => {
      await b.delete("a", { previousSeq: 100 });
    },
    Error,
    "wrong last sequence: 1",
    undefined,
  );

  await b.delete("a", { previousSeq: seq });

  await cleanup(ns, nc);
});

Deno.test("kv - purge key if revision", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const b = await js.views.kv(nuid.next());
  const seq = await b.create("a", Empty);

  await assertRejects(
    async () => {
      await b.purge("a", { previousSeq: 2 });
    },
    Error,
    "wrong last sequence: 1",
    undefined,
  );

  await b.purge("a", { previousSeq: seq });
  await cleanup(ns, nc);
});

Deno.test("kv - bind no info", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  await js.views.kv("A");

  const d = deferred();
  nc.subscribe("$JS.API.STREAM.INFO.>", {
    callback: (_err, msg) => {
      d.reject(new Error("saw stream info"));
    },
  });

  const kv = await js.views.kv("A", { bindOnly: true, allow_direct: true });
  await kv.put("a", "hello");
  const e = await kv.get("a");
  assertEquals(e?.string(), "hello");
  await kv.delete("a");

  d.resolve();
  // shouldn't have rejected earlier
  await d;

  await cleanup(ns, nc);
});

Deno.test("kv - watcher will name and filter", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const js = nc.jetstream();
  const kv = await js.views.kv("A");

  const sub = syncIterator(nc.subscribe("$JS.API.>"));
  const iter = await kv.watch({ key: "a.>" });

  const m = await sub.next();
  assert(m?.subject.startsWith("$JS.API.CONSUMER.CREATE.KV_A."));
  assert(m?.subject.endsWith("$KV.A.a.>"));

  iter.stop();

  await cleanup(ns, nc);
});

Deno.test("kv - mirror check", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );

  const js = nc.jetstream();
  const kv = await js.views.kv("A");

  await Promise.all([
    kv.put("a", "hello"),
    kv.put("b", "hi"),
    kv.put("c", "done"),
  ]);

  const kv2 = await js.views.kv("B", Bucket.mirror({}, "A", "B", "a", "c"));

  // have to wait for the mirror to catch up
  await delay(1000);
  let s2 = await kv2.status();
  assertEquals(s2.streamInfo.state.messages, 2);

  assertExists(await kv2.get("a"));
  assertEquals(await kv2.get("b"), null);
  assertExists(await kv2.get("c"));

  await cleanup(ns, nc);
});

/*
 * Copyright 2021 The NATS Authors
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
import { cleanup, jetstreamServerConf, setup } from "./jstest_util.ts";
import {
  collect,
  deferred,
  delay,
  Empty,
  nanos,
  NatsConnectionImpl,
  nuid,
  QueuedIterator,
  StorageType,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
  assertThrows,
  assertThrowsAsync,
} from "https://deno.land/std@0.95.0/testing/asserts.ts";

import { KvEntry } from "../nats-base-client/types.ts";

import {
  Base64KeyCodec,
  Bucket,
  NoopKvCodecs,
  validateBucket,
  validateKey,
} from "../nats-base-client/kv.ts";
import { notCompatible } from "./helpers/mod.ts";
import { QueuedIteratorImpl } from "../nats-base-client/queued_iterator.ts";
import { JetStreamSubscriptionInfoable } from "../nats-base-client/jsclient.ts";

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

async function crud(bucket: Bucket): Promise<void> {
  const sc = StringCodec();

  const status = await bucket.status();
  assertEquals(status.values, 0);
  assertEquals(status.history, 10);
  assertEquals(status.bucket, bucket.bucketName());
  assertEquals(status.ttl, 0);

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

  await delay(1500);
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
  await assertThrowsAsync(
    async () => {
      await b.create("a", sc.encode("a"));
    },
    Error,
    "wrong last sequence: 1",
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
  await assertThrowsAsync(
    async () => {
      await b.update("a", sc.encode("a"), 100);
    },
    Error,
    "wrong last sequence: 1",
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
    let watch = await b.watch() as QueuedIteratorImpl<unknown>;
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
    let p = deferred<KvEntry>();
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

  const f = await js.views.kv("file", {
    storage: StorageType.File,
  }) as Bucket;
  assertEquals((await f.status()).backingStore, StorageType.File);

  const m = await js.views.kv("mem", {
    storage: StorageType.Memory,
  }) as Bucket;
  assertEquals((await m.status()).backingStore, StorageType.Memory);

  await cleanup(ns, nc);
});

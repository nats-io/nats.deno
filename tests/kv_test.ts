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
  deferred,
  delay,
  Empty,
  EncodedEntry,
  nanos,
  NatsConnectionImpl,
  nuid,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
  assertThrows,
} from "https://deno.land/std@0.95.0/testing/asserts.ts";

import {
  Base64KeyCodec,
  Bucket,
  Entry,
  NoopKvCodecs,
  validateBucket,
  validateKey,
} from "../nats-base-client/kv.ts";
import { EncodedBucket } from "../nats-base-client/ekv.ts";
import { notCompatible } from "./helpers/mod.ts";

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
  if (await notCompatible(ns, nc)) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  await Bucket.create(nc, n);

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

  const pr = await bucket.purge();
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
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  await crud(await Bucket.create(nc, n, { history: 10 }) as Bucket);
  await cleanup(ns, nc);
});

Deno.test("kv - codec crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const bucket = await Bucket.create(nc, n, {
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
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const bucket = await Bucket.create(nc, n, { history: 2 });
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
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const bucket = await Bucket.create(nc, n);
  assertEquals(await bucket.get("x"), null);

  const h = await bucket.history();
  assertEquals(h.getReceived(), 0);

  const keys = await bucket.keys();
  assertEquals(keys.length, 0);

  const nci = nc as NatsConnectionImpl;
  // mux should be created
  nci.protocol.subscriptions.getMux();
  assertEquals(nci.protocol.subscriptions.subs.size, 1);

  await cleanup(ns, nc);
});

Deno.test("kv - bucket watch", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
  const n = nuid.next();
  const b = await Bucket.create(nc, n, { history: 10 });
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
        iter.stop();
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
        iter.stop();
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
  if (await notCompatible(ns, nc)) {
    return;
  }
  const bucket = await Bucket.create(nc, nuid.next()) as Bucket;
  await keyWatch(bucket);

  await cleanup(ns, nc);
});

Deno.test("kv - codec key watch", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const bucket = await Bucket.create(nc, nuid.next(), {
    history: 10,
    codec: {
      key: Base64KeyCodec(),
      value: NoopKvCodecs().value,
    },
  }) as Bucket;
  await keyWatch(bucket);
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

  let keys = await b.keys();
  assertEquals(keys.length, 4);
  assertArrayIncludes(keys, ["a", "b", "c.c.c", "x"]);
}

Deno.test("kv - keys", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const b = await Bucket.create(nc, nuid.next());
  await keys(b as Bucket);
  await cleanup(ns, nc);
});

Deno.test("kv - codec keys", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const b = await Bucket.create(nc, nuid.next(), {
    history: 10,
    codec: {
      key: Base64KeyCodec(),
      value: NoopKvCodecs().value,
    },
  });
  await keys(b as Bucket);
  await cleanup(ns, nc);
});

Deno.test("kv - ttl", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }

  const sc = StringCodec();
  const b = await Bucket.create(nc, nuid.next(), { ttl: 1000 }) as Bucket;
  const eb = new EncodedBucket<string>(b, sc);

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info(b.stream);
  assertEquals(si.config.max_age, nanos(1000));

  assertEquals(await b.get("x"), null);
  await eb.put("x", "hello");
  assertEquals((await eb.get("x"))?.value, "hello");

  await delay(1500);
  assertEquals(await eb.get("x"), null);

  await cleanup(ns, nc);
});

Deno.test("kv - no ttl", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
  const b = await Bucket.create(nc, nuid.next()) as Bucket;
  const eb = new EncodedBucket<string>(b, sc);

  await eb.put("x", "hello");
  assertEquals((await eb.get("x"))?.value, "hello");

  await delay(1500);
  assertEquals((await eb.get("x"))?.value, "hello");

  await cleanup(ns, nc);
});

Deno.test("kv - complex key", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
  const b = await Bucket.create(nc, nuid.next()) as Bucket;

  await b.put("x.y.z", sc.encode("hello"));
  const e = await b.get("x.y.z");
  assertEquals(e?.value, sc.encode("hello"));

  const d = deferred<Entry>();
  let iter = await b.watch({ key: "x.y.>" });
  await (async () => {
    for await (const r of iter) {
      d.resolve(r);
      await iter.stop();
    }
  })();

  const vv = await d;
  assertEquals(vv.value, sc.encode("hello"));

  const dd = deferred<Entry>();
  iter = await b.history({ key: "x.y.>" });
  await (async () => {
    for await (const r of iter) {
      dd.resolve(r);
      iter.stop();
    }
  })();

  const vvv = await dd;
  assertEquals(vvv.value, sc.encode("hello"));

  await cleanup(ns, nc);
});

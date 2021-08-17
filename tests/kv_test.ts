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
  Empty,
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

Deno.test("kv - crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const bucket = await Bucket.create(nc, n, { history: 10 });
  let seq = await bucket.put("k", sc.encode("hello"));
  assertEquals(seq, 1);

  let r = await bucket.get("k");
  assertEquals(sc.decode(r!.value), "hello");

  seq = await bucket.put("k", sc.encode("bye"));
  assertEquals(seq, 2);

  r = await bucket.get("k");
  assertEquals(sc.decode(r!.value), "bye");

  await bucket.delete("k");

  const values = await bucket.history("k");
  for await (const _r of values) {
    // just run through them
  }
  assertEquals(values.getProcessed(), 3);

  const pr = await bucket.purge();
  assertEquals(pr.purged, 3);
  assert(pr.success);

  const ok = await bucket.destroy();
  assert(ok);

  streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  await cleanup(ns, nc);
});

Deno.test("kv - codec crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
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
  });
  let seq = await bucket.put("k", sc.encode("hello"));
  assertEquals(seq, 1);

  let r = await bucket.get("k");
  assertEquals(sc.decode(r!.value), "hello");

  seq = await bucket.put("k", sc.encode("bye"));
  assertEquals(seq, 2);

  r = await bucket.get("k");
  assertEquals(sc.decode(r!.value), "bye");

  await bucket.delete("k");

  const values = await bucket.history("k");
  for await (const _r of values) {
    // just run through them
  }
  assertEquals(values.getProcessed(), 3);

  const pr = await bucket.purge();
  assertEquals(pr.purged, 3);
  assert(pr.success);

  const ok = await bucket.destroy();
  assert(ok);

  streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

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

Deno.test("kv - empty iterator ends", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const bucket = await Bucket.create(nc, n);
  const h = await bucket.history("k");
  assertEquals(h.getReceived(), 0);
  const nci = nc as NatsConnectionImpl;
  // mux should be created
  nci.protocol.subscriptions.getMux();
  assertEquals(nci.protocol.subscriptions.subs.size, 1);
  await cleanup(ns, nc);
});

Deno.test("kv - key watch", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const bucket = await Bucket.create(nc, n);
  const iter = await bucket.watch({ key: "k" });
  const d = deferred<Entry>();
  (async () => {
    for await (const r of iter) {
      d.resolve(r);
      iter.stop();
    }
  })().then();

  await bucket.put("a", Empty);
  assertEquals(iter.getReceived(), 0);

  const sc = StringCodec();
  await bucket.put("k", sc.encode("key"));
  await iter;
  assertEquals(iter.getReceived(), 1);
  const r = await d;
  assertEquals(r.key, "k");
  assertEquals(sc.decode(r.value), "key");

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
  const m: Map<string, string> = new Map();
  const n = nuid.next();
  const bucket = await Bucket.create(nc, n, { history: 10 });

  await bucket.put("a", sc.encode("1"));
  await bucket.put("b", sc.encode("2"));
  await bucket.put("c", sc.encode("3"));
  await bucket.put("a", sc.encode("2"));
  await bucket.put("b", sc.encode("3"));
  await bucket.delete("c");
  await bucket.put("x", Empty);

  const iter = await bucket.watch();
  await (async () => {
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

  assertEquals(iter.getProcessed(), 7);
  assertEquals(m.get("a"), "2");
  assertEquals(m.get("b"), "3");
  assert(!m.has("c"));
  assertEquals(m.get("x"), "");

  await cleanup(ns, nc);
});

Deno.test("kv - keys", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
  const bucket = await Bucket.create(nc, nuid.next());

  await bucket.put("a", sc.encode("1"));
  await bucket.put("b", sc.encode("2"));
  await bucket.put("c", sc.encode("3"));
  await bucket.put("a", sc.encode("2"));
  await bucket.put("b", sc.encode("3"));
  await bucket.delete("c");
  await bucket.put("x", Empty);

  const s = await bucket.keys();
  assertEquals(s.length, 4);
  assertArrayIncludes(s, ["a", "b", "c", "x"]);

  await cleanup(ns, nc);
});

Deno.test("encoded kv - crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const bucket = new EncodedBucket<string>(
    await Bucket.create(nc, n, { history: 10 }) as Bucket,
    sc,
  );

  let seq = await bucket.put("k", "hello");
  assertEquals(seq, 1);

  let r = await bucket.get("k");
  assertEquals(r!.value, "hello");

  seq = await bucket.put("k", "bye");
  assertEquals(seq, 2);

  r = await bucket.get("k");
  assertEquals(r!.value, "bye");

  await bucket.delete("k");

  const values = await bucket.history("k");
  for await (const _r of values) {
    // just run through them
  }
  assertEquals(values.getProcessed(), 3);

  const pr = await bucket.purge();
  assertEquals(pr.purged, 3);
  assert(pr.success);

  const ok = await bucket.destroy();
  assert(ok);

  streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  await cleanup(ns, nc);
});

Deno.test("kv - not found", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }

  const b = await Bucket.create(nc, nuid.next()) as Bucket;
  assertEquals(await b.get("x"), null);

  const sc = StringCodec();
  const eb = new EncodedBucket<string>(b, sc);
  assertEquals(await eb.get("x"), null);

  await cleanup(ns, nc);
});

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
  assertEquals,
} from "https://deno.land/std@0.95.0/testing/asserts.ts";

import { Bucket, Entry } from "../nats-base-client/kv.ts";
import { EncodedBucket } from "../nats-base-client/ekv.ts";

Deno.test("kv - init creates stream", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
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
  const sc = StringCodec();
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const bucket = await Bucket.create(nc, n);
  let seq = await bucket.put("k", sc.encode("hello"));
  assertEquals(seq, 1);

  let r = await bucket.get("k");
  assertEquals(sc.decode(r.value), "hello");

  seq = await bucket.put("k", sc.encode("bye"));
  assertEquals(seq, 2);

  r = await bucket.get("k");
  assertEquals(sc.decode(r.value), "bye");

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

Deno.test("kv - empty iterator ends", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );

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
  const sc = StringCodec();
  const m: Map<string, string> = new Map();
  const n = nuid.next();
  const bucket = await Bucket.create(nc, n);

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
  assertEquals(s.size, 4);
  assert(s.has("a"));
  assert(s.has("b"));
  assert(s.has("c"));
  assert(s.has("x"));

  await cleanup(ns, nc);
});

Deno.test("encoded kv - crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  const sc = StringCodec();
  const jsm = await nc.jetstreamManager();
  let streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const bucket = new EncodedBucket<string>(
    await Bucket.create(nc, n) as Bucket,
    sc,
  );

  let seq = await bucket.put("k", "hello");
  assertEquals(seq, 1);

  let r = await bucket.get("k");
  assertEquals(r.value, "hello");

  seq = await bucket.put("k", "bye");
  assertEquals(seq, 2);

  r = await bucket.get("k");
  assertEquals(r.value, "bye");

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

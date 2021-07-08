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
import { nuid, StringCodec } from "../nats-base-client/internal_mod.ts";
import { assertEquals } from "https://deno.land/std@0.95.0/testing/asserts.ts";

import { Bucket } from "../nats-base-client/kv.ts";

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
  assertEquals(sc.decode(r.data), "hello");

  seq = await bucket.put("k", sc.encode("bye"));
  assertEquals(seq, 2);

  r = await bucket.get("k");
  assertEquals(sc.decode(r.data), "bye");

  await bucket.delete("k");

  const values = await bucket.history("k");
  assertEquals(values.length, 3);

  await cleanup(ns, nc);
});

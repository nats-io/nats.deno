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
  EncodedEntry,
  NatsConnectionImpl,
  nuid,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
} from "https://deno.land/std@0.95.0/testing/asserts.ts";

import {
  Base64KeyCodec,
  Bucket,
  NoopKvCodecs,
} from "../nats-base-client/kv.ts";
import { EncodedBucket } from "../nats-base-client/ekv.ts";
import { notCompatible } from "./helpers/mod.ts";

async function crud(eb: EncodedBucket<string>): Promise<void> {
  const status = await eb.status();
  assertEquals(status.values, 0);
  assertEquals(status.history, 10);
  assertEquals(status.bucket, eb.bucket.bucketName());
  assertEquals(status.ttl, 0);

  await eb.put("k", "hello");
  let r = await eb.get("k");
  assertEquals(r!.value, "hello");

  await eb.put("k", "bye");
  r = await eb.get("k");
  assertEquals(r!.value, "bye");

  await eb.delete("k");
  r = await eb.get("k");
  assert(r);
  assertEquals(r.operation, "DEL");

  const buf: string[] = [];
  const values = await eb.history();
  for await (const r of values) {
    buf.push(r.value ?? "");
  }
  assertEquals(values.getProcessed(), 3);
  assertEquals(buf.length, 3);
  assertEquals(buf[0], "hello");
  assertEquals(buf[1], "bye");
  assertEquals(buf[2], "");

  const pr = await eb.purge();
  assertEquals(pr.purged, 3);
  assert(pr.success);

  const ok = await eb.destroy();
  assert(ok);

  const streams = await eb.bucket.jsm.streams.list().next();
  assertEquals(streams.length, 0);
}

Deno.test("ekv - crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const b = await Bucket.create(nc, n, { history: 10 }) as Bucket;
  await crud(new EncodedBucket<string>(b, StringCodec()));
  await cleanup(ns, nc);
});

Deno.test("ekv - codec crud", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);

  const n = nuid.next();
  const b = await Bucket.create(nc, n, {
    history: 10,
    codec: {
      key: Base64KeyCodec(),
      value: NoopKvCodecs().value,
    },
  }) as Bucket;
  await crud(new EncodedBucket<string>(b, StringCodec()));
  await cleanup(ns, nc);
});

Deno.test("ekv - history", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const b = await Bucket.create(nc, n, { history: 2 });
  const bucket = new EncodedBucket<string>(b as Bucket, StringCodec());
  let status = await bucket.status();
  assertEquals(status.values, 0);
  assertEquals(status.history, 2);

  await bucket.put("A", "");
  await bucket.put("A", "");
  await bucket.put("A", "");
  await bucket.put("A", "");

  status = await bucket.status();
  assertEquals(status.values, 2);
  await cleanup(ns, nc);
});

Deno.test("ekv - cleanups", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const b = await Bucket.create(nc, n) as Bucket;
  const bucket = new EncodedBucket<string>(b as Bucket, StringCodec());

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

Deno.test("ekv - bucket watch", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  const n = nuid.next();
  const b = await Bucket.create(nc, n, { history: 10 });
  const eb = new EncodedBucket<string>(b as Bucket, StringCodec());

  const m: Map<string, string> = new Map();
  const iter = await eb.watch();
  const done = (async () => {
    for await (const r of iter) {
      if (r.operation === "DEL") {
        m.delete(r.key);
      } else {
        m.set(r.key, r.value ?? "");
      }
      if (r.key === "x") {
        iter.stop();
      }
    }
  })();

  await eb.put("a", "1");
  await eb.put("b", "2");
  await eb.put("c", "3");
  await eb.put("a", "2");
  await eb.put("b", "3");
  await eb.delete("c");
  await eb.put("x", "");

  await done;

  assertEquals(iter.getProcessed(), 7);
  assertEquals(m.get("a"), "2");
  assertEquals(m.get("b"), "3");
  assert(!m.has("c"));
  assertEquals(m.get("x"), "");

  await cleanup(ns, nc);
});

async function keyWatch(b: Bucket): Promise<void> {
  const bucket = new EncodedBucket<string>(b, StringCodec());
  const m: Map<string, string> = new Map();

  const iter = await bucket.watch({ key: "a.>" });
  const done = (async () => {
    for await (const r of iter) {
      if (r.operation === "DEL") {
        m.delete(r.key);
      } else {
        m.set(r.key, r.value ?? "");
      }
      if (r.key === "a.x") {
        iter.stop();
      }
    }
  })();

  await bucket.put("a.b", "1");
  await bucket.put("b.b", "2");
  await bucket.put("c.b", "3");
  await bucket.put("a.b", "2");
  await bucket.put("b.b", "3");
  await bucket.delete("c.b");
  await bucket.put("a.x", "");
  await done;

  assertEquals(iter.getProcessed(), 3);
  assertEquals(m.get("a.b"), "2");
  assertEquals(m.get("a.x"), "");
}

Deno.test("ekv - key watch", async () => {
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

Deno.test("ekv - codec key watch", async () => {
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

async function keys(bucket: Bucket): Promise<void> {
  const b = new EncodedBucket(bucket, StringCodec());

  await b.put("a", "1");
  await b.put("b", "2");
  await b.put("c.c.c", "3");
  await b.put("a", "2");
  await b.put("b", "3");
  await b.delete("c.c.c");
  await b.put("x", "");

  const keys = await b.keys();
  assertEquals(keys.length, 4);
  assertArrayIncludes(keys, ["a", "b", "c.c.c", "x"]);
}

Deno.test("ekv - keys", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );
  if (await notCompatible(ns, nc)) {
    return;
  }
  await keys(await Bucket.create(nc, nuid.next()) as Bucket);
  await cleanup(ns, nc);
});

Deno.test("ekv - codec keys", async () => {
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

Deno.test("ekv - complex key", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({}, true),
  );

  console.log("server running on port", nc.info!.port);

  if (await notCompatible(ns, nc)) {
    return;
  }
  const sc = StringCodec();
  const b = await Bucket.create(nc, nuid.next()) as Bucket;
  const eb = new EncodedBucket<string>(b, sc);

  await eb.put("x.y.z", "hello");
  const e = await eb.get("x.y.z");
  assertEquals((e?.value), "hello");

  const d = deferred<EncodedEntry<string>>();
  let iter = await eb.watch({ key: "x.y.>" });
  await (async () => {
    for await (const r of iter) {
      d.resolve(r);
      await iter.stop();
    }
  })();

  const vv = await d;
  assertEquals(vv.value, "hello");

  const dd = deferred<EncodedEntry<string>>();
  iter = await eb.history({ key: "x.y.>" });
  await (async () => {
    for await (const r of iter) {
      dd.resolve(r);
      iter.stop();
    }
  })();

  const vvv = await dd;
  assertEquals(vvv.value, "hello");

  await cleanup(ns, nc);
});

/*
 * Copyright 2022 The NATS Authors
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
import { notCompatible } from "./helpers/mod.ts";
import { ObjectStoreInfoImpl } from "../nats-base-client/objectstore.ts";
import {
  assert,
  assertEquals,
  assertExists,
  equal,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { DataBuffer } from "../nats-base-client/databuffer.ts";
import { crypto } from "https://deno.land/std@0.152.0/crypto/mod.ts";
import { headers, StorageType, StringCodec } from "../nats-base-client/mod.ts";
import { assertRejects } from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { equals } from "https://deno.land/std@0.152.0/bytes/mod.ts";
import { ObjectInfo, ObjectStoreMeta } from "../nats-base-client/types.ts";

function readableStreamFrom(data: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

async function fromReadableStream(
  rs: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const buf = new DataBuffer();
  const reader = rs.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      return buf.drain();
    }
    if (value && value.length) {
      buf.fill(value);
    }
  }
}

function makeData(n: number): Uint8Array {
  const data = new Uint8Array(n);
  let index = 0;
  let bytes = n;
  while (true) {
    if (bytes === 0) {
      break;
    }
    const len = bytes > 65536 ? 65536 : bytes;
    bytes -= len;
    const buf = new Uint8Array(len);
    crypto.getRandomValues(buf);
    data.set(buf, index);
    index += buf.length;
  }
  return data;
}

Deno.test("objectstore - basics", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const blob = new Uint8Array(65536);
  crypto.getRandomValues(blob);

  const js = nc.jetstream();
  const os = await js.views.os("OBJS", { description: "testing" });

  const oi = await os.put({ name: "BLOB" }, readableStreamFrom(blob));
  assertEquals(oi.bucket, "OBJS");
  assertEquals(oi.nuid.length, 22);
  assertEquals(oi.name, "BLOB");
  // assert(1000 > (Date.now() - millis(oi.mtime)));

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info("OBJ_OBJS");
  assertExists(si);

  const osi = await os.seal();
  assertEquals(osi.sealed, true);
  assert(osi.size > blob.length);
  assertEquals(osi.storage, StorageType.File);
  assertEquals(osi.description, "testing");

  let or = await os.get("foo");
  assertEquals(or, null);

  or = await os.get("BLOB");
  assertExists(or);
  const read = await fromReadableStream(or!.data);
  equal(read, blob);

  assertEquals(await os.destroy(), true);
  await assertRejects(
    async () => {
      await jsm.streams.info("OBJ_OBJS");
    },
    Error,
    "stream not found",
  );

  await cleanup(ns, nc);
});

Deno.test("objectstore - default status", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test", { description: "testing" });
  const blob = new Uint8Array(65536);
  crypto.getRandomValues(blob);
  await os.put({ name: "BLOB" }, readableStreamFrom(blob));

  const status = await os.status();
  assertEquals(status.backingStore, "JetStream");
  assertEquals(status.bucket, "test");
  const si = status as ObjectStoreInfoImpl;
  assertEquals(si.si.config.name, "OBJ_test");

  await cleanup(ns, nc);
});

Deno.test("objectstore - chunked content", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    jetstream: {
      max_memory_store: 10 * 1024 * 1024 + 33,
    },
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test", { storage: StorageType.Memory });

  const data = makeData(nc.info!.max_payload * 3);
  await os.put(
    { name: "blob", options: { max_chunk_size: nc.info!.max_payload } },
    readableStreamFrom(data),
  );

  const d = await os.get("blob");
  const vv = await fromReadableStream(d!.data);
  equals(vv, data);

  await cleanup(ns, nc);
});

Deno.test("objectstore - multi content", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test", { storage: StorageType.Memory });

  const a = makeData(128);
  await os.put(
    { name: "a.js", options: { max_chunk_size: 1 } },
    readableStreamFrom(a),
  );
  const sc = StringCodec();
  const b = sc.encode("hello world from object store");
  await os.put(
    { name: "b.js", options: { max_chunk_size: nc.info!.max_payload } },
    readableStreamFrom(b),
  );

  let d = await os.get("a.js");
  let vv = await fromReadableStream(d!.data);
  equals(vv, a);

  d = await os.get("b.js");
  vv = await fromReadableStream(d!.data);
  equals(vv, b);

  await cleanup(ns, nc);
});

Deno.test("objectstore - delete markers", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test", { storage: StorageType.Memory });

  const a = makeData(128);
  await os.put(
    { name: "a", options: { max_chunk_size: 10 } },
    readableStreamFrom(a),
  );

  const p = await os.delete("a");
  assertEquals(p.purged, 13);

  const info = await os.info("a");
  assertExists(info);
  assertEquals(info!.deleted, true);

  await cleanup(ns, nc);
});

Deno.test("objectstore - multi with delete", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test", { storage: StorageType.Memory });

  const sc = StringCodec();

  await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("a!")),
  );

  const si = await os.status({ subjects_filter: ">" }) as ObjectStoreInfoImpl;
  await os.put(
    { name: "b", options: { max_chunk_size: nc.info!.max_payload } },
    readableStreamFrom(sc.encode("b!")),
  );

  await os.get("b");
  await os.delete("b");

  const s2 = await os.status({ subjects_filter: ">" }) as ObjectStoreInfoImpl;
  // should have the tumbstone for the deleted subject
  assertEquals(s2.si.state.messages, si.si.state.messages + 1);

  await cleanup(ns, nc);
});

Deno.test("objectstore - object names", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test", { storage: StorageType.Memory });
  const sc = StringCodec();
  await os.put({ name: "blob.txt" }, readableStreamFrom(sc.encode("A")));
  await os.put({ name: "foo bar" }, readableStreamFrom(sc.encode("A")));
  await os.put({ name: " " }, readableStreamFrom(sc.encode("A")));

  await assertRejects(async () => {
    await os.put({ name: "*" }, readableStreamFrom(sc.encode("A")));
  });
  await assertRejects(async () => {
    await os.put({ name: ">" }, readableStreamFrom(sc.encode("A")));
  });
  await assertRejects(async () => {
    await os.put({ name: "" }, readableStreamFrom(sc.encode("A")));
  });
  await cleanup(ns, nc);
});

Deno.test("objectstore - metadata", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test", { storage: StorageType.Memory });
  const sc = StringCodec();

  await os.put({ name: "a" }, readableStreamFrom(sc.encode("A")));

  // rename a
  let meta = { name: "b" } as ObjectStoreMeta;
  await os.update("a", meta);
  let info = await os.info("b");
  assertExists(info);
  assertEquals(info!.name, "b");

  // add some headers
  meta = {} as ObjectStoreMeta;
  meta.headers = headers();
  meta.headers.set("color", "red");
  await os.update("b", meta);

  info = await os.info("b");
  assertExists(info);
  assertEquals(info!.headers?.get("color"), "red");

  await cleanup(ns, nc);
});

Deno.test("objectstore - empty entry", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("empty");

  const oi = await os.put(
    { name: "empty" },
    readableStreamFrom(new Uint8Array(0)),
  );
  assertEquals(oi.nuid.length, 22);
  assertEquals(oi.name, "empty");

  const or = await os.get("empty");
  assert(or !== null);
  assertEquals(await or.error, null);
  const v = await fromReadableStream(or.data);
  assertEquals(v.length, 0);

  await cleanup(ns, nc);
});

Deno.test("objectstore - list", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");
  let infos = await os.list();
  assertEquals(infos.length, 0);

  await os.put(
    { name: "a" },
    readableStreamFrom(new Uint8Array(0)),
  );

  infos = await os.list();
  assertEquals(infos.length, 1);
  assertEquals(infos[0].name, "a");

  await cleanup(ns, nc);
});

Deno.test("objectstore - watch initially empty", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");

  const buf: ObjectInfo[] = [];
  const iter = await os.watch({ includeHistory: true });
  const done = (async () => {
    for await (const info of iter) {
      if (info === null) {
        assertEquals(buf.length, 0);
      } else {
        buf.push(info);
        if (buf.length === 3) {
          break;
        }
      }
    }
  })();
  const infos = await os.list();
  assertEquals(infos.length, 0);

  const sc = StringCodec();
  await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("a")),
  );

  await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("aa")),
  );

  await os.put({ name: "b" }, readableStreamFrom(sc.encode("b")));

  await done;

  assertEquals(buf.length, 3);
  assertEquals(buf[0].name, "a");
  assertEquals(buf[0].size, 1);
  assertEquals(buf[1].name, "a");
  assertEquals(buf[1].size, 2);
  assertEquals(buf[2].name, "b");
  assertEquals(buf[2].size, 1);

  await cleanup(ns, nc);
});

Deno.test("objectstore - watch skip history", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");

  const sc = StringCodec();
  await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("a")),
  );

  await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("aa")),
  );

  const buf: ObjectInfo[] = [];
  const iter = await os.watch({ includeHistory: false });
  const done = (async () => {
    for await (const info of iter) {
      if (info === null) {
        assertEquals(buf.length, 1);
      } else {
        buf.push(info);
        if (buf.length === 1) {
          break;
        }
      }
    }
  })();

  await os.put({ name: "c" }, readableStreamFrom(sc.encode("c")));

  await done;

  assertEquals(buf.length, 1);
  assertEquals(buf[0].name, "c");
  assertEquals(buf[0].size, 1);

  await cleanup(ns, nc);
});

Deno.test("objectstore - watch history", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");

  const sc = StringCodec();
  await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("a")),
  );

  await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("aa")),
  );

  const buf: ObjectInfo[] = [];
  const iter = await os.watch({ includeHistory: true });
  const done = (async () => {
    for await (const info of iter) {
      if (info === null) {
        assertEquals(buf.length, 1);
      } else {
        buf.push(info);
        if (buf.length === 2) {
          break;
        }
      }
    }
  })();

  await os.put({ name: "c" }, readableStreamFrom(sc.encode("c")));

  await done;

  assertEquals(buf.length, 2);
  assertEquals(buf[0].name, "a");
  assertEquals(buf[0].size, 2);
  assertEquals(buf[1].name, "c");
  assertEquals(buf[1].size, 1);

  await cleanup(ns, nc);
});

Deno.test("objectstore - self link", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");

  const sc = StringCodec();
  const src = await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("a")),
  );
  const oi = await os.link("ref", src);
  assertEquals(oi.options?.link, undefined);
  assertEquals(oi.nuid, src.nuid);

  const a = await os.list();
  assertEquals(a.length, 2);
  assertEquals(a[0].name, "a");
  assertEquals(a[1].name, "ref");

  await cleanup(ns, nc);
});

Deno.test("objectstore - external link", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");

  const sc = StringCodec();
  const src = await os.put(
    { name: "a" },
    readableStreamFrom(sc.encode("a")),
  );

  const os2 = await js.views.os("another");
  const io = await os2.link("ref", src);
  assertExists(io.options?.link);
  assertEquals(io.options?.link?.bucket, "test");
  assertEquals(io.options?.link?.name, "a");

  await cleanup(ns, nc);
});

Deno.test("objectstore - store link", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");

  const os2 = await js.views.os("another");
  const si = await os2.linkStore("src", os);
  assertExists(si.options?.link);
  assertEquals(si.options?.link?.bucket, "test");

  await cleanup(ns, nc);
});

Deno.test("objectstore - max chunk is max payload", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 8 * 1024,
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  assertEquals(nc.info?.max_payload, 8 * 1024);

  const js = nc.jetstream();
  const os = await js.views.os("test");

  const rs = readableStreamFrom(makeData(32 * 1024));

  const info = await os.put({ name: "t" }, rs);
  assertEquals(info.size, 32 * 1024);
  assertEquals(info.chunks, 4);
  assertEquals(info.options?.max_chunk_size, 8 * 1024);

  await cleanup(ns, nc);
});

Deno.test("objectstore - default chunk is 128k", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  assertEquals(nc.info?.max_payload, 1024 * 1024);

  const js = nc.jetstream();
  const os = await js.views.os("test");

  const rs = readableStreamFrom(makeData(129 * 1024));

  const info = await os.put({ name: "t" }, rs);
  assertEquals(info.size, 129 * 1024);
  assertEquals(info.chunks, 2);
  assertEquals(info.options?.max_chunk_size, 128 * 1024);

  await cleanup(ns, nc);
});

Deno.test("objectstore - sanitize", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");
  await os.put({ name: "has.dots.here" }, readableStreamFrom(makeData(1)));
  await os.put(
    { name: "the spaces are here" },
    readableStreamFrom(makeData(1)),
  );

  const info = await os.status({ subjects_filter: ">" }) as ObjectStoreInfoImpl;
  assertEquals(info.si.state?.subjects!["$O.test.M.has_dots_here"], 1);
  assertEquals(info.si.state.subjects!["$O.test.M.the_spaces_are_here"], 1);

  await cleanup(ns, nc);
});

// Deno.test("objectstore - compat", async () => {
//   const nc = await connect();
//   const js = nc.jetstream();
//   const os = await js.views.os("test");
//   console.log(await os.status({ subjects_filter: ">" }));
//
//   const a = await os.list();
//   console.log(a);
//
//   const rs = await os.get("./main.go");
//   const data = await fromReadableStream(rs!.data);
//   const sc = StringCodec();
//   console.log(sc.decode(data));
//
//   await os.put({ name: "hello" }, readableStreamFrom(sc.encode("hello world")));
//
//   await nc.close();
// });

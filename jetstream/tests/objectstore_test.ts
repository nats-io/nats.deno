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
  jetstreamServerConf,
  notCompatible,
  setup,
} from "../../tests/helpers/mod.ts";
import {
  assert,
  assertEquals,
  assertExists,
  assertRejects,
  equal,
} from "https://deno.land/std@0.190.0/testing/asserts.ts";
import { DataBuffer } from "../../nats-base-client/databuffer.ts";
import { crypto } from "https://deno.land/std@0.190.0/crypto/mod.ts";
import { ObjectInfo, ObjectStoreMeta, StorageType } from "../mod.ts";
import { Empty, headers, StringCodec } from "../../src/mod.ts";
import { equals } from "https://deno.land/std@0.190.0/bytes/mod.ts";
import { SHA256 } from "../../nats-base-client/sha256.js";
import { Base64UrlPaddedCodec } from "../../nats-base-client/base64.ts";
import { digestType, ObjectStoreImpl } from "../objectstore.ts";

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

function digest(data: Uint8Array): string {
  const sha = new SHA256();
  sha.update(data);
  const digest = sha.digest("base64");
  const pad = digest.length % 3;
  const padding = pad > 0 ? "=".repeat(pad) : "";
  return `${digestType}${digest}${padding}`;
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

  const info = await os.status();
  assertEquals(info.description, "testing");
  assertEquals(info.ttl, 0);
  assertEquals(info.replicas, 1);
  assertEquals(info.streamInfo.config.name, "OBJ_OBJS");

  const oi = await os.put(
    { name: "BLOB", description: "myblob" },
    readableStreamFrom(blob),
  );
  assertEquals(oi.bucket, "OBJS");
  assertEquals(oi.nuid.length, 22);
  assertEquals(oi.name, "BLOB");
  assertEquals(oi.digest, digest(blob));
  assertEquals(oi.description, "myblob");
  assertEquals(oi.deleted, false);
  assert(typeof oi.mtime === "string");

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
  assertEquals(status.streamInfo.config.name, "OBJ_test");

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
  assertEquals(d!.info.digest, digest(data));
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

Deno.test("objectstore - get on deleted returns error", async () => {
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

  const r = await os.get("a");
  assertEquals(r, null);

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

  const si = await os.status({ subjects_filter: ">" });
  await os.put(
    { name: "b", options: { max_chunk_size: nc.info!.max_payload } },
    readableStreamFrom(sc.encode("b!")),
  );

  await os.get("b");
  await os.delete("b");

  const s2 = await os.status({ subjects_filter: ">" });
  // should have the tumbstone for the deleted subject
  assertEquals(s2.streamInfo.state.messages, si.streamInfo.state.messages + 1);

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
  if (os.version() === 1) {
    await os.put({ name: "foo bar" }, readableStreamFrom(sc.encode("A")));
  } else {
    await assertRejects(
      async () => {
        await os.put({ name: "foo bar" }, readableStreamFrom(sc.encode("A")));
      },
      Error,
      "invalid key",
    );
  }
  if (os.version() === 1) {
    await os.put({ name: " " }, readableStreamFrom(sc.encode("A")));
  } else {
    await assertRejects(
      async () => {
        await os.put({ name: "foo bar" }, readableStreamFrom(sc.encode("A")));
      },
      Error,
      "invalid key",
    );
  }

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
  assertEquals(oi.digest, digest(new Uint8Array(0)));
  assertEquals(oi.chunks, 0);

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
  if (os.version() === 1) {
    await os.put(
      { name: "the spaces are here" },
      readableStreamFrom(makeData(1)),
    );
  } else {
    await assertRejects(
      async () => {
        await os.put(
          { name: "the spaces are here" },
          readableStreamFrom(makeData(1)),
        );
      },
      Error,
      "invalid key",
    );
  }

  const info = await os.status({
    subjects_filter: ">",
  });
  const osi = os as ObjectStoreImpl;
  assertEquals(
    info.streamInfo.state
      ?.subjects![osi.keys.metaSubject("has.dots.here")],
    1,
  );
  if (os.version() === 1) {
    assertEquals(
      info.streamInfo.state
        .subjects![
          osi.keys.metaSubject("the spaces are here")
        ],
      1,
    );
  }

  await cleanup(ns, nc);
});

Deno.test("objectstore - partials", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");
  const sc = StringCodec();

  const data = sc.encode("".padStart(7, "a"));

  const info = await os.put(
    { name: "test", options: { max_chunk_size: 2 } },
    readableStreamFrom(data),
  );
  assertEquals(info.chunks, 4);
  assertEquals(info.digest, digest(data));

  const rs = await os.get("test");
  const reader = rs!.data.getReader();
  let i = 0;
  while (true) {
    i++;
    const { done, value } = await reader.read();
    if (done) {
      assertEquals(i, 5);
      break;
    }
    if (i === 4) {
      assertEquals(value!.length, 1);
    } else {
      assertEquals(value!.length, 2);
    }
  }
  await cleanup(ns, nc);
});

Deno.test("objectstore - no store", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");
  await os.put({ name: "test" }, readableStreamFrom(Empty));
  await os.delete("test");
  const oi = await os.info("test");
  await assertRejects(
    async () => {
      await os.link("bar", oi!);
    },
    Error,
    "object is deleted",
  );

  const r = await os.delete("foo");
  assertEquals(r, { purged: 0, success: false });

  await assertRejects(
    async () => {
      await os.update("baz", oi!);
    },
    Error,
    "object not found",
  );

  const jsm = await nc.jetstreamManager();
  await jsm.streams.delete("OBJ_test");
  await assertRejects(
    async () => {
      await os.seal();
    },
    Error,
    "object store not found",
  );

  await assertRejects(
    async () => {
      await os.status();
    },
    Error,
    "object store not found",
  );

  await assertRejects(
    async () => {
      await os.put({ name: "foo" }, readableStreamFrom(Empty));
    },
    Error,
    "stream not found",
  );

  await cleanup(ns, nc);
});

Deno.test("objectstore - hashtests", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
    jetstream: {
      max_file_store: 1024 * 1024 * 2,
    },
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("hashes");

  const base =
    "https://raw.githubusercontent.com/nats-io/nats.client.deps/main/digester_test/";
  const tests: { hash: string; file: string }[] = [{
    hash: "IdgP4UYMGt47rgecOqFoLrd24AXukHf5-SVzqQ5Psg8=",
    file: "digester_test_bytes_000100.txt",
  }, {
    hash: "DZj4RnBpuEukzFIY0ueZ-xjnHY4Rt9XWn4Dh8nkNfnI=",
    file: "digester_test_bytes_001000.txt",
  }, {
    hash: "RgaJ-VSJtjNvgXcujCKIvaheiX_6GRCcfdRYnAcVy38=",
    file: "digester_test_bytes_010000.txt",
  }, {
    hash: "yan7pwBVnC1yORqqgBfd64_qAw6q9fNA60_KRiMMooE=",
    file: "digester_test_bytes_100000.txt",
  }];

  for (let i = 0; i < tests.length; i++) {
    const t = tests[i];
    const r = await fetch(`${base}${t.file}`);
    const rs = await r.blob();

    const oi = await os.put(
      { name: t.hash, options: { max_chunk_size: 9 } },
      rs.stream(),
    );
    assertEquals(oi.digest, `${digestType}${t.hash}`);
  }

  await cleanup(ns, nc);
});

Deno.test("objectstore - meta update", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const js = nc.jetstream();
  const os = await js.views.os("test");

  // cannot update meta of an object that doesn't exist
  await assertRejects(
    async () => {
      await os.update("A", { name: "B" });
    },
    Error,
    "object not found",
  );

  // cannot update the meta of a deleted object
  await os.put({ name: "D" }, readableStreamFrom(makeData(1)));
  await os.delete("D");
  await assertRejects(
    async () => {
      await os.update("D", { name: "DD" });
    },
    Error,
    "cannot update meta for a deleted object",
  );

  // cannot update the meta to an object that already exists
  await os.put({ name: "A" }, readableStreamFrom(makeData(1)));
  await os.put({ name: "B" }, readableStreamFrom(makeData(1)));

  await assertRejects(
    async () => {
      await os.update("A", { name: "B" });
    },
    Error,
    "an object already exists with that name",
  );

  await cleanup(ns, nc);
});

Deno.test("objectstore - cannot put links", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
  }, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const sc = StringCodec();
  const js = nc.jetstream();
  const os = await js.views.os("test");

  const link = { bucket: "test", name: "a" };
  const mm = {
    name: "ref",
    options: { link: link },
  } as ObjectStoreMeta;

  await assertRejects(
    async () => {
      await os.put(mm, readableStreamFrom(sc.encode("a")));
    },
    Error,
    "link cannot be set when putting the object in bucket",
  );

  await cleanup(ns, nc);
});

Deno.test("objectstore - put purges old entries", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const js = nc.jetstream();
  const os = await js.views.os("OBJS", { description: "testing" });

  // we expect 10 messages per put
  const t = async (first: number, last: number) => {
    const status = await os.status();
    const si = status.streamInfo;
    assertEquals(si.state.first_seq, first);
    assertEquals(si.state.last_seq, last);
  };

  const blob = new Uint8Array(9);
  let oi = await os.put(
    { name: "BLOB", description: "myblob", options: { max_chunk_size: 1 } },
    readableStreamFrom(crypto.getRandomValues(blob)),
  );
  assertEquals(oi.revision, 10);
  await t(1, 10);

  oi = await os.put(
    { name: "BLOB", description: "myblob", options: { max_chunk_size: 1 } },
    readableStreamFrom(crypto.getRandomValues(blob)),
  );
  assertEquals(oi.revision, 20);
  await t(11, 20);
  await cleanup(ns, nc);
});

Deno.test("objectstore - put previous sequences", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const js = nc.jetstream();
  const os = await js.views.os("OBJS", { description: "testing" });

  // putting the first
  let oi = await os.put(
    { name: "A", options: { max_chunk_size: 1 } },
    readableStreamFrom(crypto.getRandomValues(new Uint8Array(9))),
    { previousRevision: 0 },
  );
  assertEquals(oi.revision, 10);

  // putting another value, but the first value for the key - so previousRevision is 0
  oi = await os.put(
    { name: "B", options: { max_chunk_size: 1 } },
    readableStreamFrom(crypto.getRandomValues(new Uint8Array(3))),
    { previousRevision: 0 },
  );
  assertEquals(oi.revision, 14);

  // update A, previous A is found at 10
  oi = await os.put(
    { name: "A", options: { max_chunk_size: 1 } },
    readableStreamFrom(crypto.getRandomValues(new Uint8Array(3))),
    { previousRevision: 10 },
  );
  assertEquals(oi.revision, 18);

  // update A, previous A is found at 18
  oi = await os.put(
    { name: "A", options: { max_chunk_size: 1 } },
    readableStreamFrom(Empty),
    { previousRevision: 18 },
  );
  assertEquals(oi.revision, 19);

  await cleanup(ns, nc);
});

Deno.test("objectstore - put/get blob", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const js = nc.jetstream();
  const os = await js.views.os("OBJS", { description: "testing" });

  const payload = new Uint8Array(9);

  // putting the first
  await os.put(
    { name: "A", options: { max_chunk_size: 1 } },
    readableStreamFrom(payload),
    { previousRevision: 0 },
  );

  let bg = await os.getBlob("A");
  assertExists(bg);
  assertEquals(bg.length, payload.length);
  assertEquals(bg, payload);

  await os.putBlob({ name: "B", options: { max_chunk_size: 1 } }, payload);

  bg = await os.getBlob("B");
  assertExists(bg);
  assertEquals(bg.length, payload.length);
  assertEquals(bg, payload);

  await cleanup(ns, nc);
});

Deno.test("objectstore - v1/v2", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }

  const js = nc.jetstream();
  const v1 = await js.views.os("v1", { description: "testing", version: 1 });
  assertEquals(v1.version(), 1);
  await v1.putBlob({ name: "A" }, new Uint8Array(10));

  let { subjects } = (await v1.status()).streamInfo.config;
  assert(subjects[0].startsWith("$O."));

  let v = await js.views.os("v1");
  assertEquals(v.version(), 1);
  let data = await v.getBlob("A");
  assertEquals(data?.length, 10);

  const v2 = await js.views.os("v2", { version: 2 });
  assertEquals(v2.version(), 2);
  await v2.putBlob({ name: "A" }, new Uint8Array(11));

  v = await js.views.os("v2", { version: 1 });
  assertEquals(v.version(), 2);
  data = await v.getBlob("A");
  assertEquals(data?.length, 11);

  let v3 = await js.views.os("v3", { description: "testing" });
  const sc = (await v3.status()).streamInfo.config;
  subjects = sc.subjects.map((s) => {
    return s.replaceAll("$O2.", "$O3.");
  });
  const jsm = await js.jetstreamManager();
  await jsm.streams.update(sc.name, { subjects });

  await assertRejects(
    async () => {
      await js.views.os("v3");
    },
    Error,
    "unknown objectstore version configuration",
  );

  await cleanup(ns, nc);
});

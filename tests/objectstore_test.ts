import { cleanup, jetstreamServerConf, setup } from "./jstest_util.ts";
import { notCompatible } from "./helpers/mod.ts";
import { ObjectStoreInfoImpl } from "../nats-base-client/objectstore.ts";
import {
  assert,
  assertEquals,
  assertExists,
  equal,
} from "https://deno.land/std@0.95.0/testing/asserts.ts";
import { DataBuffer } from "../nats-base-client/databuffer.ts";
import { crypto } from "https://deno.land/std@0.136.0/crypto/mod.ts";
import {
  headers,
  millis,
  StorageType,
  StringCodec,
} from "../nats-base-client/mod.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";
import { equals } from "https://deno.land/std@0.111.0/bytes/mod.ts";
import { ObjectStoreMeta } from "../nats-base-client/types.ts";

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
  assertEquals(oi.meta.name, "BLOB");
  assert(1000 > (Date.now() - millis(oi.mtime)));

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
  assertEquals(info!.meta.name, "b");

  // add some headers
  meta = {} as ObjectStoreMeta;
  meta.headers = headers();
  meta.headers.set("color", "red");
  await os.update("b", meta);

  info = await os.info("b");
  assertExists(info);
  assertEquals(info!.meta.headers?.get("color"), "red");

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
  assertEquals(oi.meta.name, "empty");

  const or = await os.get("empty");
  assert(or !== null);
  assertEquals(await or.error, null);
  const v = await fromReadableStream(or.data);
  assertEquals(v.length, 0);

  await cleanup(ns, nc);
});

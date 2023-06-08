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

import { validateBucket, validateKey } from "./kv.ts";
import { Base64UrlPaddedCodec } from "../nats-base-client/base64.ts";
import { JSONCodec } from "../nats-base-client/codec.ts";
import { nuid } from "../nats-base-client/nuid.ts";
import { deferred } from "../nats-base-client/util.ts";
import { DataBuffer } from "../nats-base-client/databuffer.ts";
import { headers, MsgHdrsImpl } from "../nats-base-client/headers.ts";
import {
  consumerOpts,
  JetStreamClient,
  JetStreamManager,
  JsHeaders,
  ObjectInfo,
  ObjectResult,
  ObjectStore,
  ObjectStoreMeta,
  ObjectStoreMetaOptions,
  ObjectStoreOptions,
  ObjectStorePutOpts,
  ObjectStoreStatus,
  PubAck,
} from "./types.ts";
import { QueuedIteratorImpl } from "../nats-base-client/queued_iterator.ts";
import { SHA256 } from "../nats-base-client/sha256.js";

import {
  MsgHdrs,
  NatsConnection,
  NatsError,
  QueuedIterator,
} from "../nats-base-client/core.ts";
import {
  DiscardPolicy,
  PurgeResponse,
  StorageType,
  StreamConfig,
  StreamInfo,
  StreamInfoRequestOptions,
} from "./jsapi_types.ts";
import { JsMsg } from "./jsmsg.ts";

export const osPrefix = "OBJ_";
export const digestType = "SHA-256=";

export function objectStoreStreamName(bucket: string): string {
  validateBucket(bucket);
  return `${osPrefix}${bucket}`;
}

export function objectStoreBucketName(stream: string): string {
  if (stream.startsWith(osPrefix)) {
    return stream.substring(4);
  }
  return stream;
}

export class ObjectStoreStatusImpl implements ObjectStoreStatus {
  si: StreamInfo;
  backingStore: string;

  constructor(si: StreamInfo) {
    this.si = si;
    this.backingStore = "JetStream";
  }
  get bucket(): string {
    return objectStoreBucketName(this.si.config.name);
  }
  get description(): string {
    return this.si.config.description ?? "";
  }
  get ttl(): number {
    return this.si.config.max_age;
  }
  get storage(): StorageType {
    return this.si.config.storage;
  }
  get replicas(): number {
    return this.si.config.num_replicas;
  }
  get sealed(): boolean {
    return this.si.config.sealed;
  }
  get size(): number {
    return this.si.state.bytes;
  }
  get streamInfo(): StreamInfo {
    return this.si;
  }
}

type ServerObjectStoreMeta = {
  name: string;
  description?: string;
  headers?: Record<string, string[]>;
  options?: ObjectStoreMetaOptions;
};

type ServerObjectInfo = {
  bucket: string;
  nuid: string;
  size: number;
  chunks: number;
  digest: string;
  deleted?: boolean;
  mtime: string;
  revision: number;
} & ServerObjectStoreMeta;

class ObjectInfoImpl implements ObjectInfo {
  info: ServerObjectInfo;
  hdrs!: MsgHdrs;
  constructor(oi: ServerObjectInfo) {
    this.info = oi;
  }
  get name(): string {
    return this.info.name;
  }
  get description(): string {
    return this.info.description ?? "";
  }
  get headers(): MsgHdrs {
    if (!this.hdrs) {
      this.hdrs = MsgHdrsImpl.fromRecord(this.info.headers || {});
    }
    return this.hdrs;
  }
  get options(): ObjectStoreMetaOptions | undefined {
    return this.info.options;
  }
  get bucket(): string {
    return this.info.bucket;
  }
  get chunks(): number {
    return this.info.chunks;
  }
  get deleted(): boolean {
    return this.info.deleted ?? false;
  }
  get digest(): string {
    return this.info.digest;
  }
  get mtime(): string {
    return this.info.mtime;
  }
  get nuid(): string {
    return this.info.nuid;
  }
  get size(): number {
    return this.info.size;
  }
  get revision(): number {
    return this.info.revision;
  }
}

function toServerObjectStoreMeta(
  meta: Partial<ObjectStoreMeta>,
): ServerObjectStoreMeta {
  const v = {
    name: meta.name,
    description: meta.description ?? "",
    options: meta.options,
  } as ServerObjectStoreMeta;

  if (meta.headers) {
    const mhi = meta.headers as MsgHdrsImpl;
    v.headers = mhi.toRecord();
  }
  return v;
}

function meta(oi: ObjectInfo): ObjectStoreMeta {
  return {
    name: oi.name,
    description: oi.description,
    headers: oi.headers,
    options: oi.options,
  };
}

function emptyReadableStream(): ReadableStream {
  return new ReadableStream({
    pull(c) {
      c.enqueue(new Uint8Array(0));
      c.close();
    },
  });
}

export interface ObjectStoreKeys {
  keyName(key: string): { name: string; error?: Error };
  metaSubject(key: string): string;
  chunkSubject(id: string, key: string): string;
  metaSubjectAll(): string;
  streamSubjectNames(): string[];
  version(): number;
}

export class ObjectStoreKeysV1 implements ObjectStoreKeys {
  name: string;
  constructor(name: string) {
    this.name = name;
  }

  version() {
    return 1;
  }

  keyName(name: string): { name: string; error?: Error } {
    if (!name || name.length === 0) {
      return { name, error: new Error("name cannot be empty") };
    }
    // cannot use replaceAll - node until node 16 is min
    // name = name.replaceAll(".", "_");
    // name = name.replaceAll(" ", "_");
    name = name.replace(/[. ]/g, "_");

    let error = undefined;
    try {
      validateKey(name);
    } catch (err) {
      error = err;
    }
    return { name, error };
  }

  chunkSubject(id: string, _key: string): string {
    return `$O.${this.name}.C.${id}`;
  }

  metaSubject(key: string): string {
    return `$O.${this.name}.M.${Base64UrlPaddedCodec.encode(key)}`;
  }

  metaSubjectAll(): string {
    return `$O.${this.name}.M.>`;
  }

  streamSubjectNames(): string[] {
    return [`$O.${this.name}.C.>`, `$O.${this.name}.M.>`];
  }
}

export class ObjectStoreKeysV2 implements ObjectStoreKeys {
  name: string;
  constructor(name: string) {
    this.name = name;
  }

  version() {
    return 2;
  }

  keyName(name: string): { name: string; error?: Error } {
    let error = undefined;
    try {
      validateKey(name);
    } catch (err) {
      error = err;
    }
    return { name, error };
  }

  chunkSubject(id: string, key: string): string {
    return `$O2.${this.name}.C.${id}.${key}`;
  }

  metaSubject(key: string): string {
    return `$O2.${this.name}.M.${key}`;
  }

  metaSubjectAll(): string {
    return `$O2.${this.name}.M.>`;
  }

  streamSubjectNames(): string[] {
    return [`$O2.${this.name}.C.>`, `$O2.${this.name}.M.>`];
  }
}

export class ObjectStoreImpl implements ObjectStore {
  jsm: JetStreamManager;
  js: JetStreamClient;
  stream!: string;
  name: string;
  keys: ObjectStoreKeys;

  constructor(name: string, jsm: JetStreamManager, js: JetStreamClient) {
    this.name = name;
    this.jsm = jsm;
    this.js = js;
    this.keys = new ObjectStoreKeysV2(this.name);
  }

  version(): number {
    return this.keys.version();
  }

  async info(name: string): Promise<ObjectInfo | null> {
    const info = await this.rawInfo(name);
    return info ? new ObjectInfoImpl(info) : null;
  }

  async list(): Promise<ObjectInfo[]> {
    const buf: ObjectInfo[] = [];
    const iter = await this.watch({
      ignoreDeletes: true,
      includeHistory: true,
    });
    for await (const info of iter) {
      // watch will give a null when it has initialized
      // for us that is the hint we are done
      if (info === null) {
        break;
      }
      buf.push(info);
    }
    return Promise.resolve(buf);
  }

  async rawInfo(name: string): Promise<ServerObjectInfo | null> {
    const { name: obj, error } = this.keys.keyName(name);
    if (error) {
      return Promise.reject(error);
    }

    const meta = this.keys.metaSubject(obj);
    try {
      const m = await this.jsm.streams.getMessage(this.stream, {
        last_by_subj: meta,
      });
      const jc = JSONCodec<ServerObjectInfo>();
      const soi = jc.decode(m.data) as ServerObjectInfo;
      soi.revision = m.seq;
      return soi;
    } catch (err) {
      if (err.code === "404") {
        return null;
      }
      return Promise.reject(err);
    }
  }

  async _si(
    opts?: Partial<StreamInfoRequestOptions>,
  ): Promise<StreamInfo | null> {
    try {
      return await this.jsm.streams.info(this.stream, opts);
    } catch (err) {
      const nerr = err as NatsError;
      if (nerr.code === "404") {
        return null;
      }
      return Promise.reject(err);
    }
  }

  async seal(): Promise<ObjectStoreStatus> {
    let info = await this._si();
    if (info === null) {
      return Promise.reject(new Error("object store not found"));
    }
    info.config.sealed = true;
    info = await this.jsm.streams.update(this.stream, info.config);
    return Promise.resolve(new ObjectStoreStatusImpl(info));
  }

  async status(
    opts?: Partial<StreamInfoRequestOptions>,
  ): Promise<ObjectStoreStatus> {
    const info = await this._si(opts);
    if (info === null) {
      return Promise.reject(new Error("object store not found"));
    }
    return Promise.resolve(new ObjectStoreStatusImpl(info));
  }

  destroy(): Promise<boolean> {
    return this.jsm.streams.delete(this.stream);
  }

  async _put(
    meta: ObjectStoreMeta,
    rs: ReadableStream<Uint8Array> | null,
    opts?: ObjectStorePutOpts,
  ): Promise<ObjectInfo> {
    const jsopts = this.js.getOptions();
    opts = opts || { timeout: jsopts.timeout };
    opts.timeout = opts.timeout || jsopts.timeout;
    opts.previousRevision = opts.previousRevision ?? undefined;
    const { timeout, previousRevision } = opts;
    const si = (this.js as unknown as { nc: NatsConnection }).nc.info;
    const maxPayload = si?.max_payload || 1024;
    meta = meta || {} as ObjectStoreMeta;
    meta.options = meta.options || {};
    let maxChunk = meta.options?.max_chunk_size || 128 * 1024;
    maxChunk = maxChunk > maxPayload ? maxPayload : maxChunk;
    meta.options.max_chunk_size = maxChunk;

    const old = await this.info(meta.name);
    const { name: n, error } = this.keys.keyName(meta.name);
    if (error) {
      return Promise.reject(error);
    }

    const id = nuid.next();
    const chunkSubj = this.keys.chunkSubject(id, n);
    const metaSubj = this.keys.metaSubject(n);

    const info = Object.assign({
      bucket: this.name,
      nuid: id,
      size: 0,
      chunks: 0,
    }, toServerObjectStoreMeta(meta)) as ServerObjectInfo;

    const d = deferred<ObjectInfo>();

    const proms: Promise<unknown>[] = [];
    const db = new DataBuffer();
    try {
      const reader = rs ? rs.getReader() : null;
      const sha = new SHA256();

      while (true) {
        const { done, value } = reader
          ? await reader.read()
          : { done: true, value: undefined };
        if (done) {
          // put any partial chunk in
          if (db.size() > 0) {
            const payload = db.drain();
            sha.update(payload);
            info.chunks!++;
            info.size! += payload.length;
            proms.push(this.js.publish(chunkSubj, payload, { timeout }));
          }
          // wait for all the chunks to write
          await Promise.all(proms);
          proms.length = 0;

          // prepare the metadata
          info.mtime = new Date().toISOString();
          const digest = sha.digest("base64");
          const pad = digest.length % 3;
          const padding = pad > 0 ? "=".repeat(pad) : "";
          info.digest = `${digestType}${digest}${padding}`;
          info.deleted = false;

          // trailing md for the object
          const h = headers();
          if (typeof previousRevision === "number") {
            h.set("Nats-Expected-Last-Subject-Sequence", `${previousRevision}`);
          }
          h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueSubject);

          // try to update the metadata
          const pa = await this.js.publish(metaSubj, JSONCodec().encode(info), {
            headers: h,
            timeout,
          });
          // update the revision to point to the sequence where we inserted
          info.revision = pa.seq;

          // if we are here, the new entry is live
          if (old) {
            try {
              await this.jsm.streams.purge(this.stream, {
                filter: this.keys.chunkSubject(old.nuid, meta.name),
              });
            } catch (_err) {
              // rejecting here, would mean send the wrong signal
              // the update succeeded, but cleanup of old chunks failed.
            }
          }

          // resolve the ObjectInfo
          d.resolve(new ObjectInfoImpl(info!));
          // stop
          break;
        }
        if (value) {
          db.fill(value);
          while (db.size() > maxChunk) {
            info.chunks!++;
            info.size! += maxChunk;
            const payload = db.drain(meta.options.max_chunk_size);
            sha.update(payload);
            proms.push(
              this.js.publish(chunkSubj, payload, { timeout }),
            );
          }
        }
      }
    } catch (err) {
      // we failed, remove any partials
      await this.jsm.streams.purge(this.stream, { filter: chunkSubj });
      d.reject(err);
    }

    return d;
  }

  putBlob(
    meta: ObjectStoreMeta,
    data: Uint8Array | null,
    opts?: ObjectStorePutOpts,
  ): Promise<ObjectInfo> {
    function readableStreamFrom(data: Uint8Array): ReadableStream<Uint8Array> {
      return new ReadableStream<Uint8Array>({
        pull(controller) {
          controller.enqueue(data);
          controller.close();
        },
      });
    }
    if (data === null) {
      data = new Uint8Array(0);
    }
    return this.put(meta, readableStreamFrom(data), opts);
  }

  put(
    meta: ObjectStoreMeta,
    rs: ReadableStream<Uint8Array> | null,
    opts?: ObjectStorePutOpts,
  ): Promise<ObjectInfo> {
    if (meta?.options?.link) {
      return Promise.reject(
        new Error("link cannot be set when putting the object in bucket"),
      );
    }
    return this._put(meta, rs, opts);
  }

  async getBlob(name: string): Promise<Uint8Array | null> {
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

    const r = await this.get(name);
    if (r === null) {
      return Promise.resolve(null);
    }

    const vs = await Promise.all([r.error, fromReadableStream(r.data)]);
    if (vs[0]) {
      return Promise.reject(vs[0]);
    } else {
      return Promise.resolve(vs[1]);
    }
  }

  async get(name: string): Promise<ObjectResult | null> {
    const info = await this.rawInfo(name);
    if (info === null) {
      return Promise.resolve(null);
    }

    if (info.deleted) {
      return Promise.resolve(null);
    }

    if (info.options && info.options.link) {
      const ln = info.options.link.name || "";
      if (ln === "") {
        throw new Error("link is a bucket");
      }
      const os = await ObjectStoreImpl.create(
        this.js,
        info.options.link.bucket,
      );
      return os.get(ln);
    }

    const d = deferred<Error | null>();

    const r: Partial<ObjectResult> = {
      info: new ObjectInfoImpl(info),
      error: d,
    };
    if (info.size === 0) {
      r.data = emptyReadableStream();
      d.resolve(null);
      return Promise.resolve(r as ObjectResult);
    }

    let controller: ReadableStreamDefaultController;

    const oc = consumerOpts();
    oc.orderedConsumer();
    const sha = new SHA256();
    const subj = this.keys.chunkSubject(info.nuid, name);
    const sub = await this.js.subscribe(subj, oc);
    (async () => {
      for await (const jm of sub) {
        if (jm.data.length > 0) {
          sha.update(jm.data);
          controller!.enqueue(jm.data);
        }
        if (jm.info.pending === 0) {
          const hash = sha.digest("base64");
          // go pads the hash - which should be multiple of 3 - otherwise pads with '='
          const pad = hash.length % 3;
          const padding = pad > 0 ? "=".repeat(pad) : "";
          const digest = `${digestType}${hash}${padding}`;
          if (digest !== info.digest) {
            controller!.error(
              new Error(
                `received a corrupt object, digests do not match received: ${info.digest} calculated ${digest}`,
              ),
            );
          } else {
            controller!.close();
          }
          sub.unsubscribe();
        }
      }
    })()
      .then(() => {
        d.resolve();
      })
      .catch((err) => {
        controller!.error(err);
        d.reject(err);
      });

    r.data = new ReadableStream({
      start(c) {
        controller = c;
      },
      cancel() {
        sub.unsubscribe();
      },
    });

    return r as ObjectResult;
  }

  linkStore(name: string, bucket: ObjectStore): Promise<ObjectInfo> {
    if (!(bucket instanceof ObjectStoreImpl)) {
      return Promise.reject("bucket required");
    }
    const osi = bucket as ObjectStoreImpl;
    const { name: n, error } = this.keys.keyName(name);
    if (error) {
      return Promise.reject(error);
    }

    const meta = {
      name: n,
      options: { link: { bucket: osi.name } },
    };
    return this._put(meta, null);
  }

  async link(name: string, info: ObjectInfo): Promise<ObjectInfo> {
    if (info.deleted) {
      return Promise.reject(new Error("object is deleted"));
    }
    const { name: n, error } = this.keys.keyName(name);
    if (error) {
      return Promise.reject(error);
    }

    // same object store
    if (this.name === info.bucket) {
      const copy = Object.assign({}, meta(info)) as ObjectStoreMeta;
      copy.name = n;
      try {
        await this.update(info.name, copy);
        const ii = await this.info(n);
        return ii!;
      } catch (err) {
        return Promise.reject(err);
      }
    }
    const link = { bucket: info.bucket, name: info.name };
    const mm = {
      name: n,
      options: { link: link },
    } as ObjectStoreMeta;
    return this._put(mm, null);
  }

  async delete(name: string): Promise<PurgeResponse> {
    const info = await this.rawInfo(name);
    if (info === null) {
      return Promise.resolve({ purged: 0, success: false });
    }
    info.deleted = true;
    info.size = 0;
    info.chunks = 0;
    info.digest = "";

    const jc = JSONCodec();
    const h = headers();
    h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueSubject);

    await this.js.publish(this.keys.metaSubject(info.name), jc.encode(info), {
      headers: h,
    });
    return this.jsm.streams.purge(this.stream, {
      filter: this.keys.chunkSubject(info.nuid, name),
    });
  }

  async update(
    name: string,
    meta: Partial<ObjectStoreMeta> = {},
  ): Promise<PubAck> {
    const info = await this.rawInfo(name);
    if (info === null) {
      return Promise.reject(new Error("object not found"));
    }
    if (info.deleted) {
      return Promise.reject(
        new Error("cannot update meta for a deleted object"),
      );
    }
    // FIXME: Go's implementation doesn't seem correct - it possibly adds a new meta entry
    //  effectively making the object available under 2 names, but it doesn't remove the
    //  older one.
    meta.name = meta.name ?? info.name;
    const { name: n, error } = this.keys.keyName(meta.name);
    if (error) {
      return Promise.reject(error);
    }
    if (name !== meta.name) {
      const i = await this.info(meta.name);
      if (i && !i.deleted) {
        return Promise.reject(
          new Error("an object already exists with that name"),
        );
      }
    }
    meta.name = n;

    const ii = Object.assign({}, info, toServerObjectStoreMeta(meta!));
    const jc = JSONCodec();

    return this.js.publish(this.keys.metaSubject(ii.name), jc.encode(ii));
  }

  async watch(opts: Partial<
    {
      ignoreDeletes?: boolean;
      includeHistory?: boolean;
    }
  > = {}): Promise<QueuedIterator<ObjectInfo | null>> {
    opts.includeHistory = opts.includeHistory ?? false;
    opts.ignoreDeletes = opts.ignoreDeletes ?? false;
    let initialized = false;
    const qi = new QueuedIteratorImpl<ObjectInfo | null>();
    const subj = this.keys.metaSubjectAll();
    try {
      await this.jsm.streams.getMessage(this.stream, { last_by_subj: subj });
    } catch (err) {
      if (err.code === "404") {
        qi.push(null);
        initialized = true;
      } else {
        qi.stop(err);
      }
    }
    const jc = JSONCodec<ObjectInfo>();
    const copts = consumerOpts();
    copts.orderedConsumer();
    if (opts.includeHistory) {
      copts.deliverLastPerSubject();
    } else {
      // FIXME: Go's implementation doesn't seem correct - if history is not desired
      //  the watch should only be giving notifications on new entries
      initialized = true;
      copts.deliverNew();
    }
    copts.callback((err: NatsError | null, jm: JsMsg | null) => {
      if (err) {
        qi.stop(err);
        return;
      }
      if (jm !== null) {
        const oi = jc.decode(jm.data);
        if (oi.deleted && opts.ignoreDeletes === true) {
          // do nothing
        } else {
          qi.push(oi);
        }
        if (jm.info?.pending === 0 && !initialized) {
          initialized = true;
          qi.push(null);
        }
      }
    });

    const sub = await this.js.subscribe(subj, copts);
    qi._data = sub;
    qi.iterClosed.then(() => {
      sub.unsubscribe();
    });
    sub.closed.then(() => {
      qi.stop();
    }).catch((err) => {
      qi.stop(err);
    });

    return qi;
  }

  async init(opts: Partial<ObjectStoreOptions> = {}): Promise<void> {
    try {
      this.stream = objectStoreStreamName(this.name);
    } catch (err) {
      return Promise.reject(err);
    }
    try {
      const si = await this.jsm.streams.info(this.name);
      const { subjects } = si.config;

      // if we are here, we could we have different versions
      const adapters = [
        new ObjectStoreKeysV1(this.name),
        new ObjectStoreKeysV2(this.name),
      ];
      const keys = adapters.find((k) => {
        const a = k.streamSubjectNames();
        return a.includes(subjects[0]) && a.includes(subjects[1]);
      });
      if (!keys) {
        return Promise.reject("unknown object store configuration");
      }
      this.keys = keys;
    } catch (err) {
      if (err.message === "stream not found") {
        const sc = Object.assign({}, opts) as StreamConfig;
        sc.name = this.stream;
        sc.allow_rollup_hdrs = true;
        sc.discard = DiscardPolicy.New;
        sc.subjects = this.keys.streamSubjectNames();
        if (opts.placement) {
          sc.placement = opts.placement;
        }
        // FIXME: metadata would be good, but really the
        //  subject for the keys should be different in what
        //  the stream takes
        // sc.metadata = { NatsObjectStoreVersion: "2" };
        await this.jsm.streams.add(sc);
      }
    }
  }

  static async create(
    js: JetStreamClient,
    name: string,
    opts: Partial<ObjectStoreOptions> = {},
  ): Promise<ObjectStore> {
    const jsm = await js.jetstreamManager();
    const os = new ObjectStoreImpl(name, jsm, js);
    await os.init(opts);
    return Promise.resolve(os);
  }
}

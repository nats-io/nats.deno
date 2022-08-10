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

import {
  DiscardPolicy,
  JetStreamClient,
  JetStreamManager,
  JetStreamOptions,
  JsHeaders,
  JsMsg,
  ObjectInfo,
  ObjectResult,
  ObjectStore,
  ObjectStoreInfo,
  ObjectStoreMeta,
  ObjectStoreMetaOptions,
  ObjectStoreOptions,
  PubAck,
  PurgeResponse,
  StorageType,
  StreamConfig,
  StreamInfo,
  StreamInfoRequestOptions,
} from "./types.ts";
import { validateBucket, validateKey } from "./kv.ts";
import { JSONCodec } from "./codec.ts";
import { nuid } from "./nuid.ts";
import { deferred } from "./util.ts";
import { JetStreamClientImpl } from "./jsclient.ts";
import { DataBuffer } from "./databuffer.ts";
import { headers, MsgHdrs, MsgHdrsImpl } from "./headers.ts";
import { consumerOpts } from "./jsconsumeropts.ts";
import { NatsError } from "./mod.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { SHA256 } from "./sha256.js";

export function objectStoreStreamName(bucket: string): string {
  validateBucket(bucket);
  return `OBJ_${bucket}`;
}

export function objectStoreBucketName(stream: string): string {
  if (stream.startsWith("OBJ_")) {
    return stream.substring(4);
  }
  return stream;
}

export class ObjectStoreInfoImpl implements ObjectStoreInfo {
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

export class ObjectStoreImpl implements ObjectStore {
  jsm: JetStreamManager;
  js: JetStreamClient;
  stream!: string;
  name: string;

  constructor(name: string, jsm: JetStreamManager, js: JetStreamClient) {
    this.name = name;
    this.jsm = jsm;
    this.js = js;
  }

  _sanitizeName(name: string): { name: string; error?: Error } {
    if (!name || name.length === 0) {
      return { name, error: new Error("name cannot be empty") };
    }
    // cannot use replaceAll - node until node 16 is min
    // name = name.replaceAll(".", "_");
    // name = name.replaceAll(" ", "_");
    name = name.replace(/[\. ]/g, "_");

    let error = undefined;
    try {
      validateKey(name);
    } catch (err) {
      error = err;
    }
    return { name, error };
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
    const { name: obj, error } = this._sanitizeName(name);
    if (error) {
      return Promise.reject(error);
    }

    const meta = `$O.${this.name}.M.${obj}`;
    try {
      const m = await this.jsm.streams.getMessage(this.stream, {
        last_by_subj: meta,
      });
      const jc = JSONCodec<ServerObjectInfo>();
      const info = jc.decode(m.data) as ServerObjectInfo;
      return info;
    } catch (err) {
      if (err.code === "404") {
        return null;
      }
      return Promise.reject(err);
    }
  }

  async seal(): Promise<ObjectStoreInfo> {
    let info = await this.jsm.streams.info(this.stream);
    if (info === null) {
      return Promise.reject(new Error("object store not found"));
    }
    info.config.sealed = true;
    info = await this.jsm.streams.update(this.stream, info.config);
    return Promise.resolve(new ObjectStoreInfoImpl(info));
  }

  async status(
    opts?: Partial<StreamInfoRequestOptions>,
  ): Promise<ObjectStoreInfo> {
    const info = await this.jsm.streams.info(this.stream, opts);
    if (info === null) {
      return Promise.reject(new Error("object store not found"));
    }
    return Promise.resolve(new ObjectStoreInfoImpl(info));
  }

  destroy(): Promise<boolean> {
    return this.jsm.streams.delete(this.stream);
  }

  async put(
    meta: ObjectStoreMeta,
    rs: ReadableStream<Uint8Array> | null,
  ): Promise<ObjectInfo> {
    const jsi = this.js as JetStreamClientImpl;
    const maxPayload = jsi.nc.info?.max_payload || 1024;
    meta = meta || {} as ObjectStoreMeta;
    meta.options = meta.options || {};
    let maxChunk = meta.options?.max_chunk_size || 128 * 1024;
    maxChunk = maxChunk > maxPayload ? maxPayload : maxChunk;
    meta.options.max_chunk_size = maxChunk;

    const old = await this.info(meta.name);
    const { name: n, error } = this._sanitizeName(meta.name);
    if (error) {
      return Promise.reject(error);
    }

    const id = nuid.next();
    const chunkSubj = this._chunkSubject(id);
    const metaSubj = this._metaSubject(n);

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
            info.mtime = new Date().toISOString();
            const digest = sha.digest("base64");
            const pad = digest.length % 3;
            const padding = pad > 0 ? "=".repeat(pad) : "";
            info.digest = `sha-256=${digest}${padding}`;
            info.deleted = false;
            proms.push(this.js.publish(chunkSubj, payload));
          }
          // trailing md for the object
          const h = headers();
          h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueSubject);
          proms.push(
            this.js.publish(metaSubj, JSONCodec().encode(info), { headers: h }),
          );

          // if we had this object trim it out
          if (old) {
            proms.push(
              this.jsm.streams.purge(this.stream, {
                filter: `$O.${this.name}.C.${old.nuid}`,
              }),
            );
          }
          // wait for all the sends to complete
          await Promise.all(proms);
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
              this.js.publish(chunkSubj, payload),
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

  async get(name: string): Promise<ObjectResult | null> {
    const info = await this.rawInfo(name);
    if (info === null) {
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
    const subj = `$O.${this.name}.C.${info.nuid}`;
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
          const digest = `sha-256=${hash}${padding}`;
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
    const { name: n, error } = this._sanitizeName(name);
    if (error) {
      return Promise.reject(error);
    }

    const meta = {
      name: n,
      options: { link: { bucket: osi.name } },
    };
    return this.put(meta, null);
  }

  async link(name: string, info: ObjectInfo): Promise<ObjectInfo> {
    if (info.deleted) {
      return Promise.reject(new Error("object is deleted"));
    }
    const { name: n, error } = this._sanitizeName(name);
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
    return this.put(mm, null);
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

    await this.js.publish(this._metaSubject(info.name), jc.encode(info), {
      headers: h,
    });
    return this.jsm.streams.purge(this.stream, {
      filter: this._chunkSubject(info.nuid),
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
    // FIXME: Go's implementation doesn't seem correct - it possibly adds a new meta entry
    //  effectively making the object available under 2 names, but it doesn't remove the
    //  older one.
    meta.name = meta.name ?? info.name;
    const { name: n, error } = this._sanitizeName(meta.name);
    if (error) {
      return Promise.reject(error);
    }
    meta.name = n;
    const ii = Object.assign({}, info, toServerObjectStoreMeta(meta!));
    const jc = JSONCodec();

    return this.js.publish(this._metaSubject(ii.name), jc.encode(ii));
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
    const subj = this._metaSubjectAll();
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

  _chunkSubject(id: string) {
    return `$O.${this.name}.C.${id}`;
  }

  _metaSubject(n: string): string {
    return `$O.${this.name}.M.${n}`;
  }

  _metaSubjectAll(): string {
    return `$O.${this.name}.M.>`;
  }

  async init(opts: Partial<ObjectStoreOptions> = {}): Promise<void> {
    try {
      this.stream = objectStoreStreamName(this.name);
    } catch (err) {
      return Promise.reject(err);
    }
    const sc = Object.assign({}, opts) as StreamConfig;
    sc.name = this.stream;
    sc.allow_rollup_hdrs = true;
    sc.discard = DiscardPolicy.New;
    sc.subjects = [`$O.${this.name}.C.>`, `$O.${this.name}.M.>`];
    if (opts.placement) {
      sc.placement = opts.placement;
    }

    try {
      await this.jsm.streams.info(sc.name);
    } catch (err) {
      if (err.message === "stream not found") {
        await this.jsm.streams.add(sc);
      }
    }
  }

  static async create(
    js: JetStreamClient,
    name: string,
    opts: Partial<ObjectStoreOptions> = {},
  ): Promise<ObjectStore> {
    // we may not have support in the environment
    if (typeof crypto?.subtle?.digest !== "function") {
      return Promise.reject(
        new Error(
          "objectstore: unable to calculate hashes - crypto.subtle.digest with sha256 support is required",
        ),
      );
    }

    const jsi = js as JetStreamClientImpl;
    let jsopts = jsi.opts || {} as JetStreamOptions;
    const to = jsopts.timeout || 2000;
    jsopts = Object.assign(jsopts, { timeout: to });
    const jsm = await jsi.nc.jetstreamManager(jsopts);
    const os = new ObjectStoreImpl(name, jsm, js);
    await os.init(opts);
    return Promise.resolve(os);
  }
}

class Base64Codec {
  static encode(bytes: string | Uint8Array): string {
    if (typeof bytes === "string") {
      return btoa(bytes);
    }
    const a = Array.from(bytes);
    return btoa(String.fromCharCode(...a));
  }

  static decode(s: string, binary = false): Uint8Array | string {
    const bin = atob(s);
    if (!binary) {
      return bin;
    }
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) {
      bytes[i] = bin.charCodeAt(i);
    }
    return bytes;
  }
}

class Base64UrlCodec {
  static encode(bytes: string | Uint8Array): string {
    return Base64UrlCodec.toB64URLEncoding(Base64Codec.encode(bytes));
  }

  static decode(s: string, binary = false): Uint8Array | string {
    return Base64Codec.decode(Base64UrlCodec.fromB64URLEncoding(s), binary);
  }

  static toB64URLEncoding(b64str: string): string {
    b64str = b64str.replace(/=/g, "");
    b64str = b64str.replace(/\+/g, "-");
    return b64str.replace(/\//g, "_");
  }

  static fromB64URLEncoding(b64str: string): string {
    // pads are % 4, but not necessary on decoding
    b64str = b64str.replace(/_/g, "/");
    b64str = b64str.replace(/-/g, "+");
    return b64str;
  }
}

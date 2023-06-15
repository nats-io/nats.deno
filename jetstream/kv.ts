/*
 * Copyright 2021-2023 The NATS Authors
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
  MsgHdrs,
  NatsConnection,
  NatsError,
  Payload,
} from "../nats-base-client/core.ts";
import { millis, nanos } from "./jsutil.ts";
import { QueuedIteratorImpl } from "../nats-base-client/queued_iterator.ts";
import { headers } from "../nats-base-client/headers.ts";
import {
  consumerOpts,
  DirectStreamAPI,
  JetStreamClient,
  JetStreamManager,
  JetStreamPublishOptions,
  JetStreamSubscriptionInfoable,
  JsHeaders,
  KV,
  KvCodec,
  KvCodecs,
  KvEntry,
  KvOptions,
  kvPrefix,
  KvPutOptions,
  KvRemove,
  KvStatus,
  StoredMsg,
} from "./types.ts";
import { compare, Feature, parseSemVer } from "../nats-base-client/semver.ts";
import { deferred } from "../nats-base-client/util.ts";
import { Empty } from "../nats-base-client/encoders.ts";
import { ErrorCode, QueuedIterator } from "../nats-base-client/core.ts";
import {
  AckPolicy,
  ConsumerConfig,
  ConsumerInfo,
  DeliverPolicy,
  DiscardPolicy,
  MsgRequest,
  Placement,
  PurgeOpts,
  PurgeResponse,
  Republish,
  RetentionPolicy,
  StorageType,
  StreamConfig,
  StreamInfo,
  StreamSource,
} from "./jsapi_types.ts";
import { JsMsg } from "./jsmsg.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";

export function Base64KeyCodec(): KvCodec<string> {
  return {
    encode(key: string): string {
      return btoa(key);
    },
    decode(bkey: string): string {
      return atob(bkey);
    },
  };
}

export function NoopKvCodecs(): KvCodecs {
  return {
    key: {
      encode(k: string): string {
        return k;
      },
      decode(k: string): string {
        return k;
      },
    },
    value: {
      encode(v: Uint8Array): Uint8Array {
        return v;
      },
      decode(v: Uint8Array): Uint8Array {
        return v;
      },
    },
  };
}

export function defaultBucketOpts(): Partial<KvOptions> {
  return {
    replicas: 1,
    history: 1,
    timeout: 2000,
    maxBucketSize: -1,
    maxValueSize: -1,
    codec: NoopKvCodecs(),
    storage: StorageType.File,
  };
}

type OperationType = "PUT" | "DEL" | "PURGE";

export const kvOperationHdr = "KV-Operation";
const kvSubjectPrefix = "$KV";

const validKeyRe = /^[-/=.\w]+$/;
const validSearchKey = /^[-/=.>*\w]+$/;
const validBucketRe = /^[-\w]+$/;

// this exported for tests
export function validateKey(k: string) {
  if (k.startsWith(".") || k.endsWith(".") || !validKeyRe.test(k)) {
    throw new Error(`invalid key: ${k}`);
  }
}

export function validateSearchKey(k: string) {
  if (k.startsWith(".") || k.endsWith(".") || !validSearchKey.test(k)) {
    throw new Error(`invalid key: ${k}`);
  }
}

export function hasWildcards(k: string) {
  if (k.startsWith(".") || k.endsWith(".")) {
    throw new Error(`invalid key: ${k}`);
  }
  const chunks = k.split(".");

  let hasWildcards = false;
  for (let i = 0; i < chunks.length; i++) {
    switch (chunks[i]) {
      case "*":
        hasWildcards = true;
        break;
      case ">":
        if (i !== chunks.length - 1) {
          throw new Error(`invalid key: ${k}`);
        }
        hasWildcards = true;
        break;
      default:
        // continue
    }
  }
  return hasWildcards;
}

// this exported for tests
export function validateBucket(name: string) {
  if (!validBucketRe.test(name)) {
    throw new Error(`invalid bucket name: ${name}`);
  }
}

export class Bucket implements KV, KvRemove {
  js: JetStreamClient;
  jsm: JetStreamManager;
  stream!: string;
  bucket: string;
  direct!: boolean;
  codec!: KvCodecs;
  prefix: string;
  editPrefix: string;
  useJsPrefix: boolean;
  _prefixLen: number;

  constructor(bucket: string, js: JetStreamClient, jsm: JetStreamManager) {
    validateBucket(bucket);
    this.js = js;
    this.jsm = jsm;
    this.bucket = bucket;
    this.prefix = kvSubjectPrefix;
    this.editPrefix = "";
    this.useJsPrefix = false;
    this._prefixLen = 0;
  }

  static async create(
    js: JetStreamClient,
    name: string,
    opts: Partial<KvOptions> = {},
  ): Promise<KV> {
    validateBucket(name);
    const jsm = await js.jetstreamManager();
    const bucket = new Bucket(name, js, jsm);
    await bucket.init(opts);
    return bucket;
  }

  static async bind(
    js: JetStreamClient,
    name: string,
    opts: Partial<{ codec: KvCodecs }> = {},
  ): Promise<KV> {
    const jsm = await js.jetstreamManager();
    const info = await jsm.streams.info(`${kvPrefix}${name}`);
    validateBucket(info.config.name);
    const bucket = new Bucket(name, js, jsm);
    Object.assign(bucket, info);
    bucket.codec = opts.codec || NoopKvCodecs();
    bucket.direct = info.config.allow_direct ?? false;
    bucket.initializePrefixes(info);

    return bucket;
  }

  async init(opts: Partial<KvOptions> = {}): Promise<void> {
    const bo = Object.assign(defaultBucketOpts(), opts) as KvOptions;
    this.codec = bo.codec;
    const sc = {} as StreamConfig;
    this.stream = sc.name = opts.streamName ?? this.bucketName();
    sc.retention = RetentionPolicy.Limits;
    sc.max_msgs_per_subject = bo.history;
    if (bo.maxBucketSize) {
      bo.max_bytes = bo.maxBucketSize;
    }
    if (bo.max_bytes) {
      sc.max_bytes = bo.max_bytes;
    }
    sc.max_msg_size = bo.maxValueSize;
    sc.storage = bo.storage;
    const location = opts.placementCluster ?? "";
    if (location) {
      opts.placement = {} as Placement;
      opts.placement.cluster = location;
      opts.placement.tags = [];
    }
    if (opts.placement) {
      sc.placement = opts.placement;
    }
    if (opts.republish) {
      sc.republish = opts.republish;
    }
    if (opts.description) {
      sc.description = opts.description;
    }
    if (opts.mirror) {
      const mirror = Object.assign({}, opts.mirror);
      if (!mirror.name.startsWith(kvPrefix)) {
        mirror.name = `${kvPrefix}${mirror.name}`;
      }
      sc.mirror = mirror;
      sc.mirror_direct = true;
    } else if (opts.sources) {
      const sources = opts.sources.map((s) => {
        const c = Object.assign({}, s) as StreamSource;
        if (!c.name.startsWith(kvPrefix)) {
          c.name = `${kvPrefix}${c.name}`;
        }
      });
      sc.sources = sources as unknown[] as StreamSource[];
    } else {
      sc.subjects = [this.subjectForBucket()];
    }

    const nci = (this.js as unknown as { nc: NatsConnectionImpl }).nc;
    const have = nci.getServerVersion();
    const discardNew = have ? compare(have, parseSemVer("2.7.2")) >= 0 : false;
    sc.discard = discardNew ? DiscardPolicy.New : DiscardPolicy.Old;

    const { ok: direct, min } = nci.features.get(
      Feature.JS_ALLOW_DIRECT,
    );
    if (!direct && opts.allow_direct === true) {
      const v = have
        ? `${have!.major}.${have!.minor}.${have!.micro}`
        : "unknown";
      return Promise.reject(
        new Error(
          `allow_direct is not available on server version ${v} - requires ${min}`,
        ),
      );
    }
    // if we are given allow_direct we use it, otherwise what
    // the server supports - in creation this will always rule,
    // but allows the client to opt-in even if it is already
    // available on the stream
    opts.allow_direct = typeof opts.allow_direct === "boolean"
      ? opts.allow_direct
      : direct;
    sc.allow_direct = opts.allow_direct;
    this.direct = sc.allow_direct;

    sc.num_replicas = bo.replicas;
    if (bo.ttl) {
      sc.max_age = nanos(bo.ttl);
    }
    sc.allow_rollup_hdrs = true;

    let info: StreamInfo;
    try {
      info = await this.jsm.streams.info(sc.name);
      if (!info.config.allow_direct && this.direct === true) {
        this.direct = false;
      }
    } catch (err) {
      if (err.message === "stream not found") {
        info = await this.jsm.streams.add(sc);
      } else {
        throw err;
      }
    }
    this.initializePrefixes(info);
  }

  initializePrefixes(info: StreamInfo) {
    this._prefixLen = 0;
    this.prefix = `$KV.${this.bucket}`;
    this.useJsPrefix = this.js.apiPrefix !== "$JS.API";
    const { mirror } = info.config;
    if (mirror) {
      let n = mirror.name;
      if (n.startsWith(kvPrefix)) {
        n = n.substring(kvPrefix.length);
      }
      if (mirror.external && mirror.external.api !== "") {
        const mb = mirror.name.substring(kvPrefix.length);
        this.useJsPrefix = false;
        this.prefix = `$KV.${mb}`;
        this.editPrefix = `${mirror.external.api}.$KV.${n}`;
      } else {
        this.editPrefix = this.prefix;
      }
    }
  }

  bucketName(): string {
    return this.stream ?? `${kvPrefix}${this.bucket}`;
  }

  subjectForBucket(): string {
    return `${this.prefix}.${this.bucket}.>`;
  }

  subjectForKey(k: string, edit = false): string {
    const builder: string[] = [];
    if (edit) {
      if (this.useJsPrefix) {
        builder.push(this.js.apiPrefix);
      }
      if (this.editPrefix !== "") {
        builder.push(this.editPrefix);
      } else {
        builder.push(this.prefix);
      }
    } else {
      if (this.prefix) {
        builder.push(this.prefix);
      }
    }
    builder.push(k);
    return builder.join(".");
  }

  fullKeyName(k: string): string {
    if (this.prefix !== "") {
      return `${this.prefix}.${k}`;
    }
    return `${kvSubjectPrefix}.${this.bucket}.${k}`;
  }

  get prefixLen(): number {
    if (this._prefixLen === 0) {
      this._prefixLen = this.prefix.length + 1;
    }
    return this._prefixLen;
  }

  encodeKey(key: string): string {
    const chunks: string[] = [];
    for (const t of key.split(".")) {
      switch (t) {
        case ">":
        case "*":
          chunks.push(t);
          break;
        default:
          chunks.push(this.codec.key.encode(t));
          break;
      }
    }
    return chunks.join(".");
  }

  decodeKey(ekey: string): string {
    const chunks: string[] = [];
    for (const t of ekey.split(".")) {
      switch (t) {
        case ">":
        case "*":
          chunks.push(t);
          break;
        default:
          chunks.push(this.codec.key.decode(t));
          break;
      }
    }
    return chunks.join(".");
  }

  validateKey = validateKey;

  validateSearchKey = validateSearchKey;

  hasWildcards = hasWildcards;

  close(): Promise<void> {
    return Promise.resolve();
  }

  dataLen(data: Uint8Array, h?: MsgHdrs): number {
    const slen = h ? h.get(JsHeaders.MessageSizeHdr) || "" : "";
    if (slen !== "") {
      return parseInt(slen, 10);
    }
    return data.length;
  }

  smToEntry(sm: StoredMsg): KvEntry {
    return new KvStoredEntryImpl(this.bucket, this.prefixLen, sm);
  }

  jmToEntry(jm: JsMsg): KvEntry {
    const key = this.decodeKey(jm.subject.substring(this.prefixLen));
    return new KvJsMsgEntryImpl(this.bucket, key, jm);
  }

  async create(k: string, data: Payload): Promise<number> {
    let firstErr;
    try {
      const n = await this.put(k, data, { previousSeq: 0 });
      return Promise.resolve(n);
    } catch (err) {
      firstErr = err;
      if (err?.api_error?.err_code !== 10071) {
        return Promise.reject(err);
      }
    }
    let rev = 0;
    try {
      const e = await this.get(k);
      if (e?.operation === "DEL" || e?.operation === "PURGE") {
        rev = e !== null ? e.revision : 0;
        return this.update(k, data, rev);
      } else {
        return Promise.reject(firstErr);
      }
    } catch (err) {
      return Promise.reject(err);
    }
  }

  update(k: string, data: Payload, version: number): Promise<number> {
    if (version <= 0) {
      throw new Error("version must be greater than 0");
    }
    return this.put(k, data, { previousSeq: version });
  }

  async put(
    k: string,
    data: Payload,
    opts: Partial<KvPutOptions> = {},
  ): Promise<number> {
    const ek = this.encodeKey(k);
    this.validateKey(ek);

    const o = {} as JetStreamPublishOptions;
    if (opts.previousSeq !== undefined) {
      const h = headers();
      o.headers = h;
      h.set("Nats-Expected-Last-Subject-Sequence", `${opts.previousSeq}`);
    }
    try {
      const pa = await this.js.publish(this.subjectForKey(ek, true), data, o);
      return pa.seq;
    } catch (err) {
      const ne = err as NatsError;
      if (ne.isJetStreamError()) {
        ne.message = ne.api_error?.description!;
        ne.code = `${ne.api_error?.code!}`;
        return Promise.reject(ne);
      }
      return Promise.reject(err);
    }
  }

  async get(
    k: string,
    opts?: { revision: number },
  ): Promise<KvEntry | null> {
    const ek = this.encodeKey(k);
    this.validateKey(ek);

    let arg: MsgRequest = { last_by_subj: this.subjectForKey(ek) };
    if (opts && opts.revision > 0) {
      arg = { seq: opts.revision };
    }

    let sm: StoredMsg;
    try {
      if (this.direct) {
        const direct =
          (this.jsm as unknown as { direct: DirectStreamAPI }).direct;
        sm = await direct.getMessage(this.bucketName(), arg);
      } else {
        sm = await this.jsm.streams.getMessage(this.bucketName(), arg);
      }
      const ke = this.smToEntry(sm);
      if (ke.key !== ek) {
        return null;
      }
      return ke;
    } catch (err) {
      if (
        err.code === ErrorCode.JetStream404NoMessages
      ) {
        return null;
      }
      throw err;
    }
  }

  purge(k: string): Promise<void> {
    return this._deleteOrPurge(k, "PURGE");
  }

  delete(k: string): Promise<void> {
    return this._deleteOrPurge(k, "DEL");
  }

  async purgeDeletes(
    olderMillis: number = 30 * 60 * 1000,
  ): Promise<PurgeResponse> {
    const done = deferred();
    const buf: KvEntry[] = [];
    const i = await this.watch({
      key: ">",
      initializedFn: () => {
        done.resolve();
      },
    });
    (async () => {
      for await (const e of i) {
        if (e.operation === "DEL" || e.operation === "PURGE") {
          buf.push(e);
        }
      }
    })().then();
    await done;
    i.stop();
    const min = Date.now() - olderMillis;
    const proms = buf.map((e) => {
      const subj = this.subjectForKey(e.key);
      if (e.created.getTime() >= min) {
        return this.jsm.streams.purge(this.stream, { filter: subj, keep: 1 });
      } else {
        return this.jsm.streams.purge(this.stream, { filter: subj, keep: 0 });
      }
    });
    const purged = await Promise.all(proms);
    purged.unshift({ success: true, purged: 0 });
    return purged.reduce((pv: PurgeResponse, cv: PurgeResponse) => {
      pv.purged += cv.purged;
      return pv;
    });
  }

  async _deleteOrPurge(k: string, op: "DEL" | "PURGE"): Promise<void> {
    if (!this.hasWildcards(k)) {
      return this._doDeleteOrPurge(k, op);
    }
    const iter = await this.keys(k);
    const buf: Promise<void>[] = [];
    for await (const k of iter) {
      buf.push(this._doDeleteOrPurge(k, op));
      if (buf.length === 100) {
        await Promise.all(buf);
        buf.length = 0;
      }
    }
    if (buf.length > 0) {
      await Promise.all(buf);
    }
  }

  async _doDeleteOrPurge(k: string, op: "DEL" | "PURGE"): Promise<void> {
    const ek = this.encodeKey(k);
    this.validateKey(ek);
    const h = headers();
    h.set(kvOperationHdr, op);
    if (op === "PURGE") {
      h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueSubject);
    }
    await this.js.publish(this.subjectForKey(ek, true), Empty, { headers: h });
  }

  _buildCC(
    k: string,
    history = false,
    opts: Partial<ConsumerConfig> = {},
  ): Partial<ConsumerConfig> {
    const ek = this.encodeKey(k);
    this.validateSearchKey(k);

    return Object.assign({
      "deliver_policy": history
        ? DeliverPolicy.All
        : DeliverPolicy.LastPerSubject,
      "ack_policy": AckPolicy.None,
      "filter_subject": this.fullKeyName(ek),
      "flow_control": true,
      "idle_heartbeat": nanos(5 * 1000),
    }, opts) as Partial<ConsumerConfig>;
  }

  remove(k: string): Promise<void> {
    return this.purge(k);
  }

  async history(
    opts: { key?: string; headers_only?: boolean } = {},
  ): Promise<QueuedIterator<KvEntry>> {
    const k = opts.key ?? ">";
    const qi = new QueuedIteratorImpl<KvEntry>();
    const co = {} as ConsumerConfig;
    co.headers_only = opts.headers_only || false;

    let fn: (() => void) | undefined;
    fn = () => {
      qi.stop();
    };
    let count = 0;

    const cc = this._buildCC(k, true, co);
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
    copts.bindStream(this.stream);
    copts.orderedConsumer();
    copts.callback((err, jm) => {
      if (err) {
        // sub done
        qi.stop(err);
        return;
      }
      if (jm) {
        const e = this.jmToEntry(jm);
        qi.push(e);
        qi.received++;
        //@ts-ignore - function will be removed
        if (fn && count > 0 && qi.received >= count || jm.info.pending === 0) {
          //@ts-ignore: we are injecting an unexpected type
          qi.push(fn);
          fn = undefined;
        }
      }
    });

    const sub = await this.js.subscribe(subj, copts);
    // by the time we are here, likely the subscription got messages
    if (fn) {
      const { info: { last } } = sub as unknown as {
        info: { last: ConsumerInfo };
      };
      // this doesn't sound correct - we should be looking for a seq number instead
      // then if we see a greater one, we are done.
      const expect = last.num_pending + last.delivered.consumer_seq;
      // if the iterator already queued - the only issue is other modifications
      // did happen like stream was pruned, and the ordered consumer reset, etc
      // we won't get what we are expecting - so the notification will never fire
      // the sentinel ought to be coming from the server
      if (expect === 0 || qi.received >= expect) {
        try {
          fn();
        } catch (err) {
          // fail it - there's something wrong in the user callback
          qi.stop(err);
        } finally {
          fn = undefined;
        }
      } else {
        count = expect;
      }
    }
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

  async watch(
    opts: {
      key?: string;
      headers_only?: boolean;
      initializedFn?: () => void;
    } = {},
  ): Promise<QueuedIterator<KvEntry>> {
    const k = opts.key ?? ">";
    const qi = new QueuedIteratorImpl<KvEntry>();
    const co = {} as Partial<ConsumerConfig>;
    co.headers_only = opts.headers_only || false;

    let fn = opts.initializedFn;
    let count = 0;

    const cc = this._buildCC(k, false, co);
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
    copts.bindStream(this.stream);
    copts.orderedConsumer();
    copts.callback((err, jm) => {
      if (err) {
        // sub done
        qi.stop(err);
        return;
      }
      if (jm) {
        const e = this.jmToEntry(jm);
        qi.push(e);
        qi.received++;

        // count could have changed or has already been received
        if (
          fn && (count > 0 && qi.received >= count || jm.info.pending === 0)
        ) {
          //@ts-ignore: we are injecting an unexpected type
          qi.push(fn);
          fn = undefined;
        }
      }
    });

    const sub = await this.js.subscribe(subj, copts);
    // by the time we are here, likely the subscription got messages
    if (fn) {
      const { info: { last } } = sub as unknown as {
        info: { last: ConsumerInfo };
      };
      // this doesn't sound correct - we should be looking for a seq number instead
      // then if we see a greater one, we are done.
      const expect = last.num_pending + last.delivered.consumer_seq;
      // if the iterator already queued - the only issue is other modifications
      // did happen like stream was pruned, and the ordered consumer reset, etc
      // we won't get what we are expecting - so the notification will never fire
      // the sentinel ought to be coming from the server
      if (expect === 0 || qi.received >= expect) {
        try {
          fn();
        } catch (err) {
          // fail it - there's something wrong in the user callback
          qi.stop(err);
        } finally {
          fn = undefined;
        }
      } else {
        count = expect;
      }
    }
    qi._data = sub;
    qi.iterClosed.then(() => {
      sub.unsubscribe();
    });
    sub.closed.then(() => {
      qi.stop();
    }).catch((err: Error) => {
      qi.stop(err);
    });

    return qi;
  }

  async keys(k = ">"): Promise<QueuedIterator<string>> {
    const keys = new QueuedIteratorImpl<string>();
    const cc = this._buildCC(k, false, { headers_only: true });
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
    copts.bindStream(this.stream);
    copts.orderedConsumer();

    const sub = await this.js.subscribe(subj, copts);
    (async () => {
      for await (const jm of sub) {
        const op = jm.headers?.get(kvOperationHdr);
        if (op !== "DEL" && op !== "PURGE") {
          const key = this.decodeKey(jm.subject.substring(this.prefixLen));
          keys.push(key);
        }
        if (jm.info.pending === 0) {
          sub.unsubscribe();
        }
      }
    })()
      .then(() => {
        keys.stop();
      })
      .catch((err) => {
        keys.stop(err);
      });

    const si = sub as unknown as JetStreamSubscriptionInfoable;
    if (si.info!.last.num_pending === 0) {
      sub.unsubscribe();
    }
    return keys;
  }

  purgeBucket(opts?: PurgeOpts): Promise<PurgeResponse> {
    return this.jsm.streams.purge(this.bucketName(), opts);
  }

  destroy(): Promise<boolean> {
    return this.jsm.streams.delete(this.bucketName());
  }

  async status(): Promise<KvStatus> {
    const nc = (this.js as unknown as { nc: NatsConnection }).nc;
    const cluster = nc.info?.cluster ?? "";
    const bn = this.bucketName();
    const si = await this.jsm.streams.info(bn);
    return new KvStatusImpl(si, cluster);
  }
}

export class KvStatusImpl implements KvStatus {
  si: StreamInfo;
  cluster: string;

  constructor(si: StreamInfo, cluster = "") {
    this.si = si;
    this.cluster = cluster;
  }

  get bucket(): string {
    return this.si.config.name.startsWith(kvPrefix)
      ? this.si.config.name.substring(kvPrefix.length)
      : this.si.config.name;
  }

  get values(): number {
    return this.si.state.messages;
  }

  get history(): number {
    return this.si.config.max_msgs_per_subject;
  }

  get ttl(): number {
    return millis(this.si.config.max_age);
  }

  get bucket_location(): string {
    return this.cluster;
  }

  get backingStore(): StorageType {
    return this.si.config.storage;
  }

  get storage(): StorageType {
    return this.si.config.storage;
  }

  get replicas(): number {
    return this.si.config.num_replicas;
  }

  get description(): string {
    return this.si.config.description ?? "";
  }

  get maxBucketSize(): number {
    return this.si.config.max_bytes;
  }

  get maxValueSize(): number {
    return this.si.config.max_msg_size;
  }

  get max_bytes(): number {
    return this.si.config.max_bytes;
  }

  get placement(): Placement {
    return this.si.config.placement || { cluster: "", tags: [] };
  }

  get placementCluster(): string {
    return this.si.config.placement?.cluster ?? "";
  }

  get republish(): Republish {
    return this.si.config.republish ?? { src: "", dest: "" };
  }

  get streamInfo(): StreamInfo {
    return this.si;
  }

  get size(): number {
    return this.si.state.bytes;
  }
}

class KvStoredEntryImpl implements KvEntry {
  bucket: string;
  sm: StoredMsg;
  prefixLen: number;

  constructor(bucket: string, prefixLen: number, sm: StoredMsg) {
    this.bucket = bucket;
    this.prefixLen = prefixLen;
    this.sm = sm;
  }

  get key(): string {
    return this.sm.subject.substring(this.prefixLen);
  }

  get value(): Uint8Array {
    return this.sm.data;
  }

  get delta(): number {
    return 0;
  }

  get created(): Date {
    return this.sm.time;
  }

  get revision(): number {
    return this.sm.seq;
  }

  get operation(): OperationType {
    return this.sm.header.get(kvOperationHdr) as OperationType || "PUT";
  }

  get length(): number {
    const slen = this.sm.header.get(JsHeaders.MessageSizeHdr) || "";
    if (slen !== "") {
      return parseInt(slen, 10);
    }
    return this.sm.data.length;
  }

  json<T>(): T {
    return this.sm.json();
  }

  string(): string {
    return this.sm.string();
  }
}

class KvJsMsgEntryImpl implements KvEntry {
  bucket: string;
  key: string;
  sm: JsMsg;

  constructor(bucket: string, key: string, sm: JsMsg) {
    this.bucket = bucket;
    this.key = key;
    this.sm = sm;
  }

  get value(): Uint8Array {
    return this.sm.data;
  }

  get created(): Date {
    return new Date(millis(this.sm.info.timestampNanos));
  }

  get revision(): number {
    return this.sm.seq;
  }

  get operation(): OperationType {
    return this.sm.headers?.get(kvOperationHdr) as OperationType || "PUT";
  }

  get delta(): number {
    return this.sm.info.pending;
  }

  get length(): number {
    const slen = this.sm.headers?.get(JsHeaders.MessageSizeHdr) || "";
    if (slen !== "") {
      return parseInt(slen, 10);
    }
    return this.sm.data.length;
  }

  json<T>(): T {
    return this.sm.json();
  }

  string(): string {
    return this.sm.string();
  }
}

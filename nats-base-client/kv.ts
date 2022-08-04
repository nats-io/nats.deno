/*
 * Copyright 2021-2022 The NATS Authors
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
  AckPolicy,
  callbackFn,
  ConsumerConfig,
  ConsumerInfo,
  DeliverPolicy,
  DiscardPolicy,
  Empty,
  JetStreamClient,
  JetStreamManager,
  JetStreamOptions,
  JetStreamPublishOptions,
  JsHeaders,
  JsMsg,
  KV,
  KvCodec,
  KvCodecs,
  KvEntry,
  KvOptions,
  KvPutOptions,
  KvRemove,
  KvStatus,
  MsgRequest,
  Placement,
  PurgeOpts,
  PurgeResponse,
  RetentionPolicy,
  StorageType,
  StoredMsg,
  StreamConfig,
} from "./types.ts";
import {
  JetStreamClientImpl,
  JetStreamSubscriptionInfoable,
} from "./jsclient.ts";
import { millis, nanos } from "./jsutil.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { headers, MsgHdrs } from "./headers.ts";
import { consumerOpts, deferred, ErrorCode } from "./mod.ts";
import { compare, parseSemVer } from "./semver.ts";
import { JetStreamManagerImpl } from "./jsm.ts";

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
const kvPrefix = "KV_";
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
  jsm: JetStreamManager;
  js: JetStreamClient;
  stream!: string;
  bucket: string;
  direct!: boolean;
  codec!: KvCodecs;
  _prefixLen: number;
  subjPrefix: string;

  constructor(bucket: string, jsm: JetStreamManager, js: JetStreamClient) {
    validateBucket(bucket);
    this.jsm = jsm;
    this.js = js;
    this.bucket = bucket;
    this._prefixLen = 0;
    this.subjPrefix = kvSubjectPrefix;

    const jsi = js as JetStreamClientImpl;
    const prefix = jsi.prefix || "$JS.API";
    if (prefix !== "$JS.API") {
      this.subjPrefix = `${prefix}.${kvSubjectPrefix}`;
    }
  }

  static async create(
    js: JetStreamClient,
    name: string,
    opts: Partial<KvOptions> = {},
  ): Promise<KV> {
    validateBucket(name);
    const to = opts.timeout || 2000;
    const jsi = js as JetStreamClientImpl;
    let jsopts = jsi.opts || {} as JetStreamOptions;
    jsopts = Object.assign(jsopts, { timeout: to });
    const jsm = await jsi.nc.jetstreamManager(jsopts);
    const bucket = new Bucket(name, jsm, js);
    await bucket.init(opts);
    return bucket;
  }

  static async bind(
    js: JetStreamClient,
    name: string,
    opts: Partial<{ codec: KvCodecs }> = {},
  ): Promise<KV> {
    const jsi = js as JetStreamClientImpl;
    const jsm = await jsi.nc.jetstreamManager();
    const info = await jsm.streams.info(`${kvPrefix}${name}`);
    validateBucket(info.config.name);
    const bucket = new Bucket(name, jsm, js);
    Object.assign(bucket, info);
    bucket.codec = opts.codec || NoopKvCodecs();
    bucket.direct = info.config.allow_direct ?? false;
    return bucket;
  }

  async init(opts: Partial<KvOptions> = {}): Promise<void> {
    const bo = Object.assign(defaultBucketOpts(), opts) as KvOptions;
    this.codec = bo.codec;
    const sc = {} as StreamConfig;
    this.stream = sc.name = opts.streamName ?? this.bucketName();
    sc.subjects = [this.subjectForBucket()];
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

    const nci = (this.js as JetStreamClientImpl).nc;
    const have = nci.getServerVersion();
    const discardNew = have ? compare(have, parseSemVer("2.7.2")) >= 0 : false;
    sc.discard = discardNew ? DiscardPolicy.New : DiscardPolicy.Old;

    const direct = have ? compare(have, parseSemVer("2.8.5")) >= 0 : false;
    sc.allow_direct = opts.allow_direct ? direct : false;
    this.direct = sc.allow_direct;

    sc.num_replicas = bo.replicas;
    if (bo.ttl) {
      sc.max_age = nanos(bo.ttl);
    }
    sc.allow_rollup_hdrs = true;

    try {
      await this.jsm.streams.info(sc.name);
    } catch (err) {
      if (err.message === "stream not found") {
        await this.jsm.streams.add(sc);
      }
    }
  }

  bucketName(): string {
    return this.stream ?? `${kvPrefix}${this.bucket}`;
  }

  subjectForBucket(): string {
    return `${this.subjPrefix}.${this.bucket}.>`;
  }

  subjectForKey(k: string): string {
    return `${this.subjPrefix}.${this.bucket}.${k}`;
  }

  fullKeyName(k: string): string {
    return `${kvSubjectPrefix}.${this.bucket}.${k}`;
  }

  get prefixLen(): number {
    if (this._prefixLen === 0) {
      this._prefixLen = `${kvSubjectPrefix}.${this.bucket}.`.length;
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
    return {
      bucket: this.bucket,
      key: sm.subject.substring(this.prefixLen),
      value: sm.data,
      delta: 0,
      created: sm.time,
      revision: sm.seq,
      operation: sm.header.get(kvOperationHdr) as OperationType || "PUT",
      length: this.dataLen(sm.data, sm.header),
    };
  }

  jmToEntry(jm: JsMsg): KvEntry {
    const key = this.decodeKey(jm.subject.substring(this.prefixLen));
    return {
      bucket: this.bucket,
      key: key,
      value: jm.data,
      created: new Date(millis(jm.info.timestampNanos)),
      revision: jm.seq,
      operation: jm.headers?.get(kvOperationHdr) as OperationType || "PUT",
      delta: jm.info.pending,
      length: this.dataLen(jm.data, jm.headers),
    } as KvEntry;
  }

  create(k: string, data: Uint8Array): Promise<number> {
    return this.put(k, data, { previousSeq: 0 });
  }

  update(k: string, data: Uint8Array, version: number): Promise<number> {
    if (version <= 0) {
      throw new Error("version must be greater than 0");
    }
    return this.put(k, data, { previousSeq: version });
  }

  async put(
    k: string,
    data: Uint8Array,
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
    const pa = await this.js.publish(this.subjectForKey(ek), data, o);
    return pa.seq;
  }

  async get(
    k: string,
    opts?: { revision: number },
  ): Promise<KvEntry | null> {
    const ek = this.encodeKey(k);
    this.validateKey(ek);

    let arg: MsgRequest = { last_by_subj: this.fullKeyName(ek) };
    if (opts && opts.revision > 0) {
      arg = { seq: opts.revision };
    }

    let sm: StoredMsg;
    try {
      if (this.direct) {
        const jsmi = this.jsm as JetStreamManagerImpl;
        sm = await jsmi.direct.getMessage(this.bucketName(), arg);
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
    await this.js.publish(this.subjectForKey(ek), Empty, { headers: h });
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

    let fn: callbackFn | undefined;
    fn = () => {
      qi.stop();
    };
    let count = 0;

    const cc = this._buildCC(k, true, co);
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
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
      initializedFn?: callbackFn;
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
    }).catch((err) => {
      qi.stop(err);
    });

    return qi;
  }

  async keys(k = ">"): Promise<QueuedIterator<string>> {
    const keys = new QueuedIteratorImpl<string>();
    const cc = this._buildCC(k, false, { headers_only: true });
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
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
    const ji = this.js as JetStreamClientImpl;
    const cluster = ji.nc.info?.cluster ?? "";
    const si = await this.jsm.streams.info(this.bucketName());
    return {
      bucket: this.bucketName(),
      values: si.state.messages,
      history: si.config.max_msgs_per_subject,
      ttl: si.config.max_age,
      bucket_location: cluster,
      backingStore: si.config.storage,
      storage: si.config.storage,
      replicas: si.config.num_replicas,
    } as KvStatus;
  }
}

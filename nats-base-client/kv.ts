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

import {
  ConsumerOpts,
  DiscardPolicy,
  JetStreamClient,
  JetStreamManager,
  JetStreamPublishOptions,
  JsMsg,
  Nanos,
  NatsConnection,
  PurgeOpts,
  PurgeResponse,
  RetentionPolicy,
  StorageType,
  StoredMsg,
  StreamConfig,
} from "./types.ts";
import { JetStreamClientImpl } from "./jsclient.ts";
import { createInbox, headers, millis } from "./mod.ts";
import { deferred, timeout } from "./util.ts";

export type Result = {
  bucket: string;
  key: string;
  data: Uint8Array; // data lines up better with other nats apis in javascript
  created: Date;
  sequence: number;
  delta?: number;
  "origin_cluster"?: string;
  operation: "PUT" | "DEL"; // these are redundant - a delete is empty payload...
};

export interface Status {
  bucket: string;
  values: number;
  history: number;
  ttl: Nanos;
  cluster?: string;
  keys(): Promise<string[]>;
  backingStore: string;
}

export interface BucketOpts {
  replicas: number;
  history: number;
  timeout: number;
  maxBucketSize: number;
  maxValueSize: number;
  placementCluster: string;
  mirrorBucket: string;
  ttl: number;
  streamName: string;
}

export function defaultBucketOpts(): Partial<BucketOpts> {
  return {
    replicas: 1,
    history: 1,
    timeout: 2000,
    maxBucketSize: -1,
    maxValueSize: -1,
  };
}

export interface PutOptions {
  previousSeq: number;
}

const kvOriginClusterHdr = "KV-Origin-Cluster";
const kvOperationHdr = "KV-Operation";
const kvPrefix = "KV_";
const kvSubjectPrefix = "$KV";

export interface RoKV {
  get(k: string): Promise<Result>;
  history(k: string): Promise<Result[]>;
  // watch(opts: { key?: string }): Iterator<string>;
  close(): Promise<void>;
  // status(): Promise<Status>;
}

export interface KV extends RoKV {
  put(k: string, data: Uint8Array, opts?: Partial<PutOptions>): Promise<number>;
  delete(k: string): Promise<void>;
  purge(opts?: PurgeOpts): Promise<PurgeResponse>;
  destroy(): Promise<boolean>;
}

export class Bucket implements KV {
  jsm: JetStreamManager;
  js: JetStreamClient;
  bucket: string;
  maxPerSubject!: number;

  constructor(bucket: string, jsm: JetStreamManager, js: JetStreamClient) {
    this.jsm = jsm;
    this.js = js;
    this.bucket = bucket;
  }

  async init(opts: Partial<BucketOpts> = {}): Promise<void> {
    const sc = {} as StreamConfig;
    sc.name = opts.streamName ?? this.bucketName();
    sc.subjects = [this.subjectForBucket()];
    sc.retention = RetentionPolicy.Limits;
    this.maxPerSubject = sc.max_msgs_per_subject = opts.history ?? 5;
    sc.max_msgs = -1;
    sc.storage = StorageType.File;
    sc.discard = DiscardPolicy.Old;
    sc.num_replicas = opts.replicas ?? 1;

    try {
      await this.jsm.streams.info(sc.name);
    } catch (err) {
      if (err.message === "stream not found") {
        await this.jsm.streams.add(sc);
      }
    }
  }

  bucketName(): string {
    return `${kvPrefix}${this.bucket}`;
  }

  subjectForBucket(): string {
    return `${kvSubjectPrefix}.${this.bucket}.*`;
  }

  subjectForKey(k: string): string {
    return `${kvSubjectPrefix}.${this.bucket}.${k}`;
  }

  static async create(
    nc: NatsConnection,
    name: string,
    opts: Partial<BucketOpts> = {},
  ): Promise<KV> {
    const to = opts.timeout || 2000;
    const jsm = await nc.jetstreamManager({ timeout: to });
    const bucket = new Bucket(name, jsm, nc.jetstream({ timeout: to }));
    await bucket.init(opts);
    return bucket;
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  async delete(k: string): Promise<void> {
    const ji = this.js as JetStreamClientImpl;
    const cluster = ji.nc.info?.cluster ?? "";
    const h = headers();
    h.set(kvOriginClusterHdr, cluster);
    h.set(kvOperationHdr, "DEL");
    await this.js.publish(this.subjectForKey(k));
  }

  toResult(k: string, sm: StoredMsg): Result {
    return {
      bucket: this.bucket,
      key: k,
      data: sm.data,
      created: sm.time,
      sequence: sm.seq,
      origin_cluster: sm.header.get(kvOriginClusterHdr),
      operation: sm.header.get(kvOperationHdr) === "DEL" ? "DEL" : "PUT",
    };
  }

  jtoResult(k: string, jm: JsMsg): Result {
    return {
      bucket: this.bucket,
      key: k,
      data: jm.data,
      created: new Date(millis(jm.info.timestampNanos)),
      sequence: jm.seq,
      origin_cluster: jm.headers?.get(kvOriginClusterHdr),
      operation: jm.headers?.get(kvOperationHdr) === "DEL" ? "DEL" : "PUT",
    };
  }

  async get(k: string): Promise<Result> {
    const sm = await this.jsm.streams.getMessage(this.bucketName(), {
      last_by_subj: this.subjectForKey(k),
    });
    return this.toResult(k, sm);
  }

  async history(k: string): Promise<Result[]> {
    const ji = this.js as JetStreamClientImpl;
    const nc = ji.nc;
    const buf: Result[] = [];

    const inbox = createInbox(nc.options.inboxPrefix);
    const d = deferred<Result[]>();

    const opts = {
      config: { deliver_subject: inbox },
    } as ConsumerOpts;
    // fixme: change to create my own ephemeral, and have heartbeats to bail early
    const sub = await this.js.subscribe(this.subjectForKey(k), opts);
    const to = timeout(ji.timeout);
    to.catch(() => {
      sub.unsubscribe();
      d.resolve(buf);
    });
    (async () => {
      for await (const m of sub) {
        to.cancel();
        const r = this.jtoResult(k, m);
        buf.push(r);
        if (m.info.pending === 0) {
          sub.unsubscribe();
          d.resolve(buf);
        }
        m.ack();
      }
    })().catch((err) => {
      d.reject(err);
    });
    return d;
  }

  purge(opts?: PurgeOpts): Promise<PurgeResponse> {
    return this.jsm.streams.purge(this.bucketName(), opts);
  }

  destroy(): Promise<boolean> {
    return this.jsm.streams.delete(this.bucketName());
  }

  async put(
    k: string,
    data: Uint8Array,
    opts: Partial<PutOptions> = {},
  ): Promise<number> {
    const ji = this.js as JetStreamClientImpl;
    const cluster = ji.nc.info?.cluster ?? "";
    const h = headers();
    h.set(kvOriginClusterHdr, cluster);
    if (opts.previousSeq) {
      const o = {} as JetStreamPublishOptions;
      o.expect = {};
      o.expect.lastSubjectSequence = opts.previousSeq;
      const pa = await this.js.publish(this.subjectForKey(k), data, o);
      return pa.seq;
    } else {
      const pa = await this.js.publish(this.subjectForKey(k), data);
      return pa.seq;
    }
  }

  // status(): Promise<Status> {
  //   return Promise.resolve(undefined);
  // }
  //
  // watch(opts: { key?: string }): Iterator<string> {
  //   return undefined;
  // }
}

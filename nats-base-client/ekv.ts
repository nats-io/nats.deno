import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { Empty, PurgeOpts, PurgeResponse } from "./types.ts";
import { Codec } from "./codec.ts";
import { Bucket, Entry, KvStatus, PutOptions } from "./kv.ts";
import { JetStreamManagerImpl } from "./jsm.ts";
import { checkJsError, isFlowControlMsg, isHeartbeatMsg } from "./jsutil.ts";
import { isNatsError } from "./error.ts";
import { toJsMsg } from "./jsmsg.ts";

export interface EncodedEntry<T> {
  bucket: string;
  key: string;
  value?: T;
  created: Date;
  seq: number;
  delta?: number;
  "origin_cluster"?: string;
  operation: "PUT" | "DEL";
}

export interface EncodedRoKV<T> {
  get(k: string): Promise<EncodedEntry<T> | null>;
  history(opts?: { key?: string }): Promise<QueuedIterator<EncodedEntry<T>>>;
  watch(opts?: { key?: string }): Promise<QueuedIterator<EncodedEntry<T>>>;
  close(): Promise<void>;
  status(): Promise<KvStatus>;
  keys(): Promise<string[]>;
}

export interface EncodedKV<T> extends EncodedRoKV<T> {
  put(k: string, data?: T, opts?: Partial<PutOptions>): Promise<number>;
  delete(k: string): Promise<void>;
  purge(opts?: PurgeOpts): Promise<PurgeResponse>;
  destroy(): Promise<boolean>;
}

export class EncodedBucket<T> implements EncodedKV<T> {
  bucket: Bucket;
  codec: Codec<T>;
  constructor(bucket: Bucket, codec: Codec<T>) {
    this.bucket = bucket;
    this.codec = codec;
  }

  toEncodedEntry(e: Entry): EncodedEntry<T> {
    return {
      bucket: this.bucket.bucket,
      key: e.key,
      value: e.value.length > 0 ? this.codec.decode(e.value) : undefined,
      delta: 0,
      created: e.created,
      seq: e.seq,
      origin_cluster: e.origin_cluster,
      operation: e.operation,
    };
  }

  put(
    k: string,
    data: T,
    opts: Partial<PutOptions> = {},
  ): Promise<number> {
    const buf = data ? this.codec.encode(data) : Empty;
    return this.bucket.put(k, buf, opts);
  }

  async get(k: string): Promise<EncodedEntry<T> | null> {
    const v = await this.bucket.get(k);
    if (v) {
      return this.toEncodedEntry(v);
    }
    return null;
  }

  delete(k: string): Promise<void> {
    return this.bucket.delete(k);
  }

  async history(
    opts: { key?: string } = {},
  ): Promise<QueuedIterator<EncodedEntry<T>>> {
    const qi = new QueuedIteratorImpl<EncodedEntry<T>>();
    const iter = await this.bucket.history(opts);
    (async () => {
      for await (const e of iter) {
        qi.received++;
        qi.push(this.toEncodedEntry(e));
      }
    })().then(() => {
      qi.stop();
    }).catch((err) => {
      qi.stop(err);
    });
    return qi;
  }

  async watch(
    opts: { key?: string } = {},
  ): Promise<QueuedIterator<EncodedEntry<T>>> {
    const qi = new QueuedIteratorImpl<EncodedEntry<T>>();
    const iter = await this.bucket.watch(opts);
    (async () => {
      for await (const e of iter) {
        qi.received++;
        qi.push(this.toEncodedEntry(e));
      }
    })().then(() => {
      qi.stop();
      iter.stop();
    }).catch((err) => {
      qi.stop(err);
      iter.stop();
    });
    return qi;
  }

  keys(): Promise<string[]> {
    return this.bucket.keys();
  }

  purge(opts?: PurgeOpts): Promise<PurgeResponse> {
    return this.bucket.purge(opts);
  }

  destroy(): Promise<boolean> {
    return this.bucket.destroy();
  }

  status(): Promise<KvStatus> {
    return this.bucket.status();
  }

  close(): Promise<void> {
    return this.bucket.close();
  }
}

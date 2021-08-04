import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { Empty, PurgeOpts, PurgeResponse } from "./types.ts";
import { Codec } from "./codec.ts";
import { Bucket, Entry, KvStatus, PutOptions } from "./kv.ts";

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
  history(k: string): Promise<QueuedIterator<EncodedEntry<T>>>;
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

  async toEncodedIter(
    src: QueuedIterator<Entry>,
  ): Promise<QueuedIterator<EncodedEntry<T>>> {
    const iter = new QueuedIteratorImpl<EncodedEntry<T>>();
    await (async () => {
      for await (const e of src) {
        iter.push(this.toEncodedEntry(e));
      }
    })();
    iter.stop();
    return iter;
  }

  async history(k: string): Promise<QueuedIterator<EncodedEntry<T>>> {
    return this.toEncodedIter(await this.bucket.history(k));
  }

  async watch(
    opts: { key?: string } = {},
  ): Promise<QueuedIterator<EncodedEntry<T>>> {
    return this.toEncodedIter(await this.bucket.watch(opts));
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

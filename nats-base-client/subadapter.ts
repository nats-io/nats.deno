import {
  Deferred,
  Msg,
  NatsConnection,
  NatsError,
  QueuedIterator,
  Sub,
  SubOpts,
  SubscriptionImpl,
} from "./nbc_mod.ts";
import type { Subscription } from "./nbc_mod.ts";
import { DispatchedFn } from "./queued_iterator.ts";

export type AdapterCallback<T> = (err: NatsError | null, msg: T | null) => void;

/**
 * the adapter is a function that looks like a callback, but returns
 * a tuple of error or the new message type. If it is handled an
 * error, it is expected, to simply return it along with a no-op type
 */
export type MsgAdapter<T> = (
  err: NatsError | null,
  msg: Msg,
) => [NatsError | null, T | null];

export interface ConsumerAdapterOpts<T> extends SubOpts<T> {
  callback?: AdapterCallback<T>;
  dispatchedFn?: DispatchedFn<T>;
  adapter: MsgAdapter<T>;
  cleanupFn?: (sub: Subscription, info?: unknown) => void;
}

/**
 * SubscriptionAdapter wraps a subscription to provide payload specific
 * subscription semantics. That is messages are a transport
 * for user data, but the user code wants to avoid repetitive encoding
 * and decoding.
 */
export class SubscriptionAdapter<T> extends QueuedIterator<T>
  implements Sub<T> {
  nc: NatsConnection;
  sub: SubscriptionImpl;
  adapter: MsgAdapter<T>;

  constructor(
    nc: NatsConnection,
    subject: string,
    opts: ConsumerAdapterOpts<T>,
  ) {
    super();
    this.nc = nc;
    if (typeof opts.adapter !== "function") {
    }
    this.adapter = opts.adapter;
    this.noIterator = typeof opts.callback === "function";
    if (opts.dispatchedFn) {
      this.dispatchedFn = opts.dispatchedFn;
    }
    let callback = (err: NatsError | null, msg: Msg) => {
      this.callback(err, msg);
    };
    if (opts.callback) {
      const uh = opts.callback;
      callback = (err: NatsError | null, msg: Msg) => {
        const [jer, tm] = this.adapter(err, msg);
        uh(jer, tm);
        if (this.yieldedCb && tm) {
          this.yieldedCb(tm);
        }
      };
    }
    const { max, queue, timeout } = opts;
    const sopts = { max, queue, timeout, callback };
    this.sub = this.nc.subscribe(subject, sopts) as SubscriptionImpl;
    if (opts.cleanupFn) {
      this.sub.cleanupFn = opts.cleanupFn;
    }
    (async () => {
      await this.closed;
      this.stop();
    })().catch();
  }

  unsubscribe(max?: number): void {
    this.sub.unsubscribe(max);
  }

  drain(): Promise<void> {
    return this.sub.drain();
  }

  isDraining(): boolean {
    return this.sub.isDraining();
  }

  isClosed(): boolean {
    return this.sub.isClosed();
  }

  callback(e: NatsError | null, msg: Msg): void {
    const [err, tm] = this.adapter(e, msg);
    if (err) {
      this.stop(err);
    }
    if (tm) {
      this.push(tm);
    }
  }

  getSubject(): string {
    return this.sub.getSubject();
  }

  getReceived(): number {
    return this.sub.getReceived();
  }

  getProcessed(): number {
    return this.sub.getProcessed();
  }

  getPending(): number {
    return this.sub.getPending();
  }

  getID(): number {
    return this.sub.getID();
  }

  getMax(): number | undefined {
    return this.sub.getMax();
  }

  get closed(): Deferred<void> {
    return this.sub.closed;
  }
}

/*
 * Copyright 2020-2021 The NATS Authors
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
  Deferred,
  Msg,
  NatsConnection,
  NatsError,
  QueuedIterator,
  Sub,
  SubOpts,
  SubscriptionImpl,
} from "./internal_mod.ts";
import type { Subscription } from "./internal_mod.ts";
import { DispatchedFn } from "./queued_iterator.ts";
import { ErrorCode } from "./error.ts";

/**
 * Converts a NATS message into some other type. Implementers are expected to:
 * return [err, null] if the message callback is invoked with an error.
 * return [err, null] if converting the message yielded an error, note that
 * iterators will stop on the error, but callbacks will be presented with
 * the error.
 * return [null, T] if the conversion worked correctly
 */
export type MsgAdapter<T> = (
  err: NatsError | null,
  msg: Msg,
) => [NatsError | null, T | null];

/**
 * Callback presented to the user with the converted type
 */
export type TypedCallback<T> = (err: NatsError | null, msg: T | null) => void;

export interface TypedSubscriptionOptions<T> extends SubOpts<T> {
  adapter: MsgAdapter<T>;
  callback?: TypedCallback<T>;
  dispatchedFn?: DispatchedFn<T>;
  cleanupFn?: (sub: Subscription, info?: unknown) => void;
}

function checkFn(fn: unknown, name: string, required = false) {
  if (required === true && !fn) {
    throw NatsError.errorForCode(
      ErrorCode.ApiError,
      new Error(`${name} is not a function`),
    );
  }
  if (fn && typeof fn !== "function") {
    throw NatsError.errorForCode(
      ErrorCode.ApiError,
      new Error(`${name} is not a function`),
    );
  }
}

/**
 * TypedSubscription wraps a subscription to provide payload specific
 * subscription semantics. That is messages are a transport
 * for user data, and the data is presented as application specific
 * data to the client.
 */
export class TypedSubscription<T> extends QueuedIterator<T> implements Sub<T> {
  nc: NatsConnection;
  sub: SubscriptionImpl;
  adapter: MsgAdapter<T>;

  constructor(
    nc: NatsConnection,
    subject: string,
    opts: TypedSubscriptionOptions<T>,
  ) {
    super();
    this.nc = nc;

    checkFn(opts.adapter, "adapter", true);
    this.adapter = opts.adapter;

    if (opts.callback) {
      checkFn(opts.callback, "callback");
    }
    this.noIterator = typeof opts.callback === "function";

    if (opts.dispatchedFn) {
      checkFn(opts.dispatchedFn, "dispatchedFn");
      this.dispatchedFn = opts.dispatchedFn;
    }
    if (opts.cleanupFn) {
      checkFn(opts.cleanupFn, "cleanupFn");
    }

    let callback = (err: NatsError | null, msg: Msg) => {
      this.callback(err, msg);
    };
    if (opts.callback) {
      const uh = opts.callback;
      callback = (err: NatsError | null, msg: Msg) => {
        const [jer, tm] = this.adapter(err, msg);
        uh(jer, tm);
        if (this.dispatchedFn && tm) {
          this.dispatchedFn(tm);
        }
      };
    }
    const { max, queue, timeout } = opts;
    const sopts = { max, queue, timeout, callback };
    this.sub = this.nc.subscribe(subject, sopts) as SubscriptionImpl;
    if (opts.cleanupFn) {
      this.sub.cleanupFn = opts.cleanupFn;
    }
    (async (s) => {
      await s.closed;
      this.stop();
    })(this.sub).catch((err) => {
      console.log("err", err);
    });
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

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
import { Deferred, deferred } from "./util.ts";
import { ErrorCode, NatsError } from "./error.ts";

export interface Dispatcher<T> {
  push(v: T): void;
}

export type ProtocolFilterFn<T> = (data: T | null) => boolean;
export type DispatchedFn<T> = (data: T | null) => void;

export interface QueuedIterator<T> extends Dispatcher<T> {
  [Symbol.asyncIterator](): AsyncIterator<T>;
  stop(err?: Error): void;
  getProcessed(): number;
  getPending(): number;
  getReceived(): number;
}

export class QueuedIteratorImpl<T> implements QueuedIterator<T> {
  inflight: number;
  processed: number;
  // FIXME: this is updated by the protocol
  received: number;
  protected noIterator: boolean;
  iterClosed: Deferred<void>;
  protected done: boolean;
  private signal: Deferred<void>;
  private yields: T[];
  protocolFilterFn?: ProtocolFilterFn<T>;
  dispatchedFn?: DispatchedFn<T>;
  private err?: Error;

  constructor() {
    this.inflight = 0;
    this.processed = 0;
    this.received = 0;
    this.noIterator = false;
    this.done = false;
    this.signal = deferred<void>();
    this.yields = [];
    this.iterClosed = deferred<void>();
  }

  [Symbol.asyncIterator]() {
    return this.iterate();
  }

  push(v: T): void {
    if (this.done) {
      return;
    }
    this.yields.push(v);
    this.signal.resolve();
  }

  async *iterate(): AsyncIterableIterator<T> {
    if (this.noIterator) {
      throw new NatsError("unsupported iterator", ErrorCode.ApiError);
    }
    try {
      while (true) {
        if (this.yields.length === 0) {
          await this.signal;
        }
        if (this.err) {
          throw this.err;
        }
        const yields = this.yields;
        this.inflight = yields.length;
        this.yields = [];
        for (let i = 0; i < yields.length; i++) {
          // only pass messages that pass the filter
          const ok = this.protocolFilterFn
            ? this.protocolFilterFn(yields[i])
            : true;
          if (ok) {
            this.processed++;
            yield yields[i];
            if (this.dispatchedFn && yields[i]) {
              this.dispatchedFn(yields[i]);
            }
          }
          this.inflight--;
        }
        // yielding could have paused and microtask
        // could have added messages. Prevent allocations
        // if possible
        if (this.done) {
          break;
        } else if (this.yields.length === 0) {
          yields.length = 0;
          this.yields = yields;
          this.signal = deferred();
        }
      }
    } finally {
      // the iterator used break/return
      this.stop();
    }
  }

  stop(err?: Error): void {
    if (this.done) {
      return;
    }
    this.err = err;
    this.done = true;
    this.signal.resolve();
    this.iterClosed.resolve();
  }

  getProcessed(): number {
    return this.noIterator ? this.received : this.processed;
  }

  getPending(): number {
    return this.yields.length + this.inflight;
  }

  getReceived(): number {
    return this.received;
  }
}

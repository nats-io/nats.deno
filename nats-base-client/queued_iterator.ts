/*
 * Copyright 2020 The NATS Authors
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
import { deferred, Deferred } from "./util.ts";

export interface Dispatcher<T> {
  push(v: T): void;
}

export class QueuedIterator<T> implements Dispatcher<T> {
  processed = 0;
  received = 0; // this is updated by the protocol
  protected done: boolean = false;
  private signal: Deferred<void> = deferred<void>();
  private yields: T[] = [];
  private err?: Error;

  [Symbol.asyncIterator]() {
    return this.iterate();
  }

  push(v: T) {
    if (this.done) {
      return;
    }
    this.yields.push(v);
    this.signal.resolve();
  }

  async *iterate(): AsyncIterableIterator<T> {
    while (true) {
      await this.signal;
      if (this.err) {
        throw this.err;
      }
      while (this.yields.length > 0) {
        this.processed++;
        const v = this.yields.shift();
        yield v!;
      }
      if (this.done) {
        break;
      } else {
        this.signal = deferred();
      }
    }
  }

  stop(err?: Error): void {
    this.err = err;
    this.done = true;
    this.signal.resolve();
  }

  getProcessed(): number {
    return this.processed;
  }

  getPending(): number {
    return this.yields.length;
  }

  getReceived(): number {
    return this.received;
  }
}

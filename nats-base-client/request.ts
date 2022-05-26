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
import { Deferred, deferred, Timeout, timeout } from "./util.ts";
import type { Msg, RequestManyOptions, RequestOptions } from "./types.ts";
import { RequestStrategy } from "./types.ts";
import { ErrorCode, NatsError } from "./error.ts";
import { MuxSubscription } from "./muxsubscription.ts";
import { nuid } from "./nuid.ts";

export interface Request {
  token: string;
  requestSubject: string;
  received: number;
  resolver(err: Error | null, msg: Msg): void;
  cancel(err?: NatsError): void;
}

export class BaseRequest {
  token: string;
  received: number;
  ctx: Error;
  requestSubject: string;
  mux: MuxSubscription;

  constructor(
    mux: MuxSubscription,
    requestSubject: string,
  ) {
    this.mux = mux;
    this.requestSubject = requestSubject;
    this.received = 0;
    this.token = nuid.next();
    this.ctx = new Error();
  }
}

export interface RequestManyOptionsInternal extends RequestManyOptions {
  callback: (err: Error | null, msg: Msg | null) => void;
}

/**
 * Request expects multiple message response
 * the request ends when the timer expires,
 * an error arrives or an expected count of messages
 * arrives, end is signaled by a null message
 */
export class RequestMany extends BaseRequest implements Request {
  callback!: (err: Error | null, msg: Msg | null) => void;
  done: Deferred<void>;
  timer: number;
  max: number;
  opts: Partial<RequestManyOptionsInternal>;
  constructor(
    mux: MuxSubscription,
    requestSubject: string,
    opts: Partial<RequestManyOptions> = { maxWait: 1000 },
  ) {
    super(mux, requestSubject);
    this.opts = opts;
    if (typeof this.opts.callback !== "function") {
      throw new Error("callback is required");
    }
    this.callback = this.opts.callback;

    this.max = typeof opts.maxMessages === "number" && opts.maxMessages > 0
      ? opts.maxMessages
      : -1;
    this.done = deferred();
    this.done.then(() => {
      this.callback(null, null);
    });
    // @ts-ignore: node is not a number
    this.timer = setTimeout(() => {
      this.cancel();
    }, opts.maxWait);
  }

  cancel(err?: NatsError): void {
    if (err) {
      this.callback(err, null);
    }
    clearTimeout(this.timer);
    this.mux.cancel(this);
    this.done.resolve();
  }

  resolver(err: Error | null, msg: Msg): void {
    if (err) {
      err.stack += `\n\n${this.ctx.stack}`;
      this.cancel(err as NatsError);
    } else {
      this.callback(null, msg);
      if (this.opts.strategy === RequestStrategy.Count) {
        this.max--;
        if (this.max === 0) {
          this.cancel();
        }
      }

      if (this.opts.strategy === RequestStrategy.JitterTimer) {
        clearTimeout(this.timer);
        // @ts-ignore: node is not a number
        this.timer = setTimeout(() => {
          this.cancel();
        }, 300);
      }

      if (this.opts.strategy === RequestStrategy.SentinelMsg) {
        if (msg && msg.data.length === 0) {
          this.cancel();
        }
      }
    }
  }
}

export class RequestOne extends BaseRequest implements Request {
  deferred: Deferred<Msg>;
  timer: Timeout<Msg>;

  constructor(
    mux: MuxSubscription,
    requestSubject: string,
    opts: RequestOptions = { timeout: 1000 },
  ) {
    super(mux, requestSubject);
    // extend(this, opts);
    this.deferred = deferred();
    this.timer = timeout<Msg>(opts.timeout);
  }

  resolver(err: Error | null, msg: Msg): void {
    if (this.timer) {
      this.timer.cancel();
    }
    if (err) {
      err.stack += `\n\n${this.ctx.stack}`;
      this.deferred.reject(err);
    } else {
      this.deferred.resolve(msg);
    }
    this.cancel();
  }

  cancel(err?: NatsError): void {
    if (this.timer) {
      this.timer.cancel();
    }
    this.mux.cancel(this);
    this.deferred.reject(
      err ? err : NatsError.errorForCode(ErrorCode.Cancelled),
    );
  }
}

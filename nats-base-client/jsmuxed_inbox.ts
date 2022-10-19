/*
 * Copyright 2022 The NATS Authors
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
import { Msg, NatsConnection, NatsError, Subscription } from "./types.ts";
import { MsgImpl } from "./msg.ts";
import { createInbox } from "./protocol.ts";
import { JSONCodec } from "./codec.ts";
import { Deferred, deferred } from "./util.ts";
import { nuid } from "./nuid.ts";

const jc = JSONCodec();

/**
 * The context for a JsMuxRequest
 */
type JsMuxRequestContext<T> = {
  /**
   * ID for the inbox (matches the last token in the request subject)
   */
  id: string;
  /**
   * Actual inbox subject that matches this context
   */
  inbox: string;
  /**
   * Number of messages processed
   */
  msgs: number;
  /**
   * Number of message bytes processed
   */
  bytes: number;
  /**
   * Mux request argument or undefined if this is the default context
   */
  args?: T;
};

/**
 * JsMuxedInbox is similar to mux inboxes, with the exception that
 * the request arguments are captured as a JsMuxRequestContext.
 * Instead of calling a standard callback, the callback includes the
 * JsMuxRequestContext captured during the request.
 *
 * If a request is made that doesn't match any messages sent via `request()`,
 * an JsMuxRequestContext with a blank id/inbox and args is provided.
 *
 * Note that this is very specific to message handling in JetStream
 * pull request management, as requests generate a context where
 * control messages can be matched to the original request.
 */
export class JsMuxedInbox<T> {
  nc: NatsConnection;
  cb: (
    cookie: JsMuxRequestContext<T> | null,
    err: NatsError | null,
    msg: Msg | null,
  ) => void;
  sub: Subscription;
  prefix: string;
  tokenStart: number;
  muxes: Map<string, JsMuxRequestContext<T>>;
  general: JsMuxRequestContext<T>;
  done: Deferred<void>;
  constructor(
    nc: NatsConnection,
    cb: (
      cookie: JsMuxRequestContext<T> | null,
      err: NatsError | null,
      msg: Msg | null,
    ) => void,
  ) {
    this.nc = nc;
    this.prefix = createInbox();
    this.tokenStart = this.prefix.length + 1;
    this.general = {
      id: "",
      inbox: "",
      msgs: 0,
      bytes: 0,
    } as JsMuxRequestContext<
      T
    >;
    this.cb = cb;
    this.done = deferred<void>();

    this.sub = nc.subscribe(`${this.prefix}.*`, {
      callback: (err, msg) => {
        let cookie = null;
        if (msg?.subject.startsWith(`${this.prefix}.`)) {
          const mux = msg?.subject.substring(this.tokenStart);
          cookie = this.muxes.get(mux) ?? null;
        }
        if (cookie === null) {
          cookie = this.general;
        }
        if (cookie) {
          cookie.msgs++;
          cookie.bytes += (msg as MsgImpl).size();
        }
        this.cb(cookie, err, msg);
      },
    });
    this.muxes = new Map<string, JsMuxRequestContext<T>>();
    this.sub.closed.then(() => {
      this.done.resolve();
    });
  }

  _add(cookie: T): string {
    const id = nuid.next();
    const r = {
      id,
      inbox: `${this.prefix}.${id}`,
      args: Object.assign({}, cookie) as T,
      msgs: 0,
      bytes: 0,
    } as JsMuxRequestContext<T>;
    this.muxes.set(r.id, r);
    return r.inbox;
  }

  /**
   * Delete a context - for example JetStream sends a
   * response to the mux that the client understood ends
   * the pull request.
   * @param cookie
   */
  delete(cookie: JsMuxRequestContext<T>) {
    this.muxes.delete(cookie.id);
  }

  /**
   * Stop the subscription by draining
   */
  close(): Promise<void> {
    return this.sub.drain();
  }

  /**
   * Publish a request that handles the reply in the JsMuxedInbox callback
   * @param subject
   * @param payload
   */
  request(subject: string, payload: T) {
    const bytes = jc.encode(payload);
    const inbox = this._add(payload);
    this.nc.publish(subject, bytes, { reply: inbox });
  }
}

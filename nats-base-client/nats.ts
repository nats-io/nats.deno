/*
 * Copyright 2018-2022 The NATS Authors
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

import { deferred, isUint8Array } from "./util.ts";
import { createInbox, ProtocolHandler } from "./protocol.ts";
import { SubscriptionImpl } from "./subscription.ts";
import { ErrorCode, NatsError } from "./error.ts";
import {
  ConnectionOptions,
  Empty,
  JetStreamClient,
  JetStreamManager,
  JetStreamOptions,
  Msg,
  NatsConnection,
  PublishOptions,
  RequestOptions,
  ServerInfo,
  Stats,
  Status,
  Subscription,
  SubscriptionOptions,
} from "./types.ts";

import type { SemVer } from "./semver.ts";
import { parseSemVer } from "./semver.ts";

import { parseOptions } from "./options.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { Request } from "./request.ts";
import { isRequestError } from "./msg.ts";
import { JetStreamManagerImpl } from "./jsm.ts";
import { JetStreamClientImpl } from "./jsclient.ts";

export class NatsConnectionImpl implements NatsConnection {
  options: ConnectionOptions;
  protocol!: ProtocolHandler;
  draining: boolean;
  listeners: QueuedIterator<Status>[];

  private constructor(opts: ConnectionOptions) {
    this.draining = false;
    this.options = parseOptions(opts);
    this.listeners = [];
  }

  public static connect(opts: ConnectionOptions = {}): Promise<NatsConnection> {
    return new Promise<NatsConnection>((resolve, reject) => {
      const nc = new NatsConnectionImpl(opts);
      ProtocolHandler.connect(nc.options, nc)
        .then((ph: ProtocolHandler) => {
          nc.protocol = ph;
          (async function () {
            for await (const s of ph.status()) {
              nc.listeners.forEach((l) => {
                l.push(s);
              });
            }
          })();
          resolve(nc);
        })
        .catch((err: Error) => {
          reject(err);
        });
    });
  }

  closed(): Promise<void | Error> {
    return this.protocol.closed;
  }

  async close() {
    await this.protocol.close();
  }

  _check(subject: string, sub: boolean, pub: boolean) {
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.ConnectionClosed);
    }
    if (sub && this.isDraining()) {
      throw NatsError.errorForCode(ErrorCode.ConnectionDraining);
    }
    if (pub && this.protocol.noMorePublishing) {
      throw NatsError.errorForCode(ErrorCode.ConnectionDraining);
    }
    subject = subject || "";
    if (subject.length === 0) {
      throw NatsError.errorForCode(ErrorCode.BadSubject);
    }
  }

  publish(
    subject: string,
    data: Uint8Array = Empty,
    options?: PublishOptions,
  ): void {
    this._check(subject, false, true);
    // if argument is not undefined/null and not a Uint8Array, toss
    if (data && !isUint8Array(data)) {
      throw NatsError.errorForCode(ErrorCode.BadPayload);
    }
    this.protocol.publish(subject, data, options);
  }

  subscribe(
    subject: string,
    opts: SubscriptionOptions = {},
  ): Subscription {
    this._check(subject, true, false);
    const sub = new SubscriptionImpl(this.protocol, subject, opts);
    this.protocol.subscribe(sub);
    return sub;
  }

  _resub(s: Subscription, subject: string, max?: number) {
    this._check(subject, true, false);
    const si = s as SubscriptionImpl;
    // FIXME: need way of understanding a callbacks processed
    //   count without it, we cannot really do much - ie
    //   for rejected messages, the count would be lower, etc.
    //   To handle cases were for example KV is building a map
    //   the consumer would say how many messages we need to do
    //   a proper build before we can handle updates.
    si.max = max; // this might clear it
    if (max) {
      // we cannot auto-unsub, because we don't know the
      // number of messages we processed vs received
      // allow the auto-unsub on processMsg to work if they
      // we were called with a new max
      si.max = max + si.received;
    }
    this.protocol.resub(si, subject);
  }

  request(
    subject: string,
    data: Uint8Array = Empty,
    opts: RequestOptions = { timeout: 1000, noMux: false },
  ): Promise<Msg> {
    try {
      this._check(subject, true, true);
    } catch (err) {
      return Promise.reject(err);
    }
    opts.timeout = opts.timeout || 1000;
    if (opts.timeout < 1) {
      return Promise.reject(new NatsError("timeout", ErrorCode.InvalidOption));
    }
    if (!opts.noMux && opts.reply) {
      return Promise.reject(
        new NatsError(
          "reply can only be used with noMux",
          ErrorCode.InvalidOption,
        ),
      );
    }

    if (opts.noMux) {
      const inbox = opts.reply
        ? opts.reply
        : createInbox(this.options.inboxPrefix);
      const d = deferred<Msg>();
      const errCtx = new Error();
      const sub = this.subscribe(
        inbox,
        {
          max: 1,
          timeout: opts.timeout,
          callback: (err, msg) => {
            if (err) {
              // timeouts from `timeout()` will have the proper stack
              if (err.code !== ErrorCode.Timeout) {
                err.stack += `\n\n${errCtx.stack}`;
              }
              d.reject(err);
            } else {
              err = isRequestError(msg);
              if (err) {
                // if we failed here, help the developer by showing what failed
                err.stack += `\n\n${errCtx.stack}`;
                d.reject(err);
              } else {
                d.resolve(msg);
              }
            }
          },
        },
      );
      (sub as SubscriptionImpl).requestSubject = subject;
      this.protocol.publish(subject, data, {
        reply: inbox,
        headers: opts.headers,
      });
      return d;
    } else {
      const r = new Request(this.protocol.muxSubscriptions, subject, opts);
      this.protocol.request(r);

      try {
        this.publish(
          subject,
          data,
          {
            reply: `${this.protocol.muxSubscriptions.baseInbox}${r.token}`,
            headers: opts.headers,
          },
        );
      } catch (err) {
        r.cancel(err);
      }

      const p = Promise.race([r.timer, r.deferred]);
      p.catch(() => {
        r.cancel();
      });
      return p;
    }
  }

  /** *
   * Flushes to the server. Promise resolves when round-trip completes.
   * @returns {Promise<void>}
   */
  flush(): Promise<void> {
    if (this.isClosed()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.ConnectionClosed),
      );
    }
    return this.protocol.flush();
  }

  drain(): Promise<void> {
    if (this.isClosed()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.ConnectionClosed),
      );
    }
    if (this.isDraining()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.ConnectionDraining),
      );
    }
    this.draining = true;
    return this.protocol.drain();
  }

  isClosed(): boolean {
    return this.protocol.isClosed();
  }

  isDraining(): boolean {
    return this.draining;
  }

  getServer(): string {
    const srv = this.protocol.getServer();
    return srv ? srv.listen : "";
  }

  status(): AsyncIterable<Status> {
    const iter = new QueuedIteratorImpl<Status>();
    this.listeners.push(iter);
    return iter;
  }

  get info(): (ServerInfo | undefined) {
    return this.protocol.isClosed() ? undefined : this.protocol.info;
  }

  stats(): Stats {
    return {
      inBytes: this.protocol.inBytes,
      outBytes: this.protocol.outBytes,
      inMsgs: this.protocol.inMsgs,
      outMsgs: this.protocol.outMsgs,
    };
  }

  async jetstreamManager(
    opts: JetStreamOptions = {},
  ): Promise<JetStreamManager> {
    const adm = new JetStreamManagerImpl(this, opts);
    try {
      await adm.getAccountInfo();
    } catch (err) {
      const ne = err as NatsError;
      if (ne.code === ErrorCode.NoResponders) {
        ne.code = ErrorCode.JetStreamNotEnabled;
      }
      throw ne;
    }
    return adm;
  }

  jetstream(
    opts: JetStreamOptions = {},
  ): JetStreamClient {
    return new JetStreamClientImpl(this, opts);
  }

  getServerVersion(): SemVer | undefined {
    const info = this.info;
    return info ? parseSemVer(info.version) : undefined;
  }

  async rtt(): Promise<number> {
    if (!this.protocol._closed && !this.protocol.connected) {
      throw NatsError.errorForCode(ErrorCode.Disconnect);
    }
    const start = Date.now();
    await this.flush();
    return Date.now() - start;
  }
}

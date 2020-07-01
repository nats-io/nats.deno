/*
 * Copyright 2018-2020 The NATS Authors
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

//@ts-ignore
import { extend, isUint8Array } from "./util.ts";
import {
  Payload,
  ConnectionOptions,
  Msg,
  SubscriptionOptions,
  Events,
  //@ts-ignore
} from "./mod.ts";
import {
  ProtocolHandler,
  RequestOptions,
  Subscription,
  //@ts-ignore
} from "./protocol.ts";
//@ts-ignore
import { ErrorCode, NatsError } from "./error.ts";
//@ts-ignore
import { Nuid } from "./nuid.ts";
import { DebugEvents, defaultReq, defaultSub } from './types.ts'
import { parseOptions } from "./options.ts";

export const nuid = new Nuid();

export class NatsConnection extends EventTarget {
  options: ConnectionOptions;
  protocol!: ProtocolHandler;
  draining: boolean = false;

  private constructor(opts: ConnectionOptions) {
    super();
    this.options = parseOptions(opts);
  }

  public static connect(opts: ConnectionOptions): Promise<NatsConnection> {
    return new Promise<NatsConnection>((resolve, reject) => {
      let nc = new NatsConnection(opts);
      ProtocolHandler.connect(nc.options, nc)
        .then((ph: ProtocolHandler) => {
          nc.protocol = ph;
          // setup a listener to forward lifecycle events
          const handler = (evt: Event): void | Promise<void> => {
            const { detail } = evt as CustomEvent;
            const e = detail
              ? new CustomEvent(evt.type, { detail })
              : new Event(evt.type);
            nc.dispatchEvent(e);
          };
          ph.addEventListener(Events.DISCONNECT, handler);
          ph.addEventListener(Events.RECONNECT, handler);
          ph.addEventListener(DebugEvents.RECONNECTING, handler);
          ph.addEventListener(Events.UPDATE, handler);
          resolve(nc);
        })
        .catch((err: Error) => {
          reject(err);
        });
    });
  }

  status(): Promise<void | Error> {
    return this.protocol.closed;
  }

  async close() {
    await this.protocol.close();
  }

  publish(subject: string, data: any = undefined, reply: string = ""): void {
    subject = subject || "";
    if (subject.length === 0) {
      throw (NatsError.errorForCode(ErrorCode.BAD_SUBJECT));
    }
    // we take string, object to JSON and Uint8Array - if argument is not
    // Uint8Array, then process the payload
    if (!isUint8Array(data)) {
      if (this.options.payload !== Payload.JSON) {
        data = data || "";
      } else {
        data = data === undefined ? null : data;
        data = JSON.stringify(data);
      }
      // here we are a string
      data = new TextEncoder().encode(data);
    }

    this.protocol.publish(subject, data, reply);
  }

  subscribe(
    subject: string,
    cb: (error: NatsError | null, msg: Msg) => void,
    opts: SubscriptionOptions = {},
  ): Subscription {
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
    }
    if (this.isDraining()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING);
    }
    subject = subject || "";
    if (subject.length === 0) {
      throw NatsError.errorForCode(ErrorCode.BAD_SUBJECT);
    }

    let s = defaultSub();
    extend(s, opts);
    s.subject = subject;
    s.callback = cb;
    return this.protocol.subscribe(s);
  }

  request(
    subject: string,
    timeout: number = 1000,
    data: any = undefined,
  ): Promise<Msg> {
    if (this.isClosed()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED),
      );
    }
    if (this.isDraining()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING),
      );
    }
    subject = subject || "";
    if (subject.length === 0) {
      return Promise.reject(NatsError.errorForCode(ErrorCode.BAD_SUBJECT));
    }

    return new Promise<Msg>((resolve, reject) => {
      let r = defaultReq();
      let opts = { max: 1 } as RequestOptions;
      extend(r, opts);
      r.token = nuid.next();
      //@ts-ignore
      r.timeout = setTimeout(() => {
        request.cancel();
        reject(NatsError.errorForCode(ErrorCode.CONNECTION_TIMEOUT));
      }, timeout);
      r.callback = (err: Error | null, msg: Msg) => {
        if (err) {
          reject(msg);
        } else {
          resolve(msg);
        }
      };
      let request = this.protocol.request(r);
      this.publish(
        subject,
        data,
        `${this.protocol.muxSubscriptions.baseInbox}${r.token}`,
      );
    });
  }

  /***
     * Flushes to the server. If a callback is provided, the callback is c
     * @returns {Promise<void>}
     */
  flush(): Promise<void> {
    return this.protocol.flush();
  }

  drain(): Promise<void> {
    if (this.isClosed()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED),
      );
    }
    if (this.isDraining()) {
      return Promise.reject(
        NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING),
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
    return srv ? srv.url.host : "";
  }
}

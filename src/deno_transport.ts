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

import { BufWriter } from "https://deno.land/std/io/mod.ts";
import { Deferred, deferred } from "https://deno.land/std/async/mod.ts";
import Conn = Deno.Conn;
import {
  CLOSE_EVT,
  ConnectionOptions,
  DataBuffer,
  ErrorCode,
  extractProtocolMessage,
  INFO,
  NatsError,
  render,
  Transport,
} from "../nats-base-client/mod.ts";

const VERSION = "0.0.1";
const LANG = "Deno";

// if trying to simply write to the connection for some reason
// messages are dropped - deno websocket implementation does this.
export async function write(
  frame: Uint8Array,
  writer: BufWriter,
): Promise<void> {
  await writer.write(frame);
  await writer.flush();
}

export class DenoTransport extends EventTarget implements Transport {
  version: string = VERSION;
  lang: string = LANG;
  closeError?: Error;
  private options!: ConnectionOptions;
  private buf: Uint8Array;
  private encrypted = false;
  private closed = false;
  private conn!: Conn;
  private writer!: BufWriter;

  // the async writes to the socket do not guarantee
  // the order of the writes - this leads to interleaving
  // which results in protocol errors on the server
  private sendQueue: Array<{
    frame: Uint8Array;
    d: Deferred<void>;
  }> = [];

  constructor() {
    super();
    this.buf = new Uint8Array(1024 * 8);
  }

  async connect(
    hp: { hostname: string; port: number },
    options: ConnectionOptions,
  ): Promise<any> {
    this.options = options;
    try {
      this.conn = await Deno.connect(hp);
      const info = await this.peekInfo();
      this.checkOpts(info);

      // @ts-ignore
      const { tls_required } = info;
      if (tls_required) {
        await this.startTLS(hp.hostname);
      } else {
        this.writer = new BufWriter(this.conn);
      }
      return Promise.resolve();
    } catch (err) {
      return Promise.reject(err);
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }

  async peekInfo(): Promise<object> {
    const inbound = new DataBuffer();
    let pm: string;
    while (true) {
      let c = await this.conn.read(this.buf);
      if (c) {
        if (null === c) {
          // EOF
          return Promise.reject(
            new Error("socket closed while expecting INFO"),
          );
        }
        const frame = this.buf.slice(0, c);
        if (this.options.debug) {
          console.info(`> ${render(frame)}`);
        }
        inbound.fill(frame);
        const raw = inbound.peek();
        pm = extractProtocolMessage(raw);
        if (pm) {
          break;
        }
      }
    }
    // reset the buffer to previously read, so the client
    // can validate the info matches the connection
    this.buf = new Uint8Array(inbound.drain());
    // expecting the info protocol
    const m = INFO.exec(pm);
    if (!m) {
      return Promise.reject(new Error("unexpected response from server"));
    }
    return Promise.resolve(JSON.parse(m[1]));
  }

  checkOpts(info: object) {
    //@ts-ignore
    const { proto } = info;
    if ((proto === undefined || proto < 1) && this.options.noEcho) {
      throw new NatsError("noEcho", ErrorCode.SERVER_OPTION_NA);
    }
  }

  async startTLS(hostname: string): Promise<void> {
    this.conn = await Deno.startTls(this.conn, { hostname });
    this.encrypted = true;
    this.writer = new BufWriter(this.conn);
    return Promise.resolve();
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    let reason: Error | undefined;
    // yield what we initially read
    yield this.buf;

    this.buf = new Uint8Array(64 * 1024);
    while (!this.closed) {
      try {
        let c = await this.conn.read(this.buf);
        if (c === null) {
          break;
        }
        if (c) {
          const frame = this.buf.slice(0, c);
          if (this.options.debug) {
            console.info(`> ${render(frame)}`);
          }
          yield frame;
        }
      } catch (err) {
        reason = err;
        break;
      }
    }
    this._closed(reason);
  }

  private enqueue(frame: Uint8Array): Promise<void> {
    if (this.closed) {
      return Promise.resolve();
    }
    const d = deferred<void>();
    this.sendQueue.push({ frame, d });
    if (this.sendQueue.length === 1) {
      this.dequeue();
    }
    return d;
  }

  private dequeue(): void {
    const [entry] = this.sendQueue;
    if (!entry) return;
    if (this.closed) return;
    const { frame, d } = entry;
    write(frame, this.writer)
      .then(() => {
        if (this.options.debug) {
          console.info(`< ${render(frame)}`);
        }
        d.resolve();
      })
      .catch((err) => {
        if (this.options.debug) {
          console.error(`!!! ${render(frame)}: ${err}`);
        }
        d.reject(err);
      })
      .finally(() => {
        this.sendQueue.shift();
        this.dequeue();
      });
  }

  send(frame: Uint8Array): Promise<void> {
    return this.enqueue(frame);
  }

  isEncrypted(): boolean {
    return this.encrypted;
  }

  async close(err?: Error): Promise<void> {
    return this._closed(err, false);
  }

  private async _closed(err?: Error, internal: boolean = true): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.closeError = err;
    if (!err) {
      try {
        // this is a noop for the server, but gives us a place to hang
        // a close and ensure that we sent all before closing
        await this.enqueue(new TextEncoder().encode("+OK\r\n"));
      } catch (err) {
        if (this.options.debug) {
          console.log("nats close terminated with an error", err);
        }
      }
    }
    try {
      this.conn?.close();
    } catch (err) {
    }

    if (internal) {
      this.dispatchEvent(new Event(CLOSE_EVT));
    }
  }
}

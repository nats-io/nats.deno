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

import { BufWriter } from "https://deno.land/std@0.80.0/io/mod.ts";
import { Deferred, deferred } from "https://deno.land/std@0.80.0/async/mod.ts";
import Conn = Deno.Conn;
import {
  checkOptions,
  ConnectionOptions,
  DataBuffer,
  ErrorCode,
  extractProtocolMessage,
  INFO,
  NatsError,
  render,
  TE,
  Transport,
} from "../nats-base-client/internal_mod.ts";
import type { TlsOptions } from "../nats-base-client/types.ts";

const VERSION = "1.0.0-13";
const LANG = "nats.deno";

// if trying to simply write to the connection for some reason
// messages are dropped - deno websocket implementation does this.
export async function write(
  frame: Uint8Array,
  writer: BufWriter,
): Promise<void> {
  await writer.write(frame);
  await writer.flush();
}

export class DenoTransport implements Transport {
  version: string = VERSION;
  lang: string = LANG;
  closeError?: Error;
  private options!: ConnectionOptions;
  private buf: Uint8Array;
  private encrypted = false;
  private done = false;
  private closedNotification: Deferred<void | Error> = deferred();
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
    this.buf = new Uint8Array(1024 * 8);
  }

  async connect(
    hp: { hostname: string; port: number; tlsName: string },
    options: ConnectionOptions,
  ): Promise<any> {
    this.options = options;
    try {
      this.conn = await Deno.connect(hp);
      const info = await this.peekInfo();
      checkOptions(info, this.options);
      // @ts-ignore
      const { tls_required } = info;
      if (tls_required) {
        const tlsn = hp.tlsName ? hp.tlsName : hp.hostname;
        await this.startTLS(tlsn);
      } else {
        this.writer = new BufWriter(this.conn);
      }
    } catch (err) {
      err = err.name === "ConnectionRefused"
        ? NatsError.errorForCode(ErrorCode.CONNECTION_REFUSED)
        : err;
      throw err;
    }
  }

  get isClosed(): boolean {
    return this.done;
  }

  async peekInfo(): Promise<object> {
    const inbound = new DataBuffer();
    let pm: string;
    while (true) {
      const c = await this.conn.read(this.buf);
      if (c === null) {
        // EOF
        throw new Error("socket closed while expecting INFO");
      } else if (c) {
        const frame = this.buf.subarray(0, c);
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
      throw new Error("unexpected response from server");
    }
    return JSON.parse(m[1]);
  }

  async startTLS(hostname: string): Promise<void> {
    const tls = this.options && this.options.tls
      ? this.options.tls
      : {} as TlsOptions;
    this.conn = await Deno.startTls(
      this.conn,
      { hostname, certFile: tls.caFile },
    );
    this.encrypted = true;
    this.writer = new BufWriter(this.conn);
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    let reason: Error | undefined;
    // yield what we initially read
    yield this.buf;

    while (!this.done) {
      try {
        this.buf = new Uint8Array(64 * 1024);
        let c = await this.conn.read(this.buf);
        if (c === null) {
          break;
        }
        if (c) {
          const frame = this.buf.subarray(0, c);
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
    if (this.done) {
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
    if (this.done) return;
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

  disconnect() {
    this._closed(undefined, true)
      .then().catch();
  }

  private async _closed(err?: Error, internal: boolean = true): Promise<void> {
    if (this.done) return;
    this.closeError = err;
    if (!err) {
      try {
        // this is a noop but gives us a place to hang
        // a close and ensure that we sent all before closing
        await this.enqueue(TE.encode(""));
      } catch (err) {
        if (this.options.debug) {
          console.log("transport close terminated with an error", err);
        }
      }
    }
    this.done = true;
    try {
      this.conn.close();
    } catch (err) {
    }

    if (internal) {
      this.closedNotification.resolve(err);
    }
  }

  closed(): Promise<void | Error> {
    return this.closedNotification;
  }
}

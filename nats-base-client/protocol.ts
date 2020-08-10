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
import {
  ConnectionOptions,
  Events,
  Status,
  DebugEvents,
  DEFAULT_RECONNECT_TIME_WAIT,
  Subscription,
  DEFAULT_PING_INTERVAL,
  DEFAULT_MAX_PING_OUT,
} from "./types.ts";
import { Transport, newTransport } from "./transport.ts";
import { ErrorCode, NatsError } from "./error.ts";
import {
  CR_LF,
  buildMessage,
  extend,
  extractProtocolMessage,
  timeout,
  deferred,
  Deferred,
  delay,
  render,
} from "./util.ts";
import { Nuid } from "./nuid.ts";
import { DataBuffer } from "./databuffer.ts";
import { Server, Servers } from "./servers.ts";
import { Dispatcher, QueuedIterator } from "./queued_iterator.ts";
import { MsgHdrs, MsgHdrsImpl } from "./headers.ts";
import { SubscriptionImpl } from "./subscription.ts";
import { Subscriptions } from "./subscriptions.ts";
import { MuxSubscription } from "./muxsubscription.ts";
import { Request } from "./request.ts";
import { Heartbeat, PH } from "./heartbeats.ts";
import {
  describe,
  Kind,
  MsgArg,
  Parser,
  ParserEvent,
} from "./parser.ts";
import { MsgImpl } from "./msg.ts";

const nuid = new Nuid();

const FLUSH_THRESHOLD = 1024 * 8;

const PING = /^PING\r\n/i;
const PONG = /^PONG\r\n/i;
const SUBRE = /^SUB\s+([^\r\n]+)\r\n/i;
export const INFO = /^INFO\s+([^\r\n]+)\r\n/i;

export enum ParserState {
  CLOSED = -1,
  AWAITING_CONTROL = 0,
  AWAITING_MSG_PAYLOAD = 1,
}

export function createInbox(): string {
  return `_INBOX.${nuid.next()}`;
}

export class Connect {
  echo?: boolean;
  no_responders?: boolean;
  protocol: number = 1;

  constructor(
    transport: { version: string; lang: string },
    opts: ConnectionOptions,
    nonce?: string,
  ) {
    if (opts.noEcho) {
      this.echo = false;
    }
    if (opts.noResponders) {
      this.no_responders = true;
    }
    const creds = (opts?.authenticator ? opts.authenticator(nonce) : {}) || {};
    extend(this, opts, transport, creds);
  }
}

export interface Publisher {
  publish(
    subject: string,
    data: any,
    options?: { reply?: string; headers?: MsgHdrs },
  ): void;
}

export class ProtocolHandler implements Dispatcher<ParserEvent> {
  connected: boolean = false;
  connectedOnce: boolean = false;
  infoReceived: boolean = false;
  info?: any;
  muxSubscriptions: MuxSubscription;
  options: ConnectionOptions;
  outbound: DataBuffer;
  pongs: Array<Deferred<void>>;
  pout: number = 0;
  subscriptions: Subscriptions;
  transport!: Transport;
  noMorePublishing: boolean = false;
  connectError?: Function;
  publisher: Publisher;
  closed: Deferred<Error | void>;
  listeners: QueuedIterator<Status>[] = [];
  heartbeats: Heartbeat;
  parser: Parser;

  private servers: Servers;
  private server!: Server;

  constructor(options: ConnectionOptions, publisher: Publisher) {
    this.options = options;
    this.publisher = publisher;
    this.subscriptions = new Subscriptions();
    this.muxSubscriptions = new MuxSubscription();
    this.outbound = new DataBuffer();
    this.pongs = [];
    this.servers = new Servers(
      !options.noRandomize,
      //@ts-ignore
      options.servers,
    );
    this.closed = deferred<Error | void>();
    this.parser = new Parser(this);

    this.heartbeats = new Heartbeat(
      this as PH,
      this.options.pingInterval || DEFAULT_PING_INTERVAL,
      this.options.maxPingOut || DEFAULT_MAX_PING_OUT,
    );
  }

  resetOutbound(): void {
    const pending = this.outbound;
    this.outbound = new DataBuffer();
    // strip any pings/pongs/subs we have
    pending.buffers.forEach((buf) => {
      const m = extractProtocolMessage(buf);
      if (PING.exec(m)) {
        return;
      }
      if (SUBRE.exec(m)) {
        return;
      }
      if (PONG.exec(m)) {
        return;
      }
      this.outbound.fill(buf);
    });

    const pongs = this.pongs;
    this.pongs = [];
    // reject the pongs
    pongs.forEach((p) => {
      p.reject(NatsError.errorForCode(ErrorCode.DISCONNECT));
    });
    this.parser = new Parser(this);
    this.infoReceived = false;
  }

  dispatchStatus(status: Status): void {
    this.listeners.forEach((q) => {
      q.push(status);
    });
  }

  status(): AsyncIterable<Status> {
    const iter = new QueuedIterator<Status>();
    this.listeners.push(iter);
    return iter;
  }

  private prepare(): Deferred<void> {
    this.resetOutbound();

    const pong = deferred<void>();
    this.pongs.unshift(pong);

    this.connectError = undefined;

    this.connectError = (err: NatsError) => {
      pong.reject(err);
    };

    this.transport = newTransport();
    this.transport.closed()
      .then(async (err?) => {
        this.connected = false;
        if (!this.parser.closed()) {
          await this.disconnected(this.transport.closeError);
          return;
        }
      });

    return pong;
  }

  public disconnect(): void {
    this.dispatchStatus({ type: DebugEvents.STALE_CONNECTION, data: "" });
    this.transport.disconnect();
  }

  async disconnected(err?: Error): Promise<void> {
    this.dispatchStatus(
      {
        type: Events.DISCONNECT,
        data: this.servers.getCurrentServer().toString(),
      },
    );
    if (this.options.reconnect) {
      await this.dialLoop()
        .then(() => {
          this.dispatchStatus(
            {
              type: Events.RECONNECT,
              data: this.servers.getCurrentServer().toString(),
            },
          );
        })
        .catch((err) => {
          this._close(err);
        });
    } else {
      await this._close();
    }
  }

  dial(srv: Server): Promise<void> {
    const pong = this.prepare();
    const timer = timeout(this.options.timeout || 20000);
    this.transport.connect(srv.hostport(), this.options)
      .then(() => {
        (async () => {
          try {
            for await (const b of this.transport) {
              this.parser.parse(b);
            }
          } catch (err) {
            console.log("reader closed", err);
          }
        })();
      })
      .catch((err: Error) => {
        pong.reject(err);
      });
    return Promise.race([timer, pong])
      .then(() => {
        timer.cancel();
        this.connected = true;
        this.connectError = undefined;
        this.sendSubscriptions();
        this.connectedOnce = true;
        this.server.didConnect = true;
        this.server.reconnects = 0;
        this.infoReceived = true;
        this.flushPending();
        this.heartbeats.start();
      })
      .catch((err) => {
        timer.cancel();
        this.transport?.close(err);
        throw err;
      });
  }

  async dialLoop(): Promise<void> {
    let lastError: Error | undefined;
    while (true) {
      let wait = this.options.reconnectDelayHandler
        ? this.options.reconnectDelayHandler()
        : DEFAULT_RECONNECT_TIME_WAIT;
      let maxWait = wait;
      const srv = this.selectServer();
      if (!srv) {
        throw lastError || NatsError.errorForCode(ErrorCode.CONNECTION_REFUSED);
      }
      const now = Date.now();
      if (srv.lastConnect === 0 || srv.lastConnect + wait <= now) {
        srv.lastConnect = Date.now();
        try {
          this.dispatchStatus(
            { type: DebugEvents.RECONNECTING, data: srv.toString() },
          );
          await this.dial(srv);
          break;
        } catch (err) {
          lastError = err;
          if (!this.connectedOnce) {
            if (!this.options.waitOnFirstConnect) {
              this.servers.removeCurrentServer();
            }
            continue;
          }
          srv.reconnects++;
          const mra = this.options.maxReconnectAttempts || 0;
          if (mra !== -1 && srv.reconnects >= mra) {
            this.servers.removeCurrentServer();
          }
        }
      } else {
        maxWait = Math.min(maxWait, srv.lastConnect + wait - now);
        await delay(maxWait);
      }
    }
  }

  public static async connect(
    options: ConnectionOptions,
    publisher: Publisher,
  ): Promise<ProtocolHandler> {
    const h = new ProtocolHandler(options, publisher);
    await h.dialLoop();
    return h;
  }

  static toError(s: string) {
    let t = s ? s.toLowerCase() : "";
    if (t.indexOf("permissions violation") !== -1) {
      return new NatsError(s, ErrorCode.PERMISSIONS_VIOLATION);
    } else if (t.indexOf("authorization violation") !== -1) {
      return new NatsError(s, ErrorCode.AUTHORIZATION_VIOLATION);
    } else {
      return new NatsError(s, ErrorCode.NATS_PROTOCOL_ERR);
    }
  }

  processMsg(msg: MsgArg, data: Uint8Array) {
    if (!this.subscriptions.sidCounter) {
      return;
    }

    let sub = this.subscriptions.get(msg.sid) as SubscriptionImpl;
    if (!sub) {
      return;
    }
    sub.received += 1;

    if (sub.callback) {
      sub.callback(null, new MsgImpl(msg, data, this));
    }

    if (sub.max !== undefined && sub.received >= sub.max) {
      sub.unsubscribe();
    }
  }

  async processError(m: Uint8Array) {
    const s = new TextDecoder().decode(m);
    const err = ProtocolHandler.toError(s);
    this.subscriptions.handleError(err);
    await this._close(err);
  }

  processPing() {
    this.transport.send(buildMessage(`PONG${CR_LF}`));
  }

  processPong() {
    this.pout = 0;
    const cb = this.pongs.shift();
    if (cb) {
      cb.resolve();
    }
  }

  processInfo(m: Uint8Array) {
    this.info = JSON.parse(new TextDecoder().decode(m));
    const updates = this.servers.update(this.info);
    if (!this.infoReceived) {
      // send connect
      const { version, lang } = this.transport;
      try {
        const c = new Connect(
          { version, lang },
          this.options,
          this.info.nonce,
        );

        const cs = JSON.stringify(c);
        this.transport.send(
          buildMessage(`CONNECT ${cs}${CR_LF}`),
        );
        this.transport.send(
          buildMessage(`PING${CR_LF}`),
        );
      } catch (err) {
        this._close(
          NatsError.errorForCode(ErrorCode.BAD_AUTHENTICATION, err),
        );
      }
    }
    if (updates) {
      this.dispatchStatus({ type: Events.UPDATE, data: updates });
    }
    const ldm = this.info.ldm !== undefined ? this.info.ldm : false;
    if (ldm) {
      this.dispatchStatus(
        {
          type: Events.LDM,
          data: this.servers.getCurrentServer().toString(),
        },
      );
    }
  }

  push(e: ParserEvent): void {
    switch (e.kind) {
      case Kind.MSG:
        const { msg, data } = e;
        this.processMsg(msg!, data!);
        break;
      case Kind.OK:
        break;
      case Kind.ERR:
        this.processError(e.data!);
        break;
      case Kind.PING:
        this.processPing();
        break;
      case Kind.PONG:
        this.processPong();
        break;
      case Kind.INFO:
        this.processInfo(e.data!);
        break;
    }
  }

  sendCommand(cmd: string | Uint8Array) {
    let buf: Uint8Array;
    if (typeof cmd === "string") {
      buf = new TextEncoder().encode(cmd);
    } else {
      buf = cmd as Uint8Array;
    }
    this.outbound.fill(buf);

    if (this.outbound.length() === 1) {
      setTimeout(() => {
        this.flushPending();
      });
    } else if (this.outbound.size() >= FLUSH_THRESHOLD) {
      this.flushPending();
    }
  }

  publish(
    subject: string,
    data: Uint8Array,
    options?: { reply?: string; headers?: MsgHdrs },
  ) {
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
    }
    if (this.noMorePublishing) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING);
    }

    let len = data.length;
    options = options || {};
    options.reply = options.reply || "";

    let hlen = 0;
    if (options.headers) {
      if (!this.options.headers) {
        throw new NatsError("headers", ErrorCode.SERVER_OPTION_NA);
      }
      const hdrs = options.headers as MsgHdrsImpl;
      const h = hdrs.encode();
      data = DataBuffer.concat(h, data);
      len = data.length;
      hlen = h.length;
    }

    let proto: string;
    if (options.headers) {
      if (options.reply) {
        proto = `HPUB ${subject} ${options.reply} ${hlen} ${len}\r\n`;
      } else {
        proto = `HPUB ${subject} ${hlen} ${len}\r\n`;
      }
    } else {
      if (options.reply) {
        proto = `PUB ${subject} ${options.reply} ${len}\r\n`;
      } else {
        proto = `PUB ${subject} ${len}\r\n`;
      }
    }

    this.sendCommand(buildMessage(proto, data));
  }

  request(r: Request): Request {
    this.initMux();
    this.muxSubscriptions.add(r);
    return r;
  }

  subscribe(s: SubscriptionImpl): Subscription {
    this.subscriptions.add(s);
    if (s.queue) {
      this.sendCommand(`SUB ${s.subject} ${s.queue} ${s.sid}\r\n`);
    } else {
      this.sendCommand(`SUB ${s.subject} ${s.sid}\r\n`);
    }
    if (s.max) {
      this.unsubscribe(s, s.max);
    }
    return s;
  }

  unsubscribe(s: SubscriptionImpl, max?: number) {
    this.unsub(s, max);
    if (s.max === undefined || s.received >= s.max) {
      this.subscriptions.cancel(s);
    }
  }

  unsub(s: SubscriptionImpl, max?: number) {
    if (!s || this.isClosed()) {
      return;
    }
    if (max) {
      this.sendCommand(`UNSUB ${s.sid} ${max}\r\n`);
    } else {
      this.sendCommand(`UNSUB ${s.sid}\r\n`);
    }
    s.max = max;
  }

  flush(p?: Deferred<void>): Promise<void> {
    if (!p) {
      p = deferred<void>();
    }
    this.pongs.push(p);
    this.sendCommand(`PING${CR_LF}`);
    return p;
  }

  sendSubscriptions() {
    let cmds: string[] = [];
    this.subscriptions.all().forEach((s) => {
      const sub = s as SubscriptionImpl;
      if (sub.queue) {
        cmds.push(`SUB ${sub.subject} ${sub.queue} ${sub.sid} ${CR_LF}`);
      } else {
        cmds.push(`SUB ${sub.subject} ${sub.sid} ${CR_LF}`);
      }
    });
    if (cmds.length) {
      this.transport.send(buildMessage(cmds.join("")));
    }
  }

  private _close(err?: Error): Promise<void> {
    if (this.parser.closed()) {
      return Promise.resolve();
    }
    this.heartbeats.cancel();
    if (this.connectError) {
      this.connectError(err);
      this.connectError = undefined;
    }
    this.muxSubscriptions.close();
    this.subscriptions.close();
    this.listeners.forEach((l) => {
      l.stop();
    });
    this.parser.close();
    return this.transport.close(err)
      .then(() => {
        return this.closed.resolve(err);
      });
  }

  close(): Promise<void> {
    return this._close();
  }

  isClosed(): boolean {
    return this.parser.closed();
  }

  drain(): Promise<void> {
    let subs = this.subscriptions.all();
    let promises: Promise<void>[] = [];
    subs.forEach((sub: Subscription) => {
      promises.push(sub.drain());
    });
    return Promise.all(promises)
      .then(async () => {
        this.noMorePublishing = true;
        return this.close();
      })
      .catch(() => {
        // cannot happen
      });
  }

  private flushPending() {
    if (!this.infoReceived || !this.connected) {
      return;
    }

    if (this.outbound.size()) {
      let d = this.outbound.drain();
      this.transport.send(d);
    }
  }

  private initMux(): void {
    let mux = this.subscriptions.getMux();
    if (!mux) {
      let inbox = this.muxSubscriptions.init();
      // dot is already part of mux
      const sub = new SubscriptionImpl(this, `${inbox}*`);
      sub.callback = this.muxSubscriptions.dispatcher();
      this.subscriptions.setMux(sub);
      this.subscribe(sub);
    }
  }

  private selectServer(): Server | undefined {
    let server = this.servers.selectServer();
    if (server === undefined) {
      return undefined;
    }
    // Place in client context.
    this.server = server;
    return this.server;
  }

  getServer(): Server | undefined {
    return this.server;
  }
}

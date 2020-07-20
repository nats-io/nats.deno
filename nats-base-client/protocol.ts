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
import {
  ConnectionOptions,
  Msg,
  Payload,
  Events,
  Status,
  DebugEvents,
  DEFAULT_RECONNECT_TIME_WAIT,
  Base,
  Subscription,
  SubscriptionOptions,
} from "./types.ts";
//@ts-ignore
import { Transport, newTransport } from "./transport.ts";
//@ts-ignore
import { ErrorCode, NatsError } from "./error.ts";
import {
  MSG,
  OK,
  ERR,
  PING,
  PONG,
  INFO,
  CR_LF,
  CR_LF_LEN,
  buildMessage,
  extend,
  extractProtocolMessage,
  timeout,
  deferred,
  Deferred,
  Timeout,
  //@ts-ignore
} from "./util.ts";
//@ts-ignore
import { Nuid } from "./nuid.ts";
//@ts-ignore
import { DataBuffer } from "./databuffer.ts";
import { Server, Servers } from "./servers.ts";
import { delay } from "./util.ts";
import { QueuedIterator } from "./queued_iterator.ts";

const nuid = new Nuid();

const FLUSH_THRESHOLD = 1024 * 8;

export enum ParserState {
  CLOSED = -1,
  AWAITING_CONTROL = 0,
  AWAITING_MSG_PAYLOAD = 1,
}

export function createInbox(): string {
  return `_INBOX.${nuid.next()}`;
}

export class Connect {
  auth_token?: string;
  echo?: boolean;
  jwt?: string;
  lang!: string;
  name?: string;
  pass?: string;
  pedantic: boolean = false;
  protocol: number = 1;
  user?: string;
  verbose: boolean = false;
  version!: string;

  constructor(
    transport: { version: string; lang: string },
    opts?: ConnectionOptions,
  ) {
    opts = opts || {} as ConnectionOptions;
    if (opts.token) {
      this.auth_token = opts.token;
    }
    if (opts.noEcho) {
      this.echo = false;
    }
    if (opts.userJWT) {
      if (typeof opts.userJWT === "function") {
        this.jwt = opts.userJWT();
      } else {
        this.jwt = opts.userJWT;
      }
    }
    extend(this, opts, transport);
  }
}

export interface Publisher {
  publish(subject: string, data: any, options?: { reply?: string }): void;
}

export interface RequestOptions {
  timeout: number;
}

export class Request {
  token: string;
  received: number = 0;
  deferred: Deferred<Msg> = deferred();
  timer: Timeout<Msg>;
  private mux: MuxSubscription;

  constructor(
    mux: MuxSubscription,
    opts: RequestOptions = { timeout: 1000 },
  ) {
    this.mux = mux;
    this.token = nuid.next();
    extend(this, opts);
    this.timer = timeout<Msg>(opts.timeout);
  }

  resolver(err: Error | null, msg: Msg): void {
    if (this.timer) {
      this.timer.cancel();
    }
    if (err) {
      this.deferred.reject(msg);
    } else {
      this.deferred.resolve(msg);
    }
    this.cancel();
  }

  cancel(): void {
    if (this.timer) {
      this.timer.cancel();
    }
    this.mux.cancel(this);
    this.deferred.reject(NatsError.errorForCode(ErrorCode.CANCELLED));
  }
}

export class SubscriptionImpl extends QueuedIterator<Msg>
  implements Base, Subscription {
  sid!: number;
  queue?: string;
  draining: boolean = false;
  max?: number;
  subject: string;
  drained?: Promise<void>;
  protocol: ProtocolHandler;
  timer?: Timeout<void>;

  constructor(
    protocol: ProtocolHandler,
    subject: string,
    opts: SubscriptionOptions = {},
  ) {
    super();
    extend(this, opts);
    this.protocol = protocol;
    this.subject = subject;
    if (opts.timeout) {
      this.timer = timeout<void>(opts.timeout);
      this.timer
        .then(() => {
          // timer was cancelled
          this.timer = undefined;
        })
        .catch((err) => {
          // timer fired
          this.stop(err);
        });
    }
  }

  callback(err: NatsError | null, msg: Msg) {
    this.cancelTimeout();
    err ? this.stop(err) : this.push(msg);
  }

  close(): void {
    if (!this.isClosed()) {
      this.cancelTimeout();
      this.stop();
    }
  }

  unsubscribe(max?: number): void {
    this.protocol.unsubscribe(this, max);
  }

  cancelTimeout(): void {
    if (this.timer) {
      this.timer.cancel();
      this.timer = undefined;
    }
  }

  drain(): Promise<void> {
    if (this.protocol.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
    }
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.SUB_CLOSED);
    }
    if (!this.drained) {
      this.protocol.unsub(this);
      this.drained = this.protocol.flush(deferred<void>());
      this.drained.then(() => {
        this.protocol.subscriptions.cancel(this);
      });
    }
    return this.drained;
  }

  isDraining(): boolean {
    return this.draining;
  }

  isClosed(): boolean {
    return this.done;
  }

  getSubject(): string {
    return this.subject;
  }

  getMax(): number | undefined {
    return this.max;
  }

  getID(): number {
    return this.sid;
  }
}

export class MuxSubscription {
  baseInbox!: string;
  reqs: Map<string, Request> = new Map<string, Request>();

  size(): number {
    return this.reqs.size;
  }

  init(): string {
    this.baseInbox = `${createInbox()}.`;
    return this.baseInbox;
  }

  add(r: Request) {
    if (!isNaN(r.received)) {
      r.received = 0;
    }
    this.reqs.set(r.token, r);
  }

  get(token: string): Request | undefined {
    return this.reqs.get(token);
  }

  cancel(r: Request): void {
    this.reqs.delete(r.token);
  }

  getToken(m: Msg): string | null {
    let s = m.subject || "";
    if (s.indexOf(this.baseInbox) === 0) {
      return s.substring(this.baseInbox.length);
    }
    return null;
  }

  dispatcher() {
    return (err: NatsError | null, m: Msg) => {
      let token = this.getToken(m);
      if (token) {
        let r = this.get(token);
        if (r) {
          r.resolver(err, m);
        }
      }
    };
  }

  close() {
    const err = NatsError.errorForCode(ErrorCode.TIMEOUT);
    this.reqs.forEach((req) => {
      req.resolver(err, {} as Msg);
    });
  }
}

export class Subscriptions {
  mux!: SubscriptionImpl;
  subs: Map<number, SubscriptionImpl> = new Map<number, SubscriptionImpl>();
  sidCounter: number = 0;

  size(): number {
    return this.subs.size;
  }

  add(s: SubscriptionImpl): SubscriptionImpl {
    this.sidCounter++;
    s.sid = this.sidCounter;
    this.subs.set(s.sid, s as SubscriptionImpl);
    return s;
  }

  setMux(s: SubscriptionImpl): SubscriptionImpl {
    this.mux = s;
    return s;
  }

  getMux(): SubscriptionImpl | null {
    return this.mux;
  }

  get(sid: number): (SubscriptionImpl | undefined) {
    return this.subs.get(sid);
  }

  all(): (SubscriptionImpl)[] {
    let buf = [];
    for (let s of this.subs.values()) {
      buf.push(s);
    }
    return buf;
  }

  cancel(s: SubscriptionImpl): void {
    if (s) {
      s.close();
      this.subs.delete(s.sid);
    }
  }

  handleError(err?: NatsError) {
    if (err) {
      const re = /^'Permissions Violation for Subscription to "(\S+)"'/i;
      const ma = re.exec(err.message);
      if (ma) {
        const subj = ma[1];
        this.subs.forEach((sub) => {
          if (subj == sub.subject) {
            sub.callback(err, {} as Msg);
            sub.close();
          }
        });
      }
    }
  }

  close() {
    this.subs.forEach((sub) => {
      sub.close();
    });
  }
}

class msg implements Msg {
  publisher: Publisher;
  subject!: string;
  sid!: number;
  reply?: string;
  data?: any;

  constructor(publisher: Publisher) {
    this.publisher = publisher;
  }

  // eslint-ignore-next-line @typescript-eslint/no-explicit-any
  respond(data?: any): boolean {
    if (this.reply) {
      this.publisher.publish(this.reply, data);
      return true;
    }
    return false;
  }
}

export class MsgBuffer {
  msg: Msg;
  length: number;
  buf?: Uint8Array | null;
  payload: string;
  err: NatsError | null = null;

  constructor(
    publisher: Publisher,
    chunks: RegExpExecArray,
    payload: "string" | "json" | "binary" = "string",
  ) {
    this.msg = new msg(publisher);
    this.msg.subject = chunks[1];
    this.msg.sid = parseInt(chunks[2], 10);
    this.msg.reply = chunks[4];
    this.length = parseInt(chunks[5], 10) + CR_LF_LEN;
    this.payload = payload;
  }

  fill(data: Uint8Array) {
    if (!this.buf) {
      this.buf = data;
    } else {
      this.buf = DataBuffer.concat(this.buf, data);
    }
    this.length -= data.length;

    if (this.length === 0) {
      this.msg.data = this.buf.slice(0, this.buf.length - 2);
      switch (this.payload) {
        case Payload.JSON:
          this.msg.data = new TextDecoder("utf-8").decode(this.msg.data);
          try {
            this.msg.data = JSON.parse(this.msg.data);
          } catch (err) {
            this.err = NatsError.errorForCode(ErrorCode.BAD_JSON, err);
          }
          break;
        case Payload.STRING:
          this.msg.data = new TextDecoder("utf-8").decode(this.msg.data);
          break;
        case Payload.BINARY:
          break;
      }
      this.buf = null;
    }
  }
}

export class ProtocolHandler {
  connected: boolean = false;
  inbound: DataBuffer;
  infoReceived: boolean = false;
  muxSubscriptions: MuxSubscription;
  options: ConnectionOptions;
  outbound: DataBuffer;
  payload: MsgBuffer | null = null;
  pongs: Array<Deferred<void>>;
  pout: number = 0;
  state: ParserState = ParserState.AWAITING_CONTROL;
  subscriptions: Subscriptions;
  transport!: Transport;
  noMorePublishing: boolean = false;
  connectError?: Function;
  publisher: Publisher;
  closed: Deferred<Error | void>;
  listeners: QueuedIterator<Status>[] = [];

  private servers: Servers;
  private server!: Server;

  constructor(options: ConnectionOptions, publisher: Publisher) {
    this.options = options;
    this.publisher = publisher;
    this.subscriptions = new Subscriptions();
    this.muxSubscriptions = new MuxSubscription();
    this.inbound = new DataBuffer();
    this.outbound = new DataBuffer();
    this.pongs = [];
    this.servers = new Servers(
      !options.noRandomize,
      this.options.servers,
      this.options.url,
    );
    this.closed = deferred<Error | void>();
  }

  private resetOutbound(): void {
    this.pongs.forEach((p) => {
      p.reject(NatsError.errorForCode(ErrorCode.DISCONNECT));
    });
    this.pongs.length = 0;
    this.state = ParserState.AWAITING_CONTROL;
    this.outbound = new DataBuffer();
    this.infoReceived = false;
  }

  private dispatchStatus(status: Status): void {
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
        if (this.state !== ParserState.CLOSED) {
          await this.disconnected(this.transport.closeError);
          return;
        }
      });

    return pong;
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
              this.inbound.fill(b);
              this.processInbound();
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
        this.connectError = undefined;
        if (this.connected) {
          this.sendSubscriptions();
        }
        this.connected = true;
        this.server.didConnect = true;
        this.server.reconnects = 0;
        this.infoReceived = true;
        this.flushPending();
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
          if (!this.connected) {
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

  processInbound(): void {
    let m: RegExpExecArray | null = null;
    while (this.inbound.size()) {
      switch (this.state) {
        case ParserState.CLOSED:
          return;
        case ParserState.AWAITING_CONTROL: {
          let raw = this.inbound.peek();
          let buf = extractProtocolMessage(raw);

          if ((m = MSG.exec(buf))) {
            this.payload = new MsgBuffer(
              this.publisher,
              m,
              this.options.payload,
            );
            this.state = ParserState.AWAITING_MSG_PAYLOAD;
          } else if ((m = OK.exec(buf))) {
            // ignored
          } else if ((m = ERR.exec(buf))) {
            this.processError(m[1]);
            return;
          } else if ((m = PONG.exec(buf))) {
            this.pout = 0;
            const cb = this.pongs.shift();
            if (cb) {
              cb.resolve();
            }
          } else if ((m = PING.exec(buf))) {
            this.transport.send(buildMessage(`PONG ${CR_LF}`));
          } else if ((m = INFO.exec(buf))) {
            const info = JSON.parse(m[1]);
            const updates = this.servers.update(info);
            if (!this.infoReceived) {
              // send connect
              const { version, lang } = this.transport;
              let cs = JSON.stringify(
                new Connect({ version, lang }, this.options),
              );
              this.transport.send(
                buildMessage(`CONNECT ${cs}${CR_LF}`),
              );
              this.transport.send(
                buildMessage(`PING ${CR_LF}`),
              );
            }
            if (updates) {
              this.dispatchStatus({ type: Events.UPDATE, data: updates });
            }
          } else {
            return;
          }
          break;
        }
        case ParserState.AWAITING_MSG_PAYLOAD: {
          if (!this.payload) {
            break;
          }
          // drain what we have collected
          if (this.inbound.size() < this.payload.length) {
            let d = this.inbound.drain();
            this.payload.fill(d);
            return;
          }
          // drain the number of bytes we need
          let dd = this.inbound.drain(this.payload.length);
          this.payload.fill(dd);
          try {
            this.processMsg();
          } catch (ex) {
            // ignore exception in client handling
          }
          this.state = ParserState.AWAITING_CONTROL;
          this.payload = null;
          break;
        }
      }
      if (m) {
        let psize = m[0].length;
        if (psize >= this.inbound.size()) {
          this.inbound.drain();
        } else {
          this.inbound.drain(psize);
        }
        m = null;
      }
    }
  }

  processMsg() {
    if (!this.payload || !this.subscriptions.sidCounter) {
      return;
    }

    let m = this.payload;

    let sub = this.subscriptions.get(m.msg.sid) as SubscriptionImpl;
    if (!sub) {
      return;
    }
    sub.received += 1;

    if (sub.callback) {
      sub.callback(m.err, m.msg);
    }

    if (sub.max !== undefined && sub.received >= sub.max) {
      sub.unsubscribe();
    }
  }

  sendCommand(cmd: string | Uint8Array) {
    let buf: Uint8Array;
    if (typeof cmd === "string") {
      buf = new TextEncoder().encode(cmd);
    } else {
      buf = cmd as Uint8Array;
    }
    if (cmd) {
      this.outbound.fill(buf);
    }

    if (this.outbound.length() === 1) {
      setTimeout(() => {
        this.flushPending();
      });
    } else if (this.outbound.size() >= FLUSH_THRESHOLD) {
      this.flushPending();
    }
  }

  publish(subject: string, data: Uint8Array, options?: { reply?: string }) {
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
    }
    if (this.noMorePublishing) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING);
    }
    let len = data.length;
    options = options || {};
    options.reply = options.reply || "";

    let proto: string;
    if (options.reply) {
      proto = `PUB ${subject} ${options.reply} ${len}\r\n`;
    } else {
      proto = `PUB ${subject} ${len}\r\n`;
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
      p = deferred();
    }
    this.pongs.push(p);
    this.sendCommand(`PING ${CR_LF}`);
    return p;
  }

  async processError(s: string) {
    let err = ProtocolHandler.toError(s);
    this.subscriptions.handleError(err);
    await this._close(err);
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
    if (this.state === ParserState.CLOSED) {
      return Promise.resolve();
    }
    if (this.connectError) {
      this.connectError(err);
      this.connectError = undefined;
    }
    this.muxSubscriptions.close();
    this.subscriptions.close();
    this.listeners.forEach((l) => {
      l.stop();
    });
    this.state = ParserState.CLOSED;
    return this.transport.close(err)
      .then(() => {
        return this.closed.resolve(err);
      });
  }

  close(): Promise<void> {
    return this._close();
  }

  isClosed(): boolean {
    return this.transport.isClosed;
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
    if (!this.infoReceived) {
      return;
    }

    if (this.outbound.size()) {
      let d = this.outbound.drain();
      this.transport.send(new Uint8Array(d));
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

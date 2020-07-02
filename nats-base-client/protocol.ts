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
  Sub,
  Req,
  defaultSub,
  Events,
  DebugEvents,
} from "./types.ts";
//@ts-ignore
import { Transport, newTransport, TransportEvents } from "./transport.ts";
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
  //@ts-ignore
} from "./util.ts";
//@ts-ignore
import { Nuid } from "./nuid.ts";
//@ts-ignore
import { DataBuffer } from "./databuffer.ts";
import { Server, Servers } from "./servers.ts";
import { delay } from "./util.ts";

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
  publish(subject: string, data: any, reply: string): void;
}

export interface RequestOptions {
  timeout?: number;
}

export class Request {
  token: string;
  private protocol: ProtocolHandler;

  constructor(req: Req, protocol: ProtocolHandler) {
    this.token = req.token;
    this.protocol = protocol;
  }

  cancel(): void {
    this.protocol.cancelRequest(this.token, 0);
  }
}

export class Subscription {
  sid: number;
  private protocol: ProtocolHandler;

  constructor(sub: Sub, protocol: ProtocolHandler) {
    this.sid = sub.sid;
    this.protocol = protocol;
  }

  unsubscribe(max?: number): void {
    this.protocol.unsubscribe(this.sid, max);
  }

  hasTimeout(): boolean {
    let sub = this.protocol.subscriptions.get(this.sid);
    return sub !== null && sub.timeout !== null;
  }

  cancelTimeout(): void {
    let sub = this.protocol.subscriptions.get(this.sid);
    if (sub !== null && sub.timeout !== null) {
      clearTimeout(sub.timeout);
      sub.timeout = null;
    }
  }

  setTimeout(millis: number, cb: () => void): boolean {
    let sub = this.protocol.subscriptions.get(this.sid);
    if (sub !== null) {
      if (sub.timeout) {
        clearTimeout(sub.timeout);
        sub.timeout = null;
      }
      // @ts-ignore
      sub.timeout = setTimeout(cb, millis);
      return true;
    }
    return false;
  }

  getReceived(): number {
    let sub = this.protocol.subscriptions.get(this.sid);
    if (sub) {
      return sub.received;
    }
    return 0;
  }

  drain(): Promise<void> {
    return this.protocol.drainSubscription(this.sid);
  }

  isDraining(): boolean {
    let sub = this.protocol.subscriptions.get(this.sid);
    if (sub) {
      return sub.draining;
    }
    return false;
  }

  isCancelled(): boolean {
    return this.protocol.subscriptions.get(this.sid) === null;
  }
}

export class MuxSubscription {
  baseInbox!: string;
  reqs: { [key: string]: Req } = {};
  length: number = 0;

  init(): string {
    this.baseInbox = `${createInbox()}.`;
    return this.baseInbox;
  }

  add(r: Req) {
    if (!isNaN(r.received)) {
      r.received = 0;
    }
    this.length++;
    this.reqs[r.token] = r;
  }

  get(token: string): Req | null {
    if (token in this.reqs) {
      return this.reqs[token];
    }
    return null;
  }

  cancel(r: Req): void {
    if (r && r.timeout) {
      clearTimeout(r.timeout);
      r.timeout = null;
    }
    if (r.token in this.reqs) {
      delete this.reqs[r.token];
      this.length--;
    }
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
          r.received++;
          r.callback(err, m);
          if (r.max && r.received >= r.max) {
            this.cancel(r);
          }
        }
      }
    };
  }
}

export class Subscriptions {
  mux!: Sub;
  subs: { [key: number]: Sub } = {};
  sidCounter: number = 0;
  length: number = 0;

  add(s: Sub): Sub {
    this.sidCounter++;
    this.length++;
    s.sid = this.sidCounter;
    this.subs[s.sid] = s;
    return s;
  }

  setMux(s: Sub): Sub {
    this.mux = s;
    return s;
  }

  getMux(): Sub | null {
    return this.mux;
  }

  get(sid: number): (Sub | null) {
    if (sid in this.subs) {
      return this.subs[sid];
    }
    return null;
  }

  all(): (Sub)[] {
    let buf = [];
    for (let sid in this.subs) {
      let sub = this.subs[sid];
      buf.push(sub);
    }
    return buf;
  }

  cancel(s: Sub): void {
    if (s && s.timeout) {
      clearTimeout(s.timeout);
      s.timeout = null;
    }
    if (s.sid in this.subs) {
      delete this.subs[s.sid];
      this.length--;
    }
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

  respond(data?: any): void {
    const reply = this.reply || "";
    this.publisher.publish(reply, data, "");
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

export class ProtocolHandler extends EventTarget {
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
  private servers: Servers;
  private server!: Server;

  constructor(options: ConnectionOptions, publisher: Publisher) {
    super();
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

  private prepare(): Deferred<void> {
    this.resetOutbound();

    const pong = deferred<void>();
    this.pongs.unshift(pong);

    this.connectError = undefined;

    this.connectError = (err: NatsError) => {
      pong.reject(err);
    };

    this.transport = newTransport();
    this.transport.addEventListener(
      TransportEvents.CLOSE,
      (async (evt: CustomEvent) => {
        evt.stopPropagation();
        if (this.state !== ParserState.CLOSED) {
          await this.disconnected(this.transport.closeError);
          return;
        }
      }) as EventListener,
    );

    return pong;
  }

  async disconnected(err?: Error): Promise<void> {
    this.dispatchEvent((new Event(Events.DISCONNECT)));
    if (this.options.reconnect) {
      await this.dialLoop()
        .then(() => {
          this.dispatchEvent(new Event(Events.RECONNECT));
        })
        .catch((err) => {
          this._close(err);
        });
    } else {
      await this._close();
    }
  }

  dial(srv: Server): Promise<any> {
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
      // @ts-ignore
      let wait = this.options.reconnectDelayHandler();
      let maxWait = wait;
      const srv = this.selectServer();
      if (!srv) {
        throw lastError || NatsError.errorForCode(ErrorCode.CONNECTION_REFUSED);
      }
      const now = Date.now();
      if (srv.lastConnect === 0 || srv.lastConnect + wait <= now) {
        srv.lastConnect = Date.now();
        try {
          this.dispatchEvent(
            new CustomEvent(
              DebugEvents.RECONNECTING,
              { detail: srv.hostport() },
            ),
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
              this.dispatchEvent(
                new CustomEvent(Events.UPDATE, { detail: updates }),
              );
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

    let sub = this.subscriptions.get(m.msg.sid);
    if (!sub) {
      return;
    }
    sub.received += 1;

    if (sub.timeout && sub.max === undefined) {
      // first message clears it
      clearTimeout(sub.timeout);
      sub.timeout = null;
    }

    if (sub.callback) {
      sub.callback(m.err, m.msg);
    }

    if (sub.max !== undefined && sub.received >= sub.max) {
      this.unsubscribe(sub.sid);
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

  publish(subject: string, data: Uint8Array, reply: string) {
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
    }
    if (this.noMorePublishing) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING);
    }
    let len = data.length;
    reply = reply || "";

    let proto: string;
    if (reply) {
      proto = `PUB ${subject} ${reply} ${len}\r\n`;
    } else {
      proto = `PUB ${subject} ${len}\r\n`;
    }

    this.sendCommand(buildMessage(proto, data));
  }

  request(r: Req): Request {
    this.initMux();
    this.muxSubscriptions.add(r);
    return new Request(r, this);
  }

  subscribe(s: Sub): Subscription {
    let sub = this.subscriptions.add(s) as Sub;
    if (sub.queue) {
      this.sendCommand(`SUB ${sub.subject} ${sub.queue} ${sub.sid}\r\n`);
    } else {
      this.sendCommand(`SUB ${sub.subject} ${sub.sid}\r\n`);
    }
    return new Subscription(sub, this);
  }

  unsubscribe(sid: number, max?: number) {
    if (!sid || this.isClosed()) {
      return;
    }

    let s = this.subscriptions.get(sid);
    if (s) {
      if (max) {
        this.sendCommand(`UNSUB ${sid} ${max}\r\n`);
      } else {
        this.sendCommand(`UNSUB ${sid}\r\n`);
      }
      s.max = max;
      if (s.max === undefined || s.received >= s.max) {
        this.subscriptions.cancel(s);
      }
    }
  }

  cancelRequest(token: string, max?: number): void {
    if (!token || this.isClosed()) {
      return;
    }
    let r = this.muxSubscriptions.get(token);
    if (r) {
      r.max = max;
      if (r.max === undefined || r.received >= r.max) {
        this.muxSubscriptions.cancel(r);
      }
    }
  }

  flush(p?: Deferred<any>): Promise<void> {
    if (!p) {
      p = deferred();
    }
    this.pongs.push(p);
    this.sendCommand(`PING ${CR_LF}`);
    return p;
  }

  async processError(s: string) {
    let err = ProtocolHandler.toError(s);
    await this._close(err);

    // this.dispatchEvent(new ErrorEvent(CLOSE_EVT, new ErrorEvent("error", { error: err })));
  }

  sendSubscriptions() {
    let cmds: string[] = [];
    this.subscriptions.all().forEach((s) => {
      if (s.queue) {
        cmds.push(`SUB ${s.subject} ${s.queue} ${s.sid} ${CR_LF}`);
      } else {
        cmds.push(`SUB ${s.subject} ${s.sid} ${CR_LF}`);
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
    subs.forEach((sub: Sub) => {
      let p = this.drainSubscription(sub.sid);
      promises.push(p);
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

  async drainSubscription(sid: number): Promise<void> {
    if (this.isClosed()) {
      throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
    }
    if (!sid) {
      throw NatsError.errorForCode(ErrorCode.SUB_CLOSED);
    }
    let s = this.subscriptions.get(sid);
    if (!s) {
      throw NatsError.errorForCode(ErrorCode.SUB_CLOSED);
    }
    if (s.draining) {
      throw NatsError.errorForCode(ErrorCode.SUB_DRAINING);
    }

    let sub = s;
    sub.draining = true;
    this.sendCommand(`UNSUB ${sub.sid}\r\n`);
    await this.flush();
    this.subscriptions.cancel(sub);
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
      let sub = defaultSub();
      // dot is already part of mux
      sub.subject = `${inbox}*`;
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

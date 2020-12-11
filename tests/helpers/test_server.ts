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
export class Connection {
  conn: Deno.Conn | null;
  buf = new Uint8Array(1024 * 8);
  debug: boolean;
  pending: Promise<any>[] = [];
  ca?: ConnectionAction;

  static td = new TextDecoder();
  static ping = new TextEncoder().encode("PING\r\n");
  static pong = new TextEncoder().encode("PONG\r\n");

  constructor(conn: Deno.Conn, debug: boolean = false, ca?: ConnectionAction) {
    this.conn = conn;
    this.debug = debug;
    this.ca = ca;
  }

  async read(): Promise<any> {
    if (this.conn) {
      this.conn.read(this.buf)
        .then((c) => {
          if (c === null) {
            return this.close();
          }
          if (c) {
            const s = this.buf.slice(0, c);
            if (this.debug) {
              console.info("> cs (raw)", new TextDecoder().decode(s));
            }
            this.processInbound(this.buf.slice(0, c));
          }
          setTimeout(() => {
            this.read();
          }, 0);
        })
        .catch((err) => {
          return this.close();
        });
    }
  }

  processInbound(buf: Uint8Array): void {
    const td = new TextDecoder();
    const r = td.decode(buf);
    const lines = r.split("\r\n");
    lines.forEach((line) => {
      if (line === "") {
        return;
      }
      if (/^CONNECT\s+/.test(line)) {
        this.write(Connection.ping);
      } else if (/^PING/.test(line)) {
        this.write(Connection.pong);
        if (this.ca) {
          this.ca(this);
        }
      } else if (/^SUB\s+/i.test(line)) {
      } else if (/^PUB\s+/i.test(line)) {
      } else if (/^UNSUB\s+/i.test(line)) {
      } else if (/^MSG\s+/i.test(line)) {
      } else if (/^INFO\s+/i.test(line)) {
      }
    });
  }

  async write(buf: Uint8Array): Promise<any> {
    try {
      if (this.conn) {
        if (this.debug) {
          console.log("< cs", new TextDecoder().decode(buf));
        }
        const p = this.conn.write(buf);
        p.finally(() => {
          this.pending.shift();
        });
        this.pending.push(p);
      }
    } catch (err) {
      console.trace("error writing", err);
    }
  }

  close(): Promise<any> {
    if (!this.conn) {
      return Promise.resolve();
    }
    const conn = this.conn;
    this.conn = null;
    return Promise.all(this.pending)
      .finally(() => {
        conn.close();
      });
  }
}

export interface ConnectionAction {
  (c: Connection): void;
}

export class TestServer {
  listener?: Deno.Listener;
  port: number;
  info: Uint8Array;
  debug: boolean;
  clients: Connection[] = [];
  accept: Promise<any>;

  constructor(debug: boolean = false, ca?: ConnectionAction) {
    const listener = Deno.listen({ port: 0, transport: "tcp" });
    //@ts-ignore
    const { port } = listener.addr;
    this.port = port;
    this.debug = debug;
    this.listener = listener;
    this.info = new TextEncoder().encode(
      "INFO " + JSON.stringify({
        server_id: "TEST",
        version: "0.0.0",
        host: "127.0.0.1",
        port: port,
        auth_required: false,
      }) + "\r\n",
    );

    const self = this;
    this.accept = new Promise<void>(async (resolve) => {
      for await (const socket of listener) {
        try {
          const c = new Connection(socket, debug, ca);
          self.clients.push(c);
          c.write(self.info);
          setTimeout(() => {
            c.read();
          });
        } catch (err) {
          resolve();
          return;
        }
      }
    });
  }

  getPort(): number {
    return this.port;
  }

  async stop() {
    if (this.listener) {
      this.listener.close();
      const promises: Promise<any>[] = [];
      promises.push(this.accept);
      this.clients.forEach((c) => {
        promises.push(c.close());
      });
      Promise.all(promises)
        .finally(() => {
          this.listener?.close();
          this.listener = undefined;
        });
    } else {
      return Promise.resolve();
    }
  }
}

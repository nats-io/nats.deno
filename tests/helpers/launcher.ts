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
import * as path from "https://deno.land/std@0.80.0/path/mod.ts";
import { check } from "./mod.ts";
import {
  Deferred,
  deferred,
  delay,
  nuid,
  timeout,
} from "../../nats-base-client/internal_mod.ts";
import { assert } from "https://deno.land/std@0.80.0/testing/asserts.ts";

export const ServerSignals = Object.freeze({
  QUIT: Deno.Signal.SIGQUIT,
  STOP: Deno.Signal.SIGSTOP,
  REOPEN: Deno.Signal.SIGUSR1,
  RELOAD: Deno.Signal.SIGHUP,
  LDM: Deno.Signal.SIGUSR2,
});

export interface PortInfo {
  clusterName?: string;
  hostname: string;
  port: number;
  cluster?: number;
  monitoring?: number;
  websocket?: number;
}

export interface Ports {
  nats: string[];
  cluster?: string[];
  monitoring?: string[];
  websocket?: string[];
}

function parseHostport(s?: string) {
  if (!s) {
    return;
  }
  const idx = s.indexOf("://");
  if (idx) {
    s = s.slice(idx + 3);
  }
  const [hostname, ps] = s.split(":");
  const port = parseInt(ps, 10);

  return { hostname, port };
}

function parsePorts(ports: Ports): PortInfo {
  ports.monitoring = ports.monitoring || [];
  ports.cluster = ports.cluster || [];
  ports.websocket = ports.websocket || [];
  const listen = parseHostport(ports.nats[0]);
  const p: PortInfo = {} as PortInfo;

  if (listen) {
    p.hostname = listen.hostname;
    p.port = listen.port;
  }

  const cluster = ports.cluster.map((v) => {
    if (v) {
      return parseHostport(v)?.port;
    }
    return undefined;
  });
  p.cluster = cluster[0];

  const monitoring = ports.monitoring.map((v) => {
    if (v) {
      return parseHostport(v)?.port;
    }
    return undefined;
  });
  p.monitoring = monitoring[0];

  const websocket = ports.websocket.map((v) => {
    if (v) {
      return parseHostport(v)?.port;
    }
    return undefined;
  });
  p.websocket = websocket[0];

  return p;
}

export class NatsServer implements PortInfo {
  hostname: string;
  clusterName?: string;
  port: number;
  cluster?: number;
  monitoring?: number;
  websocket?: number;
  process: Deno.Process;
  logBuffer: string[] = [];
  stopped: boolean = false;
  done!: Deferred<void>;
  debug: boolean;
  config: any;

  constructor(opts: {
    info: PortInfo;
    process: Deno.Process;
    debug?: boolean;
    config: any;
  }) {
    const { info, process, debug, config } = opts;
    this.hostname = info.hostname;
    this.port = info.port;
    this.cluster = info.cluster;
    this.monitoring = info.monitoring;
    this.websocket = info.websocket;
    this.clusterName = info.clusterName;
    this.process = process;
    this.debug = debug || false;
    this.done = deferred<void>();
    this.config = config;

    (async () => {
      assert(process.stderr != null);
      const td = new TextDecoder();
      const buf = new Uint8Array(1024 * 8);
      while (true) {
        try {
          const c = await process.stderr.read(buf);
          if (c === null) {
            break;
          }
          if (c) {
            const t = td.decode(buf.slice(0, c));
            this.logBuffer.push(t);
            if (debug) {
              console.log(t);
            }
          }
        } catch (err) {
          break;
        }
      }
      this.done.resolve();
    })();
  }

  restart(): Promise<NatsServer> {
    const conf = JSON.parse(JSON.stringify(this.config));
    conf.port = this.port;
    return NatsServer.start(conf, this.debug);
  }

  getLog(): string {
    return this.logBuffer.join("");
  }

  static stopAll(cluster: NatsServer[]): Promise<void[]> {
    const buf: Promise<void>[] = [];
    cluster.forEach((s) => {
      buf.push(s.stop());
    });

    return Promise.all(buf);
  }

  async stop(): Promise<void> {
    if (!this.stopped) {
      this.stopped = true;
      this.process.stderr?.close();
      this.process.kill(Deno.Signal.SIGKILL);
      this.process.close();
    }
    await this.done;
  }

  signal(signal: Deno.MacOSSignal | Deno.LinuxSignal): Promise<void> {
    if (signal === Deno.Signal.SIGKILL) {
      return this.stop();
    } else {
      this.process.kill(signal);
      return Promise.resolve();
    }
  }

  async varz(): Promise<any> {
    if (!this.monitoring) {
      return Promise.reject(new Error("server is not monitoring"));
    }
    const resp = await fetch(`http://127.0.0.1:${this.monitoring}/varz`);
    return await resp.json();
  }

  static async cluster(
    count: number = 2,
    conf?: any,
    debug: boolean = false,
  ): Promise<NatsServer[]> {
    conf = conf || {};
    conf = Object.assign({}, conf);
    conf.cluster = conf.cluster || {};
    conf.cluster.name = nuid.next();
    conf.cluster.listen = conf.cluster.listen || "127.0.0.1:-1";

    const ns = await NatsServer.start(conf, debug);
    const cluster = [ns];

    for (let i = 1; i < count; i++) {
      const s = await NatsServer.addClusterMember(ns, conf, debug);
      cluster.push(s);
    }

    return cluster;
  }

  static async localClusterFormed(servers: NatsServer[]): Promise<void[]> {
    const ports = servers.map((s) => s.port);

    const fn = async function (s: NatsServer) {
      const dp = deferred<void>();
      const to = timeout<void>(5000);
      let done = false;
      to.catch((err) => {
        done = true;
        dp.reject(
          new Error(
            `${s.hostname}:${s.port} failed to resolve peers: ${err.toString}`,
          ),
        );
      });

      while (!done) {
        const data = await s.varz();
        if (data) {
          const urls = data.connect_urls as string[];
          const others = urls.map((s) => {
            return parseHostport(s)?.port;
          });

          if (others.every((v) => ports.includes(v!))) {
            dp.resolve();
            to.cancel();
            break;
          }
        }
        await delay(100);
      }
      return dp;
    };
    const proms = servers.map((s) => fn(s));
    return Promise.all(proms);
  }

  static async addClusterMember(
    ns: NatsServer,
    conf?: any,
    debug: boolean = false,
  ): Promise<NatsServer> {
    if (ns.cluster === undefined) {
      return Promise.reject(new Error("no cluster port on server"));
    }
    conf = conf || {};
    conf = Object.assign({}, conf);
    conf.port = -1;
    conf.cluster = conf.cluster || {};
    conf.cluster.name = ns.clusterName;
    conf.cluster.listen = conf.cluster.listen || "127.0.0.1:-1";
    conf.cluster.routes = [`nats://${ns.hostname}:${ns.cluster}`];
    return NatsServer.start(conf, debug);
  }

  static async start(conf?: any, debug: boolean = false): Promise<NatsServer> {
    const exe = Deno.env.get("CI") ? "nats-server/nats-server" : "nats-server";
    const tmp = path.resolve(Deno.env.get("TMPDIR") || ".");

    let srv: Deno.Process;
    return new Promise(async (resolve, reject) => {
      try {
        conf = conf || {};
        conf.ports_file_dir = tmp;
        conf.host = conf.host || "127.0.0.1";
        conf.port = conf.port || -1;
        conf.http = conf.http || "127.0.0.1:-1";

        const confFile = await Deno.makeTempFileSync();
        await Deno.writeFile(confFile, new TextEncoder().encode(toConf(conf)));
        if (debug) {
          console.info(`${exe} -c ${confFile}`);
        }
        srv = await Deno.run(
          {
            cmd: [exe, "-c", confFile],
            stderr: "piped",
            stdout: "null",
            stdin: "null",
          },
        );

        if (debug) {
          console.info(`[${srv.pid}] - launched`);
        }

        const portsFile = path.resolve(
          path.join(tmp, `nats-server_${srv.pid}.ports`),
        );

        const pi = await check(
          () => {
            try {
              const data = Deno.readFileSync(portsFile);
              const txt = new TextDecoder().decode(data);
              const d = JSON.parse(txt);
              if (d) {
                return d;
              }
            } catch (_) {
            }
          },
          1000,
          { name: "read ports file" },
        );

        if (debug) {
          console.info(`[${srv.pid}] - ports file found`);
        }

        const ports = parsePorts(pi as Ports);
        if (conf.cluster?.name) {
          ports.clusterName = conf.cluster.name;
        }
        await check(
          async () => {
            try {
              if (debug) {
                console.info(`[${srv.pid}] - attempting to connect`);
              }
              const conn = await Deno.connect(ports as Deno.ConnectOptions);
              conn.close();
              return ports.port;
            } catch (_) {
              // ignore
            }
          },
          5000,
          { name: "wait for server" },
        );
        resolve(
          new NatsServer(
            { info: ports, process: srv, debug: debug, config: conf },
          ),
        );
      } catch (err) {
        if (srv) {
          try {
            const d = await srv.stderrOutput();
            console.error(new TextDecoder().decode(d));
          } catch (err) {
            console.error("unable to read server output:", err);
          }
        }
        reject(err);
      }
    });
  }
}

export function toConf(o: object, indent?: string): string {
  let pad = indent !== undefined ? indent + "  " : "";
  let buf = [];
  for (let k in o) {
    if (o.hasOwnProperty(k)) {
      //@ts-ignore
      let v = o[k];
      if (Array.isArray(v)) {
        buf.push(`${pad}${k} [`);
        buf.push(toConf(v, pad));
        buf.push(`${pad} ]`);
      } else if (typeof v === "object") {
        // don't print a key if it is an array and it is an index
        let kn = Array.isArray(o) ? "" : k;
        buf.push(`${pad}${kn} {`);
        buf.push(toConf(v, pad));
        buf.push(`${pad} }`);
      } else {
        if (!Array.isArray(o)) {
          if (
            typeof v === "string" && v.charAt(0) >= "0" && v.charAt(0) <= "9"
          ) {
            buf.push(`${pad}${k}: "${v}"`);
          } else {
            buf.push(`${pad}${k}: ${v}`);
          }
        } else {
          buf.push(pad + v);
        }
      }
    }
  }
  return buf.join("\n");
}

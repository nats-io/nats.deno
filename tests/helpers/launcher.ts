/*
 * Copyright 2020-2021 The NATS Authors
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
// deno-lint-ignore-file no-explicit-any
import * as path from "https://deno.land/std@0.152.0/path/mod.ts";
import { rgb24 } from "https://deno.land/std@0.152.0/fmt/colors.ts";
import { check } from "./mod.ts";
import {
  Deferred,
  deferred,
  delay,
  extend,
  nuid,
  timeout,
} from "../../nats-base-client/internal_mod.ts";
import { assert } from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { jsopts } from "../jstest_util.ts";

export const ServerSignals = Object.freeze({
  QUIT: "SIGQUIT",
  STOP: "SIGSTOP",
  REOPEN: "SIGUSR1",
  RELOAD: "SIGHUP",
  LDM: "SIGUSR2",
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

export interface VarZ {
  "connect_urls": string[];
}

export interface JSZ {
  "server_id": string;
  now: string;
  config: {
    "max_memory": number;
    "max_storage": number;
    "store_dir": string;
  };
  memory: number;
  storage: number;
  api: { total: number; errors: number };
  "current_api_calls": number;
  "meta_cluster": {
    name: string;
    leader: string;
    replicas: [{ name: string; current: boolean; active: number }];
  };
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
  stopped = false;
  done!: Deferred<void>;
  debug: boolean;
  config: any;
  configFile: string;
  rgb: { r: number; g: number; b: number };

  constructor(opts: {
    info: PortInfo;
    process: Deno.Process;
    debug?: boolean;
    config: any;
    configFile: string;
  }) {
    const { info, process, debug, config, configFile } = opts;
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
    this.configFile = configFile;

    const r = Math.floor(Math.random() * 255);
    const g = Math.floor(Math.random() * 255);
    const b = Math.floor(Math.random() * 255);
    this.rgb = { r, g, b };

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
              console.log(rgb24(t.slice(0, t.length - 1), this!.rgb));
            }
          }
        } catch (_err) {
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
      this.process.kill("SIGKILL");
      this.process.close();
    }
    await this.done;
  }

  signal(signal: string): Promise<void> {
    if (signal === "SIGKILL") {
      return this.stop();
    } else {
      //@ts-ignore: this is correct
      this.process.kill(signal);
      return Promise.resolve();
    }
  }

  async varz(): Promise<VarZ> {
    if (!this.monitoring) {
      return Promise.reject(new Error("server is not monitoring"));
    }
    const resp = await fetch(`http://127.0.0.1:${this.monitoring}/varz`);
    return await resp.json();
  }

  async jsz(): Promise<JSZ> {
    if (!this.monitoring) {
      return Promise.reject(new Error("server is not monitoring"));
    }
    const resp = await fetch(`http://127.0.0.1:${this.monitoring}/jsz`);
    return await resp.json();
  }

  async dataDir(): Promise<string | null> {
    const jsz = await this.jsz();
    return jsz.config.store_dir;
  }

  static async jetstreamCluster(
    count = 2,
    serverConf?: Record<string, unknown>,
    debug = false,
  ): Promise<NatsServer[]> {
    serverConf = serverConf || {};
    // form a cluster with the specified count
    const servers = await NatsServer.cluster(count, serverConf, false);

    // extract all the configs
    const configs = servers.map((s) => {
      const { port, cluster, monitoring, websocket, config } = s;
      return { port, cluster, monitoring, websocket, config };
    });

    // stop all the servers and wait
    const proms = servers.map((s) => {
      return s.stop();
    });
    await Promise.all(proms);

    servers.forEach((s) => {
      s.debug = debug;
    });

    const routes: string[] = [];
    configs.forEach((conf, idx, arr) => {
      let { port, cluster, monitoring, websocket, config } = conf;

      // jetstream defaults
      const { jetstream } = jsopts();
      // need a server name for a cluster
      const serverName = nuid.next();
      // customize the store dir and make it
      jetstream.store_dir = path.join("/tmp", "jetstream", serverName);
      Deno.mkdirSync(jetstream.store_dir, { recursive: true });

      config = extend(
        config,
        { jetstream },
        { port, server_name: serverName },
        serverConf || {},
      );

      // set the specific ports that we ran on before
      config.cluster.listen = config.cluster.listen.replace("-1", `${cluster}`);
      routes.push(`nats://${config.cluster.listen}`);
      if (conf.monitoring) {
        config.http = config.http.replace("-1", `${monitoring}`);
      }
      if (websocket) {
        config.websocket = config.websocket || {};
        config.websocket.port = websocket;
      }

      // replace it
      arr[idx] = { port, cluster, monitoring, websocket, config };
    });

    // update the routes to be explicit
    configs.forEach((c) => {
      c.config.cluster.routes = routes.filter((v) => {
        return v.indexOf(c.config.cluster.listen) === -1;
      });
    });

    // reconfigure the servers
    servers.forEach((s, idx) => {
      s.config = configs[idx].config;
    });

    const buf: Promise<NatsServer>[] = [];
    servers.map((s) => {
      buf.push(s.restart());
    });

    return NatsServer.dataClusterFormed(buf, debug);
  }

  static async dataClusterFormed(
    proms: Promise<NatsServer>[],
    debug = false,
  ): Promise<NatsServer[]> {
    const errs = 0;
    let servers: NatsServer[] = [];
    const statusProms: Promise<JSZ>[] = [];
    const leaders: string[] = [];
    while (true) {
      try {
        leaders.length = 0;
        statusProms.length = 0;
        // await for all the servers to resolve and get /jsz
        servers = await Promise.all(proms);
        servers.forEach((s) => {
          statusProms.push(s.jsz());
        });

        // await for all the jsz to resolve
        const status = await Promise.all(statusProms);
        status.forEach((i) => {
          const leader = i.meta_cluster.leader;
          if (leader) {
            leaders.push(leader);
          }
        });
        // if we resolved leaders on all
        if (leaders.length === proms.length) {
          // unique them
          const u = leaders.filter((v, idx, a) => {
            return a.indexOf(v) === idx;
          });
          // if we have one, we fine
          if (u.length === 1) {
            const leader = servers.filter((s) => {
              return s.config.server_name === u[0];
            });
            const n = rgb24(`${u[0]}`, leader[0].rgb);
            if (debug) {
              console.log(`leader consensus ${n}`);
            }
            return servers;
          } else {
            if (debug) {
              console.log(
                `leader contention ${leaders.length}`,
              );
            }
          }
        } else {
          if (debug) {
            console.log(
              `found ${leaders.length}/${servers.length} leaders`,
            );
          }
        }
      } catch (err) {
        err++;
        if (errs > 10) {
          throw err;
        }
      }
      await delay(250);
    }
  }

  static async cluster(
    count = 2,
    conf?: any,
    debug = false,
  ): Promise<NatsServer[]> {
    conf = conf || {};
    conf = Object.assign({}, conf);
    conf.cluster = conf.cluster || {};
    conf.cluster.name = nuid.next();
    conf.cluster.listen = conf.cluster.listen || "127.0.0.1:-1";

    const ns = await NatsServer.start(conf, debug);
    const cluster = [ns];

    for (let i = 1; i < count; i++) {
      const c = Object.assign({}, conf);
      const s = await NatsServer.addClusterMember(ns, c, debug);
      cluster.push(s);
    }
    return cluster;
  }

  static localClusterFormed(servers: NatsServer[]): Promise<void[]> {
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
        await timeout(100);
      }
      return dp;
    };
    const proms = servers.map((s) => fn(s));
    return Promise.all(proms);
  }

  static addClusterMember(
    ns: NatsServer,
    conf?: any,
    debug = false,
  ): Promise<NatsServer> {
    if (ns.cluster === undefined) {
      return Promise.reject(new Error("no cluster port on server"));
    }
    conf = conf || {};
    conf = JSON.parse(JSON.stringify(conf));
    conf.port = -1;
    conf.cluster = conf.cluster || {};
    conf.cluster.name = ns.clusterName;
    conf.cluster.listen = conf.cluster.listen || "127.0.0.1:-1";
    conf.cluster.routes = [`nats://${ns.hostname}:${ns.cluster}`];
    return NatsServer.start(conf, debug);
  }

  static async start(conf?: any, debug = false): Promise<NatsServer> {
    const exe = Deno.env.get("CI") ? "nats-server/nats-server" : "nats-server";
    const tmp = path.resolve(Deno.env.get("TMPDIR") || ".");

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
    const srv = await Deno.run(
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
          // ignore
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
    try {
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

      return new NatsServer(
        {
          info: ports,
          process: srv,
          debug: debug,
          config: conf,
          configFile: confFile,
        },
      );
    } catch (err) {
      console.error(`failed to start config: ${confFile}`);
      try {
        const d = await srv.stderrOutput();
        console.error(new TextDecoder().decode(d));
      } catch (err) {
        console.error("unable to read server output:", err);
      }
      throw err;
    }
  }
}

// @ts-ignore: any is exactly what we need here
export function toConf(o: any, indent?: string): string {
  const pad = indent !== undefined ? indent + "  " : "";
  const buf = [];
  for (const k in o) {
    if (Object.prototype.hasOwnProperty.call(o, k)) {
      //@ts-ignore: tsc,
      const v = o[k];
      if (Array.isArray(v)) {
        buf.push(`${pad}${k} [`);
        buf.push(toConf(v, pad));
        buf.push(`${pad} ]`);
      } else if (typeof v === "object") {
        // don't print a key if it is an array and it is an index
        const kn = Array.isArray(o) ? "" : k;
        buf.push(`${pad}${kn} {`);
        buf.push(toConf(v, pad));
        buf.push(`${pad} }`);
      } else {
        if (!Array.isArray(o)) {
          if (
            typeof v === "string" && v.startsWith("$")
          ) {
            buf.push(`${pad}${k}: "${v}"`);
          } else if (
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

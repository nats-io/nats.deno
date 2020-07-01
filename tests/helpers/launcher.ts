import * as path from "https://deno.land/std/path/mod.ts";
import { check } from "./mod.ts";

export interface PortInfo {
  hostname: string;
  port: number;
  cluster?: number;
}

export interface Ports {
  nats: string[];
  cluster?: string;
}

function parseHostport(s?: string) {
  if (!s) {
    return;
  }
  s = s.toString().replace("nats://", "");
  const [hostname, ps] = s.split(":");
  const port = parseInt(ps, 10);

  return { hostname, port };
}

function parsePorts(ports: Ports): PortInfo {
  const listen = parseHostport(ports.nats[0]);
  const p: PortInfo = {} as PortInfo;
  if (listen) {
    p.hostname = listen.hostname;
    p.port = listen.port;
  }

  const cluster = parseHostport(ports.cluster);
  if (cluster) {
    p.cluster = cluster.port;
  }
  return p;
}

export class NatsServer implements PortInfo {
  hostname: string;
  port: number;
  cluster?: number;
  process: Deno.Process;
  srvLog!: Uint8Array;
  err?: Promise<void>;
  debug: boolean;
  stopped: boolean = false;

  constructor(
    info: PortInfo,
    process: Deno.Process,
    debug: boolean,
  ) {
    this.hostname = info.hostname;
    this.port = info.port;
    this.cluster = info.cluster;
    this.process = process;
    this.debug = debug;

    //@ts-ignore
    this.err = this.drain(process.stderr as Deno.Reader);
  }

  async drain(r: Deno.Reader): Promise<void> {
    const buf = new Uint8Array(1024 * 8);
    while (true) {
      try {
        let c = await r.read(buf);
        if (c === null) {
          break;
        }
        if (c && this.debug) {
          console.log(new TextDecoder().decode(buf.slice(0, c)));
        }
      } catch (err) {
        break;
      }
    }
    return Promise.resolve();
  }

  log() {
    console.log(new TextDecoder().decode(this.srvLog));
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
      this.process.kill(Deno.Signal.SIGKILL);
      this.process.close();
      if (this.err) {
        await this.err;
      }
    }
  }

  static async cluster(
    count: number = 2,
    conf?: any,
    debug: boolean = false,
  ): Promise<NatsServer[]> {
    conf = conf || {};
    conf = Object.assign({}, conf);
    conf.cluster = conf.cluster || {};
    conf.cluster.listen = conf.cluster.listen || "127.0.0.1:-1";

    const ns = await NatsServer.start(conf, debug);
    const cluster = [ns];

    for (let i = 1; i < count; i++) {
      const s = await NatsServer.addClusterMember(ns, conf, debug);
      cluster.push(s);
    }

    return cluster;
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
    conf.cluster.listen = conf.cluster.listen || "127.0.0.1:-1";
    conf.cluster.routes = [`nats://${ns.hostname}:${ns.cluster}`];
    return NatsServer.start(conf, debug);
  }

  static async start(conf?: any, debug: boolean = false): Promise<NatsServer> {
    const exe = Deno.env.get("CI") ? "nats-server/nats-server" : "nats-server";
    const tmp = Deno.env.get("TMPDIR") || ".";

    let srv: Deno.Process;
    return new Promise(async (resolve, reject) => {
      try {
        conf = conf || {};
        conf.ports_file_dir = tmp;
        conf.host = conf.host || "127.0.0.1";
        conf.port = conf.port || -1;

        const confFile = await Deno.makeTempFileSync();
        await Deno.writeFile(confFile, new TextEncoder().encode(toConf(conf)));
        if (debug) {
          console.info(`${exe} -c ${confFile}`);
        }
        srv = await Deno.run(
          {
            cmd: [exe, "-c", confFile],
            stderr: debug ? "piped" : "null",
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
              const d = JSON.parse(new TextDecoder().decode(data));
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
        resolve(new NatsServer(ports, srv, debug));
      } catch (err) {
        if (srv) {
          try {
            const d = await srv.stderrOutput();
            console.error(new TextDecoder().decode(d));
          } catch (err) {
            console.error("unable to read server output");
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
        buf.push(pad + k + " [");
        buf.push(toConf(v, pad));
        buf.push(pad + " ]");
      } else if (typeof v === "object") {
        // don't print a key if it is an array and it is an index
        let kn = Array.isArray(o) ? "" : k;
        buf.push(pad + kn + " {");
        buf.push(toConf(v, pad));
        buf.push(pad + " }");
      } else {
        if (!Array.isArray(o)) {
          if (
            //@ts-ignore
            typeof v === "string" && v.charAt(0) >= "0" && v.charAt(0) <= "9"
          ) {
            buf.push(pad + k + ': \"' + v + '\"');
          } else {
            buf.push(pad + k + ": " + v);
          }
        } else {
          buf.push(pad + v);
        }
      }
    }
  }
  return buf.join("\n");
}

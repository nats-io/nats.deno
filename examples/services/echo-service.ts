/*
 * Copyright 2023 The NATS Authors
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

import { cli, Command, Flags } from "https://deno.land/x/cobra@v0.0.9/mod.ts";
import {
  connect,
  headers,
  millis,
  NatsConnection,
  StringCodec,
} from "../../src/mod.ts";
import { collect } from "../../nats-base-client/util.ts";

const root = cli({
  use: "echo-service [--server server:port] (service | monitor | request)",
});

root.addFlag({
  name: "server",
  type: "string",
  usage: "NATS server host:port",
  default: "demo.nats.io",
  persistent: true,
});

const adm = root.addCommand({
  use: "monitor",
  short: "monitor [ping|info|status] [--name n] [--id id]",
});

adm.addFlag({
  short: "n",
  name: "name",
  type: "string",
  usage: "service name to filter on",
  default: "",
  persistent: true,
});
adm.addFlag({
  name: "id",
  type: "string",
  usage: "service id to filter on",
  default: "",
  persistent: true,
});

adm.addCommand({
  use: "ping [--name] [--id]",
  short: "pings services",
  run: async (
    cmd: Command,
    _args: string[],
    flags: Flags,
  ): Promise<number> => {
    const nc = await createConnection(flags);
    const opts = options(flags);
    const mc = nc.services.client();
    const infos = (await collect(await mc.ping(opts.name, opts.id))).sort(
      (a, b) => {
        const A = `${a.name} ${a.version}`;
        const B = `${b.name} ${b.version}`;
        return B.localeCompare(A);
      },
    );
    if (infos.length) {
      console.table(infos);
    } else {
      cmd.stdout("no services found");
    }
    await nc.close();
    return 0;
  },
});

adm.addCommand({
  use: "stats [--name] [--id]",
  short: "get service stats",
  run: async (
    cmd: Command,
    _args: string[],
    flags: Flags,
  ): Promise<number> => {
    const nc = await createConnection(flags);
    const mc = nc.services.client();
    const opts = options(flags);
    // stats are grouped by endpoint, if this is a multi-endpoint map this so that each
    // endpoint looks like a service
    const stats: { num_requests: number }[] = [];
    (await collect(await mc.stats(opts.name, opts.id))).forEach(
      (s) => {
        const { name, id, version } = s;
        s.endpoints?.forEach((ne) => {
          const line = Object.assign(ne, {
            name,
            endpoint: ne.name,
            id,
            version,
          });
          line.processing_time = millis(line.processing_time);
          line.average_processing_time = millis(line.average_processing_time);
          stats.push(line);
        });
      },
    );

    stats.sort((a, b) => {
      return b.num_requests - a.num_requests;
    });

    if (stats.length) {
      console.table(stats);
    } else {
      cmd.stdout("no services found");
    }

    await nc.close();
    return 0;
  },
});

adm.addCommand({
  use: "info [--name] [--id]",
  short: "get service info",
  run: async (
    cmd: Command,
    _args: string[],
    flags: Flags,
  ): Promise<number> => {
    const nc = await createConnection(flags);
    const mc = nc.services.client();
    const opts = options(flags);

    const infos = (await collect(await mc.info(opts.name, opts.id))).sort(
      (a, b) => {
        const A = `${a.name} ${a.version}`;
        const B = `${b.name} ${b.version}`;
        return B.localeCompare(A);
      },
    );
    if (infos.length) {
      console.table(infos);
    } else {
      cmd.stdout("no services found");
    }
    await nc.close();
    return 0;
  },
});

root.addCommand({
  short: "start an instance of the echo service",
  use: "start",
  run: async (
    _cmd: Command,
    _args: string[],
    flags: Flags,
  ): Promise<number> => {
    const nc = await createConnection(flags);
    const srv = await nc.services.add({
      name: "echo",
      version: "0.0.1",
      description: "echo service",
      metadata: {
        "entry": "sample echo service",
      },
    });

    srv.addEndpoint("echo", (err, req) => {
      if (err) {
        console.log(err);
        srv.stop(err);
      } else {
        console.log(`Handling request on subject ${req.subject}`);
        const h = req.headers || headers();
        h.set("ConnectedUrl", `nats://${nc.getServer()}`);
        h.set("Timestamp", new Date().toUTCString());
        if (nc.info?.cluster) {
          h.set("ConnectedCluster", nc.info.cluster);
        }
        req.respond(req.data, { headers: h });
      }
    });

    try {
      console.log("waiting for requests");
      await srv.stopped;
      return 0;
    } catch (err) {
      console.error(err);
      return 1;
    }
  },
});

const request = root.addCommand({
  short: "make a request a service",
  use: "request",
  run: async (
    _cmd: Command,
    _args: string[],
    flags: Flags,
  ): Promise<number> => {
    const nc = await createConnection(flags);
    const subj = flags.value<string>("subject");
    console.log(`Sending request on "${subj}"`);
    const payload = flags.value<string>("payload");
    const json = flags.value<string>("json");
    const timeout = flags.value<number>("timeout");
    const start = Date.now();
    const r = await nc.request(subj, StringCodec().encode(payload), {
      timeout,
    });
    const time = Date.now() - start;
    console.log(`Received with rtt ${time}ms`);

    r.headers?.keys().forEach((k) => {
      console.log(`${k}: ${r.headers?.get(k)}`);
    });
    console.log();
    if (json) {
      console.dir(r.json());
    } else {
      console.log(r.string());
    }
    console.log();

    await nc.close();

    return 0;
  },
});

request.addFlag({
  name: "subject",
  type: "string",
  usage: "subject to send the request on",
  default: "echo",
});

request.addFlag({
  name: "payload",
  type: "string",
  usage: "payload to send",
  default: "hello service",
});

request.addFlag({
  name: "timeout",
  type: "number",
  usage: "timeout in milliseconds",
  default: 2000,
});

request.addFlag({
  name: "json",
  type: "boolean",
  usage: "interpret response as JSON",
});

function createConnection(flags: Flags): Promise<NatsConnection> {
  const servers = [flags.value<string>("server")];
  return connect({ servers });
}

function options(
  flags: Flags,
): { name?: string; id?: string } {
  let name: string | undefined = flags.value<string>("name");
  if (name === "") {
    name = undefined;
  }
  let id: string | undefined = flags.value<string>("id");
  if (id === "") {
    id = undefined;
  }
  return { name, id };
}

Deno.exit(await root.execute(Deno.args));

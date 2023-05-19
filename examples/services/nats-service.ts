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
import { collect } from "../../nats-base-client/util.ts";
import { millis } from "../../nats-base-client/jsutil.ts";
import { NatsConnection } from "../../nats-base-client/types.ts";
import { connect } from "../../src/connect.ts";
import { QueuedIterator } from "../../nats-base-client/queued_iterator.ts";

const root = cli({
  use:
    "nats-service [--server host:port] [ping|info|status] [--name n] [--id id])",
});

root.addFlag({
  name: "server",
  short: "s",
  type: "string",
  usage: "NATS server host:port",
  default: "demo.nats.io",
  persistent: true,
});

root.addFlag({
  short: "n",
  name: "name",
  type: "string",
  usage: "service name to filter on",
  default: "",
  persistent: true,
});
root.addFlag({
  name: "id",
  type: "string",
  usage: "service id to filter on",
  default: "",
  persistent: true,
});

root.addCommand({
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
    const infos = (await safeCollect(mc.ping(opts.name, opts.id))).sort(
      (a, b) => {
        const A = `${a.name} ${a.version}`;
        const B = `${b.name} ${b.version}`;
        return B.localeCompare(A);
      },
    );
    if (infos.length) {
      console.table(infos);
    }
    await nc.close();
    return 0;
  },
});

root.addCommand({
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
    (await safeCollect(mc.stats(opts.name, opts.id))).forEach(
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
    }

    await nc.close();
    return 0;
  },
});

root.addCommand({
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

    const infos = (await safeCollect(mc.info(opts.name, opts.id))).sort(
      (a, b) => {
        const A = `${a.name} ${a.version}`;
        const B = `${b.name} ${b.version}`;
        return B.localeCompare(A);
      },
    );
    if (infos.length) {
      console.table(infos);
    }
    await nc.close();
    return 0;
  },
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

async function safeCollect<T>(iter: Promise<QueuedIterator<T>>): Promise<T[]> {
  try {
    return await collect(await iter);
  } catch (err) {
    if (err.code === "503") {
      console.log("no services found");
      return Promise.resolve([]);
    } else {
      throw err;
    }
  }
}

Deno.exit(await root.execute(Deno.args));

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
import { connect, headers } from "../../src/mod.ts";

const root = cli({
  use: "echo-service [--server server:port]",
  run: async (
    _cmd: Command,
    _args: string[],
    flags: Flags,
  ): Promise<number> => {
    const servers = flags.values<string>("server");
    const nc = await connect({ servers });
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

root.addFlag({
  name: "server",
  short: "s",
  type: "string",
  usage: "NATS server host:port",
  default: "demo.nats.io",
  persistent: true,
});

Deno.exit(await root.execute(Deno.args));

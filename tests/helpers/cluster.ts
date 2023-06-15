/*
 * Copyright 2020-2023 The NATS Authors
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

import { NatsServer } from "./mod.ts";
import { parse } from "https://deno.land/std@0.190.0/flags/mod.ts";
import { rgb24 } from "https://deno.land/std@0.190.0/fmt/colors.ts";
import { setTimeout } from "https://deno.land/std@0.190.0/node/timers.ts";

const defaults = {
  c: 3,
  p: 4222,
  D: false,
  chaos: false,
};

const argv = parse(
  Deno.args,
  {
    alias: {
      "p": ["port"],
      "c": ["count"],
      "d": ["debug"],
      "D": ["server-debug"],
      "j": ["jetstream"],
    },
    default: defaults,
    boolean: ["debug", "jetstream", "server-debug", "chaos"],
  },
);

if (argv.h || argv.help) {
  console.log(
    "usage: cluster [--count 3] [--port 4222] [--debug] [--server_debug] [--jetstream] [--chaos]\n",
  );
  Deno.exit(0);
}

const count = argv["count"] as number || 3;
const port = argv["port"] as number ?? 4222;

try {
  const base = { debug: false };
  const serverDebug = argv["server-debug"];
  if (serverDebug) {
    base.debug = true;
  }
  const cluster = argv.jetstream
    ? await NatsServer.jetstreamCluster(
      count,
      Object.assign(base, {
        port,
        jetstream: {
          max_file_store: -1,
          max_mem_store: -1,
        },
      }),
      base.debug,
    )
    : await NatsServer.cluster(
      count,
      Object.assign(base, { port }),
      base.debug,
    );

  cluster.forEach((s) => {
    const pid = rgb24(`[${s.process.pid}]`, s.rgb);
    console.log(
      `${pid} ${s.configFile} at nats://${s.hostname}:${s.port}
\tcluster://${s.hostname}:${s.cluster}
\thttp://127.0.0.1:${s.monitoring}
\tstore: ${s.config?.jetstream?.store_dir}`,
    );
  });

  if (argv.chaos === true && confirm("start chaos?")) {
    chaos(cluster, 0);
  }

  waitForStop();
} catch (err) {
  console.error(err);
}

function randomBetween(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min) + min);
}

function chaos(cluster: NatsServer[], delay: number) {
  setTimeout(() => {
    setInterval(() => {
      restart(cluster);
    }, 3000);
  }, delay);
}

function restart(cluster: NatsServer[]) {
  const millis = randomBetween(0, 10000);
  const idx = randomBetween(0, cluster.length);
  if (cluster[idx] === null) {
    // try again
    setTimeout(() => {
      restart(cluster);
    });
    return;
  }
  const old = cluster[idx];
  // @ts-ignore: test
  cluster[idx] = null;
  setTimeout(() => {
    const oldPid = old.pid();
    console.log(
      rgb24(
        `[${oldPid}] chaos is restarting server at port ${old.port}`,
        old.rgb,
      ),
    );
    old.restart()
      .then((s) => {
        s.rgb = old.rgb;
        cluster[idx] = s;
        console.log(rgb24(`[${s.pid()}] replaces PID ${oldPid}`, s.rgb));
      }).catch((err) => {
        console.log(`failed to replace ${oldPid}: ${err.message}`);
        console.log(`to manually restart: nats-server -c ${old.configFile}`);
      });
  }, millis);
}

function waitForStop(): void {
  console.log("control+c to terminate");
  Deno.addSignalListener("SIGTERM", () => {
    Deno.exit();
  });
}

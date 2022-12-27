/*
 * Copyright 2020-2022 The NATS Authors
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
import { parse } from "https://deno.land/std@0.168.0/flags/mod.ts";
import { rgb24 } from "https://deno.land/std@0.168.0/fmt/colors.ts";
import { setTimeout } from "https://deno.land/std@0.161.0/node/timers.ts";

const defaults = {
  c: 2,
  p: 4222,
  chaos: false,
};

const argv = parse(
  Deno.args,
  {
    alias: {
      "p": ["port"],
      "c": ["count"],
      "d": ["debug"],
      "j": ["jetstream"],
    },
    default: defaults,
    boolean: ["debug", "jetstream"],
  },
);

if (argv.h || argv.help) {
  console.log(
    "usage: cluster [--count 2] [--port 4222] [--debug] [--jetstream] [--chaos millis]\n",
  );
  Deno.exit(0);
}

try {
  const port = typeof argv.port === "number" ? argv.port : parseInt(argv.port);
  const cluster = argv.jetstream
    ? await NatsServer.jetstreamCluster(
      argv.count,
      { port },
      argv.debug,
    )
    : await NatsServer.cluster(
      argv.count,
      { port },
      argv.debug,
    );

  cluster.forEach((s) => {
    const pid = `[${s.process.pid}]`;
    console.log(rgb24(
      `${pid} ${s.configFile} at nats://${s.hostname}:${s.port}
\tcluster://${s.hostname}:${s.cluster}
\thttp://127.0.0.1:${s.monitoring}
\tstore: ${s.config?.jetstream?.store_dir}`,
      s.rgb,
    ));
  });

  if (argv.chaos) {
    chaos(cluster, parseInt(argv.chaos));
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
    const p = old.restart();
    p.then((s) => {
      s.rgb = old.rgb;
      cluster[idx] = s;
      console.log(rgb24(`[${s.pid()}] replaces PID ${oldPid}`, s.rgb));
    });
  }, millis);
}

function waitForStop(): void {
  console.log("control+c to terminate");
  Deno.addSignalListener("SIGTERM", () => {
    Deno.exit();
  });
}

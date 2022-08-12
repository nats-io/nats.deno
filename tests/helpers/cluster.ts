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
import { parse } from "https://deno.land/std@0.152.0/flags/mod.ts";
import { rgb24 } from "https://deno.land/std@0.152.0/fmt/colors.ts";

const defaults = {
  c: 2,
  p: 4222,
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
    "usage: cluster [--count 2] [--port 4222] [--debug] [--jetstream]\n",
  );
  Deno.exit(0);
}

try {
  const cluster = argv.jetstream
    ? await NatsServer.jetstreamCluster(
      argv.count,
      { port: argv.port },
      argv.debug,
    )
    : await NatsServer.cluster(
      argv.count,
      { port: argv.port },
      argv.debug,
    );

  cluster.forEach((s) => {
    const pid = `[${s.process.pid}]`;
    const cpid = rgb24(pid, s.rgb);
    console.log(
      `${cpid} ${s.configFile} at nats://${s.hostname}:${s.port} cluster://${s.hostname}:${s.cluster} http://127.0.0.1:${s.monitoring} - store: ${s.config?.jetstream?.store_dir}`,
    );
  });

  waitForStop();
} catch (err) {
  console.error(err);
}

function waitForStop(): void {
  console.log("control+c to terminate");
  Deno.addSignalListener("SIGTERM", () => {
    Deno.exit();
  });
}

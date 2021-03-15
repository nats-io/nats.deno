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

import { NatsServer } from "./mod.ts";
import { parse } from "https://deno.land/std@0.90.0/flags/mod.ts";

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
    },
    default: defaults,
  },
);

if (argv.h || argv.help) {
  console.log(
    "usage: cluster [--count 2] [--port 4222] [--debug]\n",
  );
  Deno.exit(0);
}

try {
  const cluster = await NatsServer.cluster(
    argv.count,
    { port: argv.port },
    argv.debug,
  );
  cluster.forEach((s) => {
    console.log(
      `launched server [${s.process.pid}] at ${s.hostname}:${s.port} - monitor ${s.monitoring}`,
    );
  });

  await waitForStop();
} catch (err) {
  console.error(err);
}

async function waitForStop(): Promise<void> {
  console.log("control+c to terminate");
  const sig = Deno.signal(Deno.Signal.SIGTERM);
  for await (const _ of sig) {
    sig.dispose();
  }
}

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
import { parse } from "https://deno.land/std@0.200.0/flags/mod.ts";
import { connect, ConnectionOptions, RequestStrategy } from "../../src/mod.ts";
import { humanizeBytes } from "./03_util.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "a": ["asset"],
      "c": ["chunk"],
    },
    default: {
      s: "127.0.0.1:4222",
      c: 0,
      a: 1024 * 1024,
    },
    string: ["server"],
  },
);

const copts = { servers: argv.s } as ConnectionOptions;
const nc = await connect(copts);

const max_chunk = argv.c;
const size = argv.a as number;
console.log(`requesting ${humanizeBytes(size)}`);

const start = performance.now();
const iter = await nc.requestMany(
  "data",
  JSON.stringify({ max_chunk, size }),
  {
    strategy: RequestStrategy.SentinelMsg,
    maxWait: 10000,
  },
);

let received = 0;
let count = 0;
for await (const m of iter) {
  count++;
  received += m.data.length;
  if (m.data.length === 0) {
    // sentinel
    count--;
  }
}

console.log(performance.now() - start, "ms");

console.log(`received ${count} responses: ${humanizeBytes(received)} bytes`);
await nc.close();

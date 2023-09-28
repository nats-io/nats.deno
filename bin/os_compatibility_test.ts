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

import { connect, millis, Msg } from "../src/mod.ts";

const nc = await connect({ servers: "localhost:4222" });
const js = nc.jetstream();
console.log("connected");

const sub = nc.subscribe("tests.object-store.>");

const default_bucket = async function (m: Msg): Promise<void> {
  console.log(`raw data received: ${m}`)
  const config = m.json<{
    config: {
      bucket: string;
    };
  }>();
  await js.views.os(config.config.bucket);
  m.respond();
};

const custom_bucket = async function (m: Msg): Promise<void> {
  const config = m.json< { config:Record<string, unknown> } >();
  console.log(`custom  config: ${JSON.stringify(config)}`);
  const name = config.config.bucket as string || "";
  delete config.config.bucket;
  config.config.millis = millis(config.config.max_age as number || 0);
  await js.views.os(name, config.config);
  m.respond();
};

const entry = async function (m: Msg): Promise<void> {
  const t = m.json<{
    bucket: string;
    config: { description: string; name: string };
    url: string;
  }>();

  const name = t.bucket as string || "";
  const os = await js.views.os(name);
  const d = await fetch(t.url);
  if (d.ok && d.body) {
    await os.put(
      { name: t.config.name, description: t.config.description },
      d.body,
    );
  }
  m.respond();
};

const result = function (message: Msg): Promise<void> {
  if (message.headers) {
    console.log("test failed");
    return Promise.reject("test failed");
  } else {
    console.log("object store test done");
    return Promise.resolve();
  }
};

const opts = [
  default_bucket,
  result,
  custom_bucket,
  result,
  // customized,
  // entry,
  // done,
];

console.log("waiting for tests");
let i = 0;
for await (const m of sub) {
  await opts[i++](m);
}

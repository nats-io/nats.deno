/*
 * Copyright 2020 The NATS Authors
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

// import the connect function
import { connect, Subscription } from "../../src/mod.ts";

// create a connection
const nc = await connect({ url: "demo.nats.io:4222" });

// create a simple subscriber that listens for only one message
// and then auto unsubscribes, ending the async iterator
const sub = nc.subscribe("hello", { max: 3 });
const h1 = handler(sub);
const msub = nc.subscribe("hello");
const h2 = handler(msub);

for (let i = 1; i < 6; i++) {
  nc.publish("hello", `hello-${i}`);
}

// await the handlers come back
await Promise.all([h1, h2]);
await nc.close();

async function handler(s: Subscription) {
  let processed = 0;
  console.log(
    `sub [${s.sid}] listening to ${s.subject} ${
      s.max ? "and will unsubscribe after " + s.max + " msgs" : ""
    }`,
  );
  for await (const m of s) {
    console.log(`sub [${s.sid}] #${++processed}}: ${m.data}`);
  }
  console.log(`sub [${s.sid}] is done.`);
}

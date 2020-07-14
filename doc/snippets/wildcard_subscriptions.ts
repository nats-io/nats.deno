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

import { connect, Subscription } from "../../src/mod.ts";
const nc = await connect({ url: "demo.nats.io:4222" });

// subscriptions can have wildcard subjects
// the '*' matches any string in the specified token position
const s1 = nc.subscribe("help.*.system");
const s2 = nc.subscribe("help.me.*");
// the '>' matches any tokens in that position or following
// '>' can only be specified at the end
const s3 = nc.subscribe("help.>");

async function printMsgs(s: Subscription) {
  console.log(`listening for ${s.subject}`);
  const c = (13 - s.subject.length);
  const pad = "".padEnd(c);
  for await (const m of s) {
    console.log(
      `[${s.subject}]${pad} #${s.getProcessed()} - ${m.subject} ${
        m.data ? " " + m.data : ""
      }`,
    );
  }
}

printMsgs(s1);
printMsgs(s2);
printMsgs(s3);

// don't exit until the client closes
await nc.closed();

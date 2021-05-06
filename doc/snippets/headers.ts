/*
 * Copyright 2021 The NATS Authors
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

import { connect, createInbox, Empty, headers } from "../../src/mod.ts";
import { nuid } from "../../nats-base-client/nuid.ts";

const nc = await connect(
  {
    servers: `demo.nats.io`,
  },
);

const subj = createInbox();
const sub = nc.subscribe(subj);
(async () => {
  for await (const m of sub) {
    if (m.headers) {
      for (const [key, value] of m.headers) {
        console.log(`${key}=${value}`);
      }
      console.log("ID", m.headers.get("ID"));
      console.log("Id", m.headers.get("Id"));
      console.log("id", m.headers.get("id"));
    }
  }
})().then();

// header names can be any printable ASCII character with the  exception of `:`.
// header values can be any ASCII character except `\r` or `\n`.
// see https://www.ietf.org/rfc/rfc822.txt
const h = headers();
h.append("id", nuid.next());
h.append("unix_time", Date.now().toString());
nc.publish(subj, Empty, { headers: h });

await nc.flush();
await nc.close();

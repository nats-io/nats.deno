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

import { connect } from "../src/connect.ts";
import { NatsServer } from "./helpers/launcher.ts";
import { Lock } from "./helpers/lock.ts";
import {
  assertEquals,
  assert,
  fail,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";
import { assertErrorCode } from "./helpers/mod.ts";
import { ErrorCode } from "../src/mod.ts";
import { decodeHeaders, encodeHeader } from "../nats-base-client/util.ts";

// Deno.test("headers - option", async () => {
//   const srv = await NatsServer.start();
//   const nc = await connect(
//     {
//       url: `nats://127.0.0.1:${srv.port}`,
//       headers: true,
//       noResponders: true,
//     },
//   );
//
//   const lock = Lock();
//   await nc.request("foo")
//     .then((m) => {
//       if(m.headers) {
//         console.table(new TextDecoder().decode(encodeHeader(m.headers)));
//         lock.unlock();
//       }
//     })
//     .catch((err) => {
//       console.log(err);
//     });
//
//   await lock;
//   await nc.close();
//   await srv.stop();
// });

Deno.test("headers - list", () => {
  const h = new Headers();
  h.append("a", "a");
  h.append("a", "b");

  console.log(new TextDecoder().decode(encodeHeader(h)));

  const h2 = new Headers();
  h2.set("a", "a");
  h2.append("a", "b");

  console.log(new TextDecoder().decode(encodeHeader(h)));
});

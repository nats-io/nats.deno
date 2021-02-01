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

import { connect, createInbox, Empty, ErrorCode, headers } from "../src/mod.ts";
import { assertErrorCode, Lock, NatsServer } from "./helpers/mod.ts";
import {
  assert,
  assertEquals,
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";

Deno.test("noresponders - option", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      servers: `127.0.0.1:${srv.port}`,
      headers: true,
      noResponders: true,
    },
  );

  const lock = Lock();
  await nc.request(createInbox())
    .then(() => {
      fail("should have not resolved");
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.NO_RESPONDERS);
      lock.unlock();
    });

  await lock;
  await nc.close();
  await srv.stop();
});

Deno.test("noresponders - list", async () => {
  const srv = await NatsServer.start();
  const nc = await connect(
    {
      servers: `nats://127.0.0.1:${srv.port}`,
      headers: true,
      noResponders: true,
    },
  );

  const subj = createInbox();
  const sub = nc.subscribe(subj);
  (async () => {
    for await (const m of sub) {
      const h = headers();
      h.append("a", "b");
      m.respond(Empty, { headers: h });
    }
  })().then();
  await nc.flush();

  const msg = await nc.request(subj);
  assert(msg.headers);
  assertEquals(msg.headers.get("a"), "b");
  await nc.close();
  await srv.stop();
});

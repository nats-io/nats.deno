/*
 * Copyright 2018 The NATS Authors
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

import { connect } from "../src/mod.ts";
import { Lock, NatsServer } from "./helpers/mod.ts";
import { ParserState } from "../nats-base-client/mod.ts";
import {
  assertEquals,
} from "https://deno.land/std/testing/asserts.ts";

Deno.test("close handler is called on close", async () => {
  const ns = await NatsServer.start();
  let lock = Lock(20000);
  let nc = await connect(
    { url: `nats://localhost:${ns.port}`, reconnect: false },
  );
  nc.addEventListener("close", () => {
    lock.unlock();
  });

  await ns.stop();
  await lock;
});

Deno.test("close process inbound ignores", async () => {
  const ns = await NatsServer.start();
  let lock = Lock(2000);
  let nc = await connect(
    { url: `nats://localhost:${ns.port}`, reconnect: false },
  );
  nc.addEventListener("close", () => {
    assertEquals(ParserState.CLOSED, nc.protocol.state);
    lock.unlock();
  });

  await ns.stop();
  await lock;
});

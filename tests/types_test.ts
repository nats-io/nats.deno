/*
 * Copyright 2018-2020 The NATS Authors
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

import {
  connect,
  Nuid,
  Payload,
  Msg,
} from "../src/mod.ts";
import { Lock } from "./helpers/mod.ts";
import { DataBuffer } from "../nats-base-client/mod.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std/testing/asserts.ts";

const u = "demo.nats.io:4222";

const nuid = new Nuid();

Deno.test("types - json types", async () => {
  let lock = Lock();
  let nc = await connect({ url: u, payload: Payload.JSON });
  let subj = nuid.next();
  nc.subscribe(subj, {
    callback: (_, msg: Msg) => {
      assertEquals(typeof msg.data, "number");
      assertEquals(msg.data, 6691);
      lock.unlock();
    },
    max: 1,
  });

  nc.publish(subj, 6691);
  nc.flush();
  await lock;
  await nc.close();
});

Deno.test("types - string types", async () => {
  let lock = Lock();
  let nc = await connect({ url: u, payload: Payload.STRING });
  let subj = nuid.next();
  nc.subscribe(subj, {
    callback: (_, msg: Msg) => {
      assertEquals(typeof msg.data, "string");
      assertEquals(msg.data, "hello world");
      lock.unlock();
    },
    max: 1,
  });

  nc.publish(subj, DataBuffer.fromAscii("hello world"));
  nc.flush();
  await lock;
  await nc.close();
});

Deno.test("types - binary types", async () => {
  let lock = Lock();
  let nc = await connect({ url: u, payload: Payload.BINARY });
  let subj = nuid.next();
  let m1!: Msg;
  nc.subscribe(subj, {
    callback: (_, msg: Msg) => {
      assert(msg.data instanceof Uint8Array);
      assertEquals(DataBuffer.toAscii(msg.data), "hello world");
      lock.unlock();
    },
    max: 1,
  });

  nc.publish(subj, DataBuffer.fromAscii("hello world"));
  nc.flush();
  await lock;
  await nc.close();
});

Deno.test("types - binary encoded per client", async () => {
  let lock = Lock(2);
  let nc1 = await connect({ url: u, payload: Payload.BINARY });
  let nc2 = await connect({ url: u, payload: Payload.STRING });
  let subj = nuid.next();

  nc1.subscribe(subj, {
    callback: (_, msg: Msg) => {
      lock.unlock();
      assert(msg.data instanceof Uint8Array);
      assertEquals(DataBuffer.toAscii(msg.data), "hello world");
    },
    max: 1,
  });

  nc2.subscribe(subj, {
    callback: (_, msg: Msg) => {
      lock.unlock();
      assertEquals(typeof msg.data, "string");
      assertEquals(msg.data, "hello world");
    },
    max: 1,
  });
  await nc1.flush();
  await nc2.flush();

  nc2.publish(subj, "hello world");
  await nc2.flush();
  await lock;
  await nc1.close();
  await nc2.close();
});

Deno.test("types - binary client gets binary", async () => {
  let lock = Lock();
  let nc = await connect({ url: u, payload: Payload.BINARY });
  let subj = nuid.next();
  nc.subscribe(subj, {
    callback: (_, msg: Msg) => {
      assert(msg.data instanceof Uint8Array);
      assertEquals(DataBuffer.toAscii(msg.data), "hello world");
      lock.unlock();
    },
    max: 1,
  });

  nc.publish(subj, "hello world");
  await nc.flush();
  await lock;
  await nc.close();
});

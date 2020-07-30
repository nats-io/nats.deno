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
  createInbox,
  Payload,
  Msg,
  NatsConnection,
} from "../src/mod.ts";
import {
  DataBuffer,
  deferred,
} from "../nats-base-client/internal_mod.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";

const u = "demo.nats.io:4222";

function mh(nc: NatsConnection, subj: string): Promise<Msg> {
  const dm = deferred<Msg>();
  const sub = nc.subscribe(subj, { max: 1 });
  const _ = (async () => {
    for await (const m of sub) {
      dm.resolve(m);
    }
  })();
  return dm;
}

Deno.test("types - json types", async () => {
  const nc = await connect({ url: u, payload: Payload.JSON });
  const subj = createInbox();
  const dm = mh(nc, subj);
  nc.publish(subj, 6691);
  const msg = await dm;
  assertEquals(typeof msg.data, "number");
  assertEquals(msg.data, 6691);
  await nc.close();
});

Deno.test("types - string types", async () => {
  const nc = await connect({ url: u, payload: Payload.STRING });
  const subj = createInbox();
  const dm = mh(nc, subj);
  nc.publish(subj, DataBuffer.fromAscii("hello world"));
  const msg = await dm;
  assertEquals(typeof msg.data, "string");
  assertEquals(msg.data, "hello world");
  await nc.close();
});

Deno.test("types - binary types", async () => {
  const nc = await connect({ url: u, payload: Payload.BINARY });
  const subj = createInbox();
  const dm = mh(nc, subj);
  nc.publish(subj, DataBuffer.fromAscii("hello world"));
  const msg = await dm;
  assert(msg.data instanceof Uint8Array);
  assertEquals(DataBuffer.toAscii(msg.data), "hello world");
  await nc.close();
});

Deno.test("types - binary encoded per client", async () => {
  const nc1 = await connect({ url: u, payload: Payload.BINARY });
  const nc2 = await connect({ url: u, payload: Payload.STRING });
  const subj = createInbox();

  const mhb = mh(nc1, subj);
  const mhs = mh(nc2, subj);

  await nc1.flush();
  await nc2.flush();

  nc2.publish(subj, "hello world");

  const bm = await mhb;
  assert(bm.data instanceof Uint8Array);
  assertEquals(DataBuffer.toAscii(bm.data), "hello world");

  const sm = await mhs;
  assertEquals(typeof sm.data, "string");
  assertEquals(sm.data, "hello world");

  await nc1.close();
  await nc2.close();
});

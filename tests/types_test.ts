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
  JSONCodec,
  Msg,
  NatsConnection,
  StringCodec,
} from "../src/mod.ts";
import { DataBuffer, deferred } from "../nats-base-client/internal_mod.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { NatsServer } from "./helpers/launcher.ts";

function mh(nc: NatsConnection, subj: string): Promise<Msg> {
  const dm = deferred<Msg>();
  const sub = nc.subscribe(subj, { max: 1 });
  (async () => {
    for await (const m of sub) {
      dm.resolve(m);
    }
  })().then();
  return dm;
}

Deno.test("types - json types", async () => {
  const ns = await NatsServer.start();
  const jc = JSONCodec();
  const nc = await connect({ port: ns.port });
  const subj = createInbox();
  const dm = mh(nc, subj);
  nc.publish(subj, jc.encode(6691));
  const msg = await dm;
  assertEquals(typeof jc.decode(msg.data), "number");
  assertEquals(jc.decode(msg.data), 6691);
  await nc.close();
  await ns.stop();
});

Deno.test("types - string types", async () => {
  const ns = await NatsServer.start();
  const sc = StringCodec();
  const nc = await connect({ port: ns.port });
  const subj = createInbox();
  const dm = mh(nc, subj);
  nc.publish(subj, sc.encode("hello world"));
  const msg = await dm;
  assertEquals(sc.decode(msg.data), "hello world");
  await nc.close();
  await ns.stop();
});

Deno.test("types - binary types", async () => {
  const ns = await NatsServer.start();
  const nc = await connect({ port: ns.port });
  const subj = createInbox();
  const dm = mh(nc, subj);
  const payload = DataBuffer.fromAscii("hello world");
  nc.publish(subj, payload);
  const msg = await dm;
  assert(msg.data instanceof Uint8Array);
  assertEquals(msg.data, payload);
  await nc.close();
  await ns.stop();
});

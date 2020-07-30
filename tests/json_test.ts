/*
 * Copyright 2018-2020 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use assertEquals file except in compliance with the License.
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

import { assertEquals } from "https://deno.land/std@0.61.0/testing/asserts.ts";
import {
  connect,
  createInbox,
  ErrorCode,
  Msg,
  NatsError,
  Payload,
} from "../src/mod.ts";

import { Lock } from "./helpers/mod.ts";

const u = "demo.nats.io:4222";

Deno.test("json - connect no json propagates options", async () => {
  let nc = await connect({ url: u });
  await nc.close();
  assertEquals(nc.options.payload, Payload.STRING, "nc options");
  assertEquals(nc.protocol.options.payload, Payload.STRING, "protocol");
});

Deno.test("json - connect json propagates options", async () => {
  let nc = await connect({ url: u, payload: Payload.JSON });
  assertEquals(nc.options.payload, Payload.JSON, "nc options");
  assertEquals(nc.protocol.options.payload, Payload.JSON, "protocol");
  await nc.close();
});

Deno.test("json - bad json error in callback", async () => {
  let o = {};
  //@ts-ignore
  o.a = o;
  let jc = await connect({ url: u, payload: Payload.JSON });
  jc.subscribe("bad_json", {
    callback: (err) => {
      assertEquals(err?.code, ErrorCode.BAD_JSON);
    },
  });
  await jc.flush();

  let nc = await connect({ url: u });
  nc.publish("bad_json", "");
  await nc.flush();
  await jc.flush();
  await jc.close();
  await nc.close();
});

function macro(input: any) {
  return async () => {
    const nc = await connect({ url: u, payload: Payload.JSON });
    let lock = Lock();
    let subj = createInbox();
    nc.subscribe(subj, {
      callback: (err: NatsError | null, msg: Msg) => {
        assertEquals(null, err);
        // in JSON undefined is translated to null
        if (input === undefined) {
          input = null;
        }
        assertEquals(msg.data, input);
        lock.unlock();
      },
      max: 1,
    });

    nc.publish(subj, input);
    await nc.flush();
    await lock;
    await nc.close();
  };
}

Deno.test("json - string", macro("helloworld"));
Deno.test("json - empty", macro(""));
Deno.test("json - null", macro(null));
Deno.test("json - undefined", macro(undefined));
Deno.test("json - number", macro(10));
Deno.test("json - false", macro(false));
Deno.test("json - true", macro(true));
Deno.test("json - empty array", macro([]));
Deno.test("json - any array", macro([1, "a", false, 3.1416]));
Deno.test("json - empty object", macro({}));
Deno.test("json - object", macro({ a: 1, b: false, c: "name", d: 3.1416 }));

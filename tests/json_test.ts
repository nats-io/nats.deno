/*
 * Copyright 2018-2021 The NATS Authors
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

import { assertEquals } from "https://deno.land/std@0.152.0/testing/asserts.ts";
import {
  createInbox,
  ErrorCode,
  JSONCodec,
  Msg,
  NatsError,
} from "../src/mod.ts";

import { Lock } from "./helpers/mod.ts";
import { assertThrowsErrorCode } from "./helpers/asserts.ts";
import { cleanup, setup } from "./jstest_util.ts";

Deno.test("json - bad json error in callback", () => {
  const o = {};
  //@ts-ignore: bad json
  o.a = o;

  const jc = JSONCodec();
  assertThrowsErrorCode(() => {
    jc.encode(o);
  }, ErrorCode.BadJson);
});

function macro(input: unknown) {
  return async () => {
    const { ns, nc } = await setup();
    const jc = JSONCodec();
    const lock = Lock();
    const subj = createInbox();
    nc.subscribe(subj, {
      callback: (err: NatsError | null, msg: Msg) => {
        assertEquals(null, err);
        // in JSON undefined is translated to null
        if (input === undefined) {
          input = null;
        }
        assertEquals(jc.decode(msg.data), input);
        lock.unlock();
      },
      max: 1,
    });

    nc.publish(subj, jc.encode(input));
    await nc.flush();
    await lock;
    await cleanup(ns, nc);
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

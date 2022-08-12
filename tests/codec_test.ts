/*
 * Copyright 2020-2022 The NATS Authors
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
import { JSONCodec, StringCodec } from "../nats-base-client/codec.ts";
import { assertEquals } from "https://deno.land/std@0.152.0/testing/asserts.ts";

Deno.test("codec - string", () => {
  const sc = StringCodec();
  const d = sc.encode("hello");
  assertEquals(sc.decode(d), "hello");
});

Deno.test("codec - json", () => {
  const sc = JSONCodec();
  const o = { hello: "world" };
  const d = sc.encode(o);
  assertEquals(sc.decode(d), o);
});

Deno.test("codec - json w/ reviver", () => {
  // deno-lint-ignore no-explicit-any
  const sc = JSONCodec((k: string, v: any) => k === "time" ? new Date(v) : v);
  const o = { time: new Date() };
  const d = sc.encode(o);
  assertEquals(sc.decode(d), o);
});

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
import { MsgHeaders } from "../nats-base-client/headers.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";

Deno.test("msgheaders - basics", () => {
  const h = new MsgHeaders();
  h.add("foo", "bar");
  h.add("foo", "bam");
  h.add("foo-bar", "baz");

  assertEquals(h.size(), 3);

  let header = MsgHeaders.canonicalMIMEHeaderKey("foo");
  assertEquals("Foo", header);
  const foos = h.values(header);
  assertEquals(2, foos.length);
  assert(foos.indexOf("bar") !== -1);
  assert(foos.indexOf("bam") !== -1);

  header = MsgHeaders.canonicalMIMEHeaderKey("foo-bar");
  assertEquals("Foo-Bar", header);
  const foobars = h.values(header);
  assertEquals(1, foobars.length);
  assert(foobars.indexOf("baz") !== -1);

  const a = h.encode();
  const hh = MsgHeaders.decode(a);
  console.log(h.equals(hh));
});

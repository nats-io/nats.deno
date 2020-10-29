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
import {
  assert,
  assertArrayContains,
  assertEquals,
  assertThrows,
} from "https://deno.land/std@0.74.0/testing/asserts.ts";
import { MsgHdrsImpl, NatsError } from "../nats-base-client/internal_mod.ts";

Deno.test("msgheaders - basics", () => {
  const h = new MsgHdrsImpl();
  assertEquals(h.size(), 0);
  assert(!h.has("foo"));
  h.append("foo", "bar");
  h.append("foo", "bam");
  h.append("foo-bar", "baz");

  assertEquals(h.size(), 3);
  h.set("bar-foo", "foo");
  assertEquals(h.size(), 4);
  h.delete("bar-foo");
  assertEquals(h.size(), 3);

  let header = MsgHdrsImpl.canonicalMIMEHeaderKey("foo");
  assertEquals("Foo", header);
  assert(h.has("Foo"));
  assert(h.has("foo"));
  const foos = h.values(header);
  assertEquals(2, foos.length);
  assertArrayContains(foos, ["bar", "bam"]);
  assert(foos.indexOf("baz") === -1);

  header = MsgHdrsImpl.canonicalMIMEHeaderKey("foo-bar");
  assertEquals("Foo-Bar", header);
  const foobars = h.values(header);
  assertEquals(1, foobars.length);
  assertArrayContains(foobars, ["baz"]);

  const a = h.encode();
  const hh = MsgHdrsImpl.decode(a);
  assert(h.equals(hh));

  hh.set("foo-bar-baz", "fbb");
  assert(!h.equals(hh));
});

Deno.test("msgheaders - illegal key", () => {
  const h = new MsgHdrsImpl();
  ["bad:", "bad ", String.fromCharCode(127)].forEach((v) => {
    assertThrows(() => {
      h.set(v, "aaa");
    }, NatsError);
  });

  ["\r", "\n"].forEach((v) => {
    assertThrows(() => {
      h.set("a", v);
    }, NatsError);
  });
});

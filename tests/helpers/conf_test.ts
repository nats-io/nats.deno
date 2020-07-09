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

import { toConf } from "./launcher.ts";
import { assertEquals } from "https://deno.land/std/testing/asserts.ts";

Deno.test("conf - serializing simple", () => {
  let x = {
    test: "one",
  };
  let y = toConf(x);

  let buf = y.split("\n");
  buf.forEach(function (e, i) {
    buf[i] = e.trim();
  });

  let z = buf.join(" ");
  assertEquals(z, "test: one");
});

Deno.test("conf - serializing nested", () => {
  let x = {
    a: "one",
    b: {
      a: "two",
    },
  };
  let y = toConf(x);

  let buf = y.split("\n");
  buf.forEach(function (e, i) {
    buf[i] = e.trim();
  });

  let z = buf.join(" ");
  assertEquals(z, "a: one b { a: two }");
});

Deno.test("conf - serializing array", () => {
  let x = {
    a: "one",
    b: ["a", "b", "c"],
  };
  let y = toConf(x);

  let buf = y.split("\n");
  buf.forEach(function (e, i) {
    buf[i] = e.trim();
  });

  let z = buf.join(" ");
  assertEquals(z, "a: one b [ a b c ]");
});

Deno.test("conf - serializing array objs", () => {
  let x = {
    a: "one",
    b: [{
      a: "a",
    }, {
      b: "b",
    }, {
      c: "c",
    }],
  };
  let y = toConf(x);
  let buf = y.split("\n");
  buf.forEach(function (e, i) {
    buf[i] = e.trim();
  });

  let z = buf.join(" ");
  assertEquals(z, "a: one b [ { a: a } { b: b } { c: c } ]");
});

Deno.test("conf - serializing array arrays", () => {
  let x = {
    a: "one",
    b: [{
      a: "a",
      b: ["b", "c"],
    }, {
      b: "b",
    }, {
      c: "c",
    }],
  };
  let y = toConf(x);
  let buf = y.split("\n");
  buf.forEach(function (e, i) {
    buf[i] = e.trim();
  });

  let z = buf.join(" ");
  assertEquals(z, "a: one b [ { a: a b [ b c ] } { b: b } { c: c } ]");
});

Deno.test("conf - strings that start with numbers are quoted", () => {
  let x = {
    a: "2hello",
    b: 2,
    c: "hello",
  };
  let y = toConf(x);
  let buf = y.split("\n");
  buf.forEach(function (e, i) {
    buf[i] = e.trim();
  });

  let z = buf.join(" ");
  assertEquals(z, 'a: "2hello" b: 2 c: hello');
});

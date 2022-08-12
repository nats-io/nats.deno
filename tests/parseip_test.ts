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
// JavaScript port of go net/ip/ParseIP
// ported from https://github.com/golang/go/blob/master/src/net/ip_test.go
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
import { parseIP } from "../nats-base-client/internal_mod.ts";

import { assertEquals } from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { ipV4 } from "../nats-base-client/ipparser.ts";

Deno.test("ipparser", () => {
  const tests = [
    { t: "127.0.1.2", e: ipV4(127, 0, 1, 2) },
    { t: "127.0.0.1", e: ipV4(127, 0, 0, 1) },
    { t: "127.001.002.003", e: ipV4(127, 1, 2, 3) },
    { t: "::ffff:127.1.2.3", e: ipV4(127, 1, 2, 3) },
    { t: "::ffff:127.001.002.003", e: ipV4(127, 1, 2, 3) },
    { t: "::ffff:7f01:0203", e: ipV4(127, 1, 2, 3) },
    { t: "0:0:0:0:0000:ffff:127.1.2.3", e: ipV4(127, 1, 2, 3) },
    { t: "0:0:0:0:000000:ffff:127.1.2.3", e: ipV4(127, 1, 2, 3) },
    { t: "0:0:0:0::ffff:127.1.2.3", e: ipV4(127, 1, 2, 3) },
    {
      t: "2001:4860:0:2001::68",
      e: new Uint8Array(
        [
          0x20,
          0x01,
          0x48,
          0x60,
          0,
          0,
          0x20,
          0x01,
          0,
          0,
          0,
          0,
          0,
          0,
          0x00,
          0x68,
        ],
      ),
    },
    {
      t: "2001:4860:0000:2001:0000:0000:0000:0068",
      e: new Uint8Array(
        [
          0x20,
          0x01,
          0x48,
          0x60,
          0,
          0,
          0x20,
          0x01,
          0,
          0,
          0,
          0,
          0,
          0,
          0x00,
          0x68,
        ],
      ),
    },

    { t: "-0.0.0.0", e: undefined },
    { t: "0.-1.0.0", e: undefined },
    { t: "0.0.-2.0", e: undefined },
    { t: "0.0.0.-3", e: undefined },
    { t: "127.0.0.256", e: undefined },
    { t: "abc", e: undefined },
    { t: "123:", e: undefined },
    { t: "fe80::1%lo0", e: undefined },
    { t: "fe80::1%911", e: undefined },
    { t: "", e: undefined },
    { t: "a1:a2:a3:a4::b1:b2:b3:b4", e: undefined }, // Issue 6628
  ];

  tests.forEach((tc) => {
    assertEquals(parseIP(tc.t), tc.e, tc.t);
  });
});

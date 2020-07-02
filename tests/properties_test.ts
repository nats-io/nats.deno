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
import {
  assertEquals,
  assertMatch,
  assert,
} from "https://deno.land/std/testing/asserts.ts";

import { connect } from "../src/mod.ts";
import { DenoTransport } from "../src/deno_transport.ts";
import {
  ConnectionOptions,
  Payload,
  Connect,
} from "../nats-base-client/mod.ts";

const { version, lang } = new DenoTransport();

Deno.test("VERSION is semver", () => {
  assertMatch(version, /[0-9]+\.[0-9]+\.[0-9]+/);
});

Deno.test("connect is a function", () => {
  assert(typeof connect === "function");
});

Deno.test("default connect properties", () => {
  let c = new Connect(
    { version, lang },
    { url: "nats://127.0.0.1:4222" } as ConnectionOptions,
  );
  assertEquals(c.lang, lang);
  assert(c.version);
  assertEquals(c.verbose, false);
  assertEquals(c.pedantic, false);
  assertEquals(c.protocol, 1);
  assertEquals(c.user, undefined);
  assertEquals(c.pass, undefined);
  assertEquals(c.auth_token, undefined);
  assertEquals(c.name, undefined);
});

Deno.test("configured options", () => {
  let opts = {} as ConnectionOptions;
  opts.url = "nats://127.0.0.1:4222";
  opts.payload = Payload.BINARY;
  opts.name = "test";
  opts.pass = "secret";
  opts.user = "me";
  opts.token = "abc";
  opts.pedantic = true;
  opts.verbose = true;

  let c = new Connect({ version, lang }, opts);
  assertEquals(c.verbose, opts.verbose);
  assertEquals(c.pedantic, opts.pedantic);
  assertEquals(c.name, opts.name);
  assertEquals(c.user, opts.user);
  assertEquals(c.pass, opts.pass);
  assertEquals(c.auth_token, opts.token);
});

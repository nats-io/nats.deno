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
} from "https://deno.land/std@0.63.0/testing/asserts.ts";

import { connect } from "../src/mod.ts";
import { DenoTransport } from "../src/deno_transport.ts";
import {
  Connect,
} from "../nats-base-client/internal_mod.ts";
import { buildAuthenticator } from "../nats-base-client/authenticator.ts";
import { extend } from "../nats-base-client/util.ts";
import { defaultOptions } from "../nats-base-client/options.ts";

const { version, lang } = new DenoTransport();

Deno.test("properties - VERSION is semver", () => {
  assertMatch(version, /[0-9]+\.[0-9]+\.[0-9]+/);
});

Deno.test("properties - connect is a function", () => {
  assert(typeof connect === "function");
});

Deno.test("properties - default connect properties", () => {
  const opts = defaultOptions();
  opts.url = "nats://127.0.0.1:4222";
  const c = new Connect(
    { version, lang },
    opts,
  );
  const cc = JSON.parse(JSON.stringify(c));

  assertEquals(cc.lang, lang, "lang");
  assert(cc.version);
  assertEquals(cc.verbose, false, "verbose");
  assertEquals(cc.pedantic, false, "pedantic");
  assertEquals(cc.protocol, 1, "protocol");
  assertEquals(cc.user, undefined, "user");
  assertEquals(cc.pass, undefined, "pass");
  assertEquals(cc.auth_token, undefined, "auth_token");
  assertEquals(cc.name, undefined, "name");
});

Deno.test("properties - configured options", async () => {
  let opts = defaultOptions();
  opts.url = "nats://127.0.0.1:4222";
  opts.name = "test";
  opts.pass = "secret";
  opts.user = "me";
  opts.token = "abc";
  opts.pedantic = true;
  opts.verbose = true;
  // simulate the authenticator - as 'token' is mapped to 'auth_token'
  opts.authenticator = buildAuthenticator(opts);
  const auth = await opts.authenticator();
  opts = extend(opts, auth);

  const c = new Connect({ version, lang }, opts);
  const cc = JSON.parse(JSON.stringify(c));

  assertEquals(cc.verbose, opts.verbose);
  assertEquals(cc.pedantic, opts.pedantic);
  assertEquals(cc.name, opts.name);
  assertEquals(cc.user, opts.user);
  assertEquals(cc.pass, opts.pass);
  assertEquals(cc.auth_token, opts.token);
});

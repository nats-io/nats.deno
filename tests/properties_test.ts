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
  assert,
  assertEquals,
  assertMatch,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";

import { connect, ConnectionOptions } from "../src/mod.ts";
import { DenoTransport } from "../src/deno_transport.ts";
import { Connect } from "../nats-base-client/protocol.ts";
import { buildAuthenticator } from "../nats-base-client/authenticator.ts";
import { extend } from "../nats-base-client/util.ts";
import { defaultOptions, parseOptions } from "../nats-base-client/options.ts";
import { Servers } from "../nats-base-client/servers.ts";

const { version, lang } = new DenoTransport();

Deno.test("properties - VERSION is semver", () => {
  assertMatch(version, /[0-9]+\.[0-9]+\.[0-9]+/);
});

Deno.test("properties - connect is a function", () => {
  assert(typeof connect === "function");
});

Deno.test("properties - default connect properties", () => {
  const opts = defaultOptions();
  opts.servers = "127.0.0.1:4222";
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

Deno.test("properties - configured token options", async () => {
  let opts = defaultOptions();
  opts.servers = "127.0.0.1:4222";
  opts.name = "test";
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
  assertEquals(cc.user, undefined);
  assertEquals(cc.pass, undefined);
  assertEquals(cc.auth_token, opts.token);
});

Deno.test("properties - configured user/pass options", async () => {
  let opts = defaultOptions();
  opts.servers = "127.0.0.1:4222";
  opts.user = "test";
  opts.pass = "secret";
  // simulate the autheticator
  opts.authenticator = buildAuthenticator(opts);
  const auth = await opts.authenticator();
  opts = extend(opts, auth);

  const c = new Connect({ version, lang }, opts);
  const cc = JSON.parse(JSON.stringify(c));

  assertEquals(cc.user, opts.user);
  assertEquals(cc.pass, opts.pass);
  assertEquals(cc.auth_token, undefined);
});

Deno.test("properties - tls doesn't leak options", () => {
  const tlsOptions = {
    keyFile: "keyFile",
    certFile: "certFile",
    caFile: "caFile",
    key: "key",
    cert: "cert",
    ca: "ca",
  };

  let opts = { tls: tlsOptions, cert: "another" };
  const auth = buildAuthenticator(opts);
  opts = extend(opts, auth);

  const c = new Connect({ version: "1.2.3", lang: "test" }, opts);
  const cc = JSON.parse(JSON.stringify(c));
  assertEquals(cc.tls_required, true);
  assertEquals(cc.cert, undefined);
  assertEquals(cc.keyFile, undefined);
  assertEquals(cc.certFile, undefined);
  assertEquals(cc.caFile, undefined);
  assertEquals(cc.tls, undefined);
});

Deno.test("properties - port is only honored if no servers provided", () => {
  type test = { opts?: ConnectionOptions; expected: string };
  const buf: test[] = [];
  buf.push({ expected: "127.0.0.1:4222" });
  buf.push({ opts: {}, expected: "127.0.0.1:4222" });
  buf.push({ opts: { port: 9999 }, expected: "127.0.0.1:9999" });

  buf.forEach((t) => {
    const opts = parseOptions(t.opts);
    const servers = new Servers(opts.servers as string[], {});
    const cs = servers.getCurrentServer();
    assertEquals(cs.listen, t.expected);
  });
});

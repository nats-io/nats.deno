/*
 * Copyright 2018-2023 The NATS Authors
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
  assertExists,
  assertMatch,
} from "@std/assert";

import { Authenticator, connect, ConnectionOptions } from "../src/mod.ts";
import { DenoTransport } from "../src/deno_transport.ts";
import { Connect } from "../nats-base-client/protocol.ts";
import { credsAuthenticator } from "../nats-base-client/authenticator.ts";
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

Deno.test("properties - configured token options", () => {
  let opts = defaultOptions();
  opts.servers = "127.0.0.1:4222";
  opts.name = "test";
  opts.token = "abc";
  opts.pedantic = true;
  opts.verbose = true;

  opts = parseOptions(opts);
  assertExists(opts.authenticator);
  const fn = opts.authenticator as Authenticator;
  const auth = fn();
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

Deno.test("properties - configured user/pass options", () => {
  let opts = defaultOptions();
  opts.servers = "127.0.0.1:4222";
  opts.user = "test";
  opts.pass = "secret";
  opts = parseOptions(opts);
  assertExists(opts.authenticator);
  const fn = opts.authenticator as Authenticator;
  const auth = fn();
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

  const opts = { tls: tlsOptions, cert: "another" };
  let po = parseOptions(opts);
  assertExists(po.authenticator);
  const fn = po.authenticator as Authenticator;
  const auth = fn() || {};
  po = extend(po, auth);

  const c = new Connect({ version: "1.2.3", lang: "test" }, po);
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

Deno.test("properties - multi", () => {
  const creds = `-----BEGIN NATS USER JWT-----
    eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJFU1VQS1NSNFhGR0pLN0FHUk5ZRjc0STVQNTZHMkFGWERYQ01CUUdHSklKUEVNUVhMSDJBIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJBQ1pTV0JKNFNZSUxLN1FWREVMTzY0VlgzRUZXQjZDWENQTUVCVUtBMzZNSkpRUlBYR0VFUTJXSiIsInN1YiI6IlVBSDQyVUc2UFY1NTJQNVNXTFdUQlAzSDNTNUJIQVZDTzJJRUtFWFVBTkpYUjc1SjYzUlE1V002IiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ.kCR9Erm9zzux4G6M-V2bp7wKMKgnSNqMBACX05nwePRWQa37aO_yObbhcJWFGYjo1Ix-oepOkoyVLxOJeuD8Bw
  ------END NATS USER JWT------

************************* IMPORTANT *************************
  NKEY Seed printed below can be used sign and prove identity.
    NKEYs are sensitive and should be treated as secrets.

  -----BEGIN USER NKEY SEED-----
    SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4
  ------END USER NKEY SEED------
`;

  const authenticator = credsAuthenticator(new TextEncoder().encode(creds));

  let opts = defaultOptions();
  opts.servers = "127.0.0.1:4222";
  opts.user = "test";
  opts.pass = "secret";
  opts.token = "mytoken";
  opts.authenticator = authenticator;

  opts = parseOptions(opts);
  assertExists(opts.authenticator);

  const c = new Connect({ version, lang }, opts, "hello");
  const cc = JSON.parse(JSON.stringify(c));

  assertEquals(cc.user, opts.user);
  assertEquals(cc.pass, opts.pass);
  assertEquals(cc.auth_token, opts.token);
  assertExists(cc.jwt);
  assertExists(cc.sig);
  assert(cc.sig.length > 0);
  assertExists(cc.nkey);
});

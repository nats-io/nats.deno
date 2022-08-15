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
  assertEquals,
  fail,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { connect, ErrorCode } from "../src/mod.ts";
import { assertErrorCode, Lock, NatsServer } from "./helpers/mod.ts";

import { join, resolve } from "https://deno.land/std@0.152.0/path/mod.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";
import { cleanup } from "./jstest_util.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";

Deno.test("tls - fail if server doesn't support TLS", async () => {
  const ns = await NatsServer.start();
  const lock = Lock();
  await connect({ port: ns.port, tls: {} })
    .then(() => {
      fail("shouldn't have connected");
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.ServerOptionNotAvailable);
      lock.unlock();
    });
  await lock;
  await ns.stop();
});

Deno.test("tls - connects to tls without option", async () => {
  const nc = await connect({ servers: "demo.nats.io:4443" });
  await nc.flush();
  await nc.close();
});

Deno.test("tls - custom ca fails without root", async () => {
  const cwd = Deno.cwd();
  const config = {
    host: "0.0.0.0",
    tls: {
      cert_file: resolve(join(cwd, "./tests/certs/localhost.crt")),
      key_file: resolve(join(cwd, "./tests/certs/localhost.key")),
      ca_file: resolve(join(cwd, "./tests/certs/RootCA.crt")),
    },
  };

  const ns = await NatsServer.start(config);
  const lock = Lock();
  await connect({ servers: `localhost:${ns.port}` })
    .then(() => {
      fail("shouldn't have connected without client ca");
    })
    .catch((err) => {
      // this is a bogus error name - but at least we know we are rejected
      assertEquals(err.name, "InvalidData");
      assertEquals(
        err.message,
        "invalid peer certificate contents: invalid peer certificate: UnknownIssuer",
      );
      lock.unlock();
    });

  await lock;
  await ns.stop();
});

Deno.test("tls - custom ca with root connects", async () => {
  const cwd = Deno.cwd();
  const config = {
    host: "0.0.0.0",
    tls: {
      cert_file: resolve(join(cwd, "./tests/certs/localhost.crt")),
      key_file: resolve(join(cwd, "./tests/certs/localhost.key")),
      ca_file: resolve(join(cwd, "./tests/certs/RootCA.crt")),
    },
  };

  const ns = await NatsServer.start(config);
  const nc = await connect({
    servers: `localhost:${ns.port}`,
    tls: {
      caFile: config.tls.ca_file,
    },
  });
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("tls - available connects with or without", async () => {
  const cwd = Deno.cwd();
  const config = {
    host: "0.0.0.0",
    allow_non_tls: true,
    tls: {
      cert_file: resolve(join(cwd, "./tests/certs/localhost.crt")),
      key_file: resolve(join(cwd, "./tests/certs/localhost.key")),
      ca_file: resolve(join(cwd, "./tests/certs/RootCA.crt")),
    },
  };

  const ns = await NatsServer.start(config);
  // will upgrade to tls but fail in the test because the
  // certificate will not be trusted
  await assertRejects(async () => {
    await connect({
      servers: `localhost:${ns.port}`,
    });
  });

  // will upgrade to tls as tls is required
  const a = connect({
    servers: `localhost:${ns.port}`,
    tls: {
      caFile: config.tls.ca_file,
    },
  });
  // will NOT upgrade to tls
  const b = connect({
    servers: `localhost:${ns.port}`,
    tls: null,
  });
  const conns = await Promise.all([a, b]) as NatsConnectionImpl[];
  await conns[0].flush();
  await conns[1].flush();

  assertEquals(conns[0].protocol.transport.isEncrypted(), true);
  assertEquals(conns[1].protocol.transport.isEncrypted(), false);

  await cleanup(ns, ...conns);
});

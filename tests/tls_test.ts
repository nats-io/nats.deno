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
} from "https://deno.land/std@0.80.0/testing/asserts.ts";
import { connect, ErrorCode } from "../src/mod.ts";
import { assertErrorCode, Lock, NatsServer } from "./helpers/mod.ts";

import { join, resolve } from "https://deno.land/std@0.80.0/path/mod.ts";

Deno.test("tls - fail if server doesn't support TLS", async () => {
  const lock = Lock();
  await connect({ servers: "demo.nats.io:4222", tls: {} })
    .then(() => {
      fail("shouldn't have connected");
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.SERVER_OPTION_NA);
      lock.unlock();
    });
  await lock;
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

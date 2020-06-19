/*
 * Copyright 2018-2020 The NATS Authors
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
  fail,
} from "https://deno.land/std/testing/asserts.ts";
import {
  connect,
  ErrorCode,
} from "../src/mod.ts";
import {
  assertErrorCode,
  Lock,
  NatsServer,
} from "./helpers/mod.ts";

const conf = {
  authorization: {
    PERM: {
      subscribe: "bar",
      publish: "foo",
    },
    users: [{
      user: "derek",
      password: "foobar",
      permission: "$PERM",
    }],
  },
};

Deno.test("auth none", async () => {
  const ns = await NatsServer.start(conf);
  try {
    const nc = await connect(
      { url: `nats://localhost:${ns.port}` },
    );
    await nc.close();
    fail("shouldnt have been able to connect");
  } catch (ex) {
    assertErrorCode(ex, ErrorCode.AUTHORIZATION_VIOLATION);
  }
  await ns.stop();
});

Deno.test("auth bad", async () => {
  const ns = await NatsServer.start(conf);
  try {
    const nc = await connect(
      { url: `nats://localhost:${ns.port}`, user: "me", pass: "hello" },
    );
    await nc.close();
    fail("shouldnt have been able to connect");
  } catch (ex) {
    assertErrorCode(ex, ErrorCode.AUTHORIZATION_VIOLATION);
  }
  await ns.stop();
});

Deno.test("auth", async () => {
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    { url: `nats://localhost:${ns.port}`, user: "derek", pass: "foobar" },
  );
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth cannot sub to foo", async () => {
  const ns = await NatsServer.start(conf);
  const lock = Lock();
  const nc = await connect(
    { url: `nats://localhost:${ns.port}`, user: "derek", pass: "foobar" },
  );
  nc.addEventListener("error", (err) => {
    assertErrorCode(err, ErrorCode.PERMISSIONS_VIOLATION);
    lock.unlock();
  });

  nc.subscribe("foo", () => {
    fail("should not have called message handler");
  });

  nc.publish("foo");
  nc.flush();

  await lock;
  await nc.close();
  await ns.stop();
});

Deno.test("auth cannot pub bar", async () => {
  const ns = await NatsServer.start(conf);
  const lock = Lock();
  const nc = await connect(
    { url: `nats://localhost:${ns.port}`, user: "derek", pass: "foobar" },
  );
  nc.addEventListener("error", (err) => {
    assertErrorCode(err, ErrorCode.PERMISSIONS_VIOLATION);
    lock.unlock();
  });

  nc.subscribe("bar", () => {
    fail("should not have been called");
  });

  nc.publish("bar");
  nc.flush();

  await lock;
  await nc.close();
  await ns.stop();
});

Deno.test("auth no user and token", async () => {
  connect({ url: "nats://localhost:4222", user: "derek", token: "foobar" })
    .then(async (nc) => {
      await nc.close();
      fail("should not have connected");
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.BAD_AUTHENTICATION);
    });
});

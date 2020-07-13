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
} from "https://deno.land/std@0.61.0/testing/asserts.ts";
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

Deno.test("auth - none", async () => {
  const ns = await NatsServer.start(conf);
  try {
    const nc = await connect(
      { port: ns.port },
    );
    await nc.close();
    fail("shouldnt have been able to connect");
  } catch (ex) {
    assertErrorCode(ex, ErrorCode.AUTHORIZATION_VIOLATION);
  }
  await ns.stop();
});

Deno.test("auth - bad", async () => {
  const ns = await NatsServer.start(conf);
  try {
    const nc = await connect(
      { port: ns.port, user: "me", pass: "hello" },
    );
    await nc.close();
    fail("shouldnt have been able to connect");
  } catch (ex) {
    assertErrorCode(ex, ErrorCode.AUTHORIZATION_VIOLATION);
  }
  await ns.stop();
});

Deno.test("auth - un/pw", async () => {
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    { port: ns.port, user: "derek", pass: "foobar" },
  );
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - sub permissions", async () => {
  const ns = await NatsServer.start(conf);
  const lock = Lock(2);
  const nc = await connect(
    { port: ns.port, user: "derek", pass: "foobar" },
  );
  nc.closed().then((err) => {
    assertErrorCode(err as Error, ErrorCode.PERMISSIONS_VIOLATION);
    lock.unlock();
  });

  nc.subscribe("foo", {
    callback: (err, msg) => {
      lock.unlock();
      assertErrorCode(err as Error, ErrorCode.PERMISSIONS_VIOLATION);
    },
  });

  nc.publish("foo");

  await lock;
  await ns.stop();
});

Deno.test("auth - pub perm", async () => {
  const ns = await NatsServer.start(conf);
  const lock = Lock();
  const nc = await connect(
    { port: ns.port, user: "derek", pass: "foobar" },
  );
  nc.closed().then((err) => {
    assertErrorCode(err as Error, ErrorCode.PERMISSIONS_VIOLATION);
    lock.unlock();
  });

  nc.subscribe("bar", {
    callback: () => {
      fail("should not have been called");
    },
  });

  nc.publish("bar");

  await lock;
  await ns.stop();
});

Deno.test("auth - no user and token", async () => {
  connect({ url: "nats://127.0.0.1:4222", user: "derek", token: "foobar" })
    .then(async (nc) => {
      await nc.close();
      fail("should not have connected");
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.BAD_AUTHENTICATION);
    });
});

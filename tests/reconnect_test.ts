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
 *
 */
import {
  assert,
  assertEquals,
  fail,
} from "https://deno.land/std/testing/asserts.ts";
import {
  connect,
  ErrorCode,
  Events,
} from "../src/mod.ts";
import {
  assertErrorCode,
  Lock,
  NatsServer,
} from "./helpers/mod.ts";
import { nuid } from "../nats-base-client/nats.ts";
import { DebugEvents } from "../nats-base-client/types.ts";

Deno.test("reconnect - should receive when some servers are invalid", async () => {
  const lock = Lock(1);
  const servers = ["nats://127.0.0.1:7", "demo.nats.io:4222"];
  const nc = await connect({ servers: servers, noRandomize: true });
  const subj = nuid.next();
  await nc.subscribe(subj, (err, msg) => {
    lock.unlock();
  });
  nc.publish(subj);
  await lock;
  await nc.close();
  // @ts-ignore
  const a = nc.protocol.servers.getServers();
  assertEquals(a.length, 1);
  assert(a[0].didConnect);
});

Deno.test("reconnect - events", async () => {
  const srv = await NatsServer.start();

  let nc = await connect({
    port: srv.port,
    waitOnFirstConnect: true,
    reconnectTimeWait: 100,
    maxReconnectAttempts: 10,
  });

  let disconnects = 0;
  nc.addEventListener(Events.DISCONNECT, () => {
    disconnects++;
  });

  let reconnecting = 0;
  nc.addEventListener(DebugEvents.RECONNECTING, () => {
    reconnecting++;
  });
  await srv.stop();
  try {
    await nc.status();
  } catch (err) {
    assertErrorCode(err, ErrorCode.CONNECTION_REFUSED);
  }
  assertEquals(disconnects, 1);
  assertEquals(reconnecting, 10);
});

Deno.test("reconnect - reconnect not emitted if suppressed", async () => {
  const srv = await NatsServer.start();
  let nc = await connect({
    port: srv.port,
    reconnect: false,
  });

  let disconnects = 0;
  nc.addEventListener(Events.DISCONNECT, () => {
    disconnects++;
  });

  nc.addEventListener(DebugEvents.RECONNECTING, () => {
    fail("shouldn't have emitted reconnecting");
  });

  await srv.stop();
  await nc.status();
});

Deno.test("reconnect - reconnecting after proper delay", async () => {
  const srv = await NatsServer.start();
  let nc = await connect({
    port: srv.port,
    reconnectTimeWait: 500,
    maxReconnectAttempts: 1,
  });
  // @ts-ignore
  const serverLastConnect = nc.protocol.servers.getCurrentServer().lastConnect;
  await srv.stop();

  nc.addEventListener(DebugEvents.RECONNECTING, () => {
    const elapsed = Date.now() - serverLastConnect;
    assert(elapsed >= 500 && elapsed <= 600);
  });

  await nc.status();
});

Deno.test("reconnect - indefinite reconnects", async () => {
  let srv = await NatsServer.start();

  let nc = await connect({
    port: srv.port,
    reconnectTimeWait: 100,
    maxReconnectAttempts: -1,
  });

  let disconnects = 0;
  nc.addEventListener(Events.DISCONNECT, () => {
    disconnects++;
  });

  let reconnects = 0;
  nc.addEventListener(DebugEvents.RECONNECTING, () => {
    reconnects++;
  });

  let reconnect = false;
  nc.addEventListener(Events.RECONNECT, () => {
    reconnect = true;
    nc.close();
  });

  await srv.stop();

  const lock = Lock(1);
  setTimeout(async () => {
    srv = await srv.restart();
    lock.unlock();
  }, 1000);

  await nc.status();
  await srv.stop();
  await lock;
  await srv.stop();
  assert(reconnects > 5);
  assert(reconnect);
  assertEquals(disconnects, 1);
});

Deno.test("reconnect - jitter", async () => {
  let srv = await NatsServer.start();

  let called = false;
  const h = () => {
    called = true;
    return 15;
  };

  let hasDefaultFn = false;
  let dc = await connect({
    port: srv.port,
    reconnect: false,
  });
  hasDefaultFn = typeof dc.options.reconnectDelayHandler === "function";

  let nc = await connect({
    port: srv.port,
    maxReconnectAttempts: 1,
    reconnectDelayHandler: h,
  });

  await srv.stop();
  await nc.status();
  await dc.status();
  assert(called);
  assert(hasDefaultFn);
});

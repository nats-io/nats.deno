/*
 * Copyright 2018-2021 The NATS Authors
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
} from "https://deno.land/std@0.80.0/testing/asserts.ts";
import { connect, createInbox, ErrorCode, Events } from "../src/mod.ts";
import { assertErrorCode, Lock, NatsServer } from "./helpers/mod.ts";
import {
  DebugEvents,
  deferred,
  NatsConnectionImpl,
} from "../nats-base-client/internal_mod.ts";

Deno.test("reconnect - should receive when some servers are invalid", async () => {
  const lock = Lock(1);
  const servers = ["127.0.0.1:7", "demo.nats.io:4222"];
  const nc = await connect(
    { servers: servers, noRandomize: true },
  ) as NatsConnectionImpl;
  const subj = createInbox();
  await nc.subscribe(subj, {
    callback: () => {
      lock.unlock();
    },
  });
  nc.publish(subj);
  await lock;
  await nc.close();
  const a = nc.protocol.servers.getServers();
  assertEquals(a.length, 1);
  assert(a[0].didConnect);
});

Deno.test("reconnect - events", async () => {
  const srv = await NatsServer.start();

  const nc = await connect({
    port: srv.port,
    waitOnFirstConnect: true,
    reconnectTimeWait: 100,
    maxReconnectAttempts: 10,
  });

  let disconnects = 0;
  let reconnecting = 0;

  (async () => {
    for await (const e of nc.status()) {
      switch (e.type) {
        case Events.DISCONNECT:
          disconnects++;
          break;
        case DebugEvents.RECONNECTING:
          reconnecting++;
          break;
      }
    }
  })().then();
  await srv.stop();
  try {
    await nc.closed();
  } catch (err) {
    assertErrorCode(err, ErrorCode.CONNECTION_REFUSED);
  }
  assertEquals(disconnects, 1);
  assertEquals(reconnecting, 10);
});

Deno.test("reconnect - reconnect not emitted if suppressed", async () => {
  const srv = await NatsServer.start();
  const nc = await connect({
    port: srv.port,
    reconnect: false,
  });

  let disconnects = 0;
  (async () => {
    for await (const e of nc.status()) {
      switch (e.type) {
        case Events.DISCONNECT:
          disconnects++;
          break;
        case DebugEvents.RECONNECTING:
          fail("shouldn't have emitted reconnecting");
          break;
      }
    }
  })().then();

  await srv.stop();
  await nc.closed();
});

Deno.test("reconnect - reconnecting after proper delay", async () => {
  const srv = await NatsServer.start();
  const nc = await connect({
    port: srv.port,
    reconnectTimeWait: 500,
    maxReconnectAttempts: 1,
  }) as NatsConnectionImpl;
  const serverLastConnect = nc.protocol.servers.getCurrentServer().lastConnect;

  const dt = deferred<number>();
  (async () => {
    for await (const e of nc.status()) {
      switch (e.type) {
        case DebugEvents.RECONNECTING: {
          const elapsed = Date.now() - serverLastConnect;
          dt.resolve(elapsed);
          break;
        }
      }
    }
  })().then();
  await srv.stop();
  const elapsed = await dt;
  assert(elapsed >= 500 && elapsed <= 700, `elapsed was ${elapsed}`);
  await nc.closed();
});

Deno.test("reconnect - indefinite reconnects", async () => {
  let srv = await NatsServer.start();

  const nc = await connect({
    port: srv.port,
    reconnectTimeWait: 100,
    maxReconnectAttempts: -1,
  });

  let disconnects = 0;
  let reconnects = 0;
  let reconnect = false;
  (async () => {
    for await (const e of nc.status()) {
      switch (e.type) {
        case Events.DISCONNECT:
          disconnects++;
          break;
        case Events.RECONNECT:
          reconnect = true;
          nc.close();
          break;
        case DebugEvents.RECONNECTING:
          reconnects++;
          break;
      }
    }
  })().then();

  await srv.stop();

  const lock = Lock(1);
  setTimeout(async () => {
    srv = await srv.restart();
    lock.unlock();
  }, 1000);

  await nc.closed();
  await srv.stop();
  await lock;
  await srv.stop();
  assert(reconnects > 5);
  assert(reconnect);
  assertEquals(disconnects, 1);
});

Deno.test("reconnect - jitter", async () => {
  const srv = await NatsServer.start();

  let called = false;
  const h = () => {
    called = true;
    return 15;
  };

  const dc = await connect({
    port: srv.port,
    reconnect: false,
  }) as NatsConnectionImpl;
  const hasDefaultFn = typeof dc.options.reconnectDelayHandler === "function";

  const nc = await connect({
    port: srv.port,
    maxReconnectAttempts: 1,
    reconnectDelayHandler: h,
  });

  await srv.stop();
  await nc.closed();
  await dc.closed();
  assert(called);
  assert(hasDefaultFn);
});

Deno.test("reconnect - internal disconnect forces reconnect", async () => {
  const srv = await NatsServer.start();
  const nc = await connect({
    port: srv.port,
    reconnect: true,
    reconnectTimeWait: 200,
  }) as NatsConnectionImpl;

  let stale = false;
  let disconnect = false;
  const lock = Lock();
  (async () => {
    for await (const e of nc.status()) {
      switch (e.type) {
        case DebugEvents.STALE_CONNECTION:
          stale = true;
          break;
        case Events.DISCONNECT:
          disconnect = true;
          break;
        case Events.RECONNECT:
          lock.unlock();
          break;
      }
    }
  })().then();

  nc.protocol.disconnect();
  await lock;

  assert(disconnect, "disconnect");
  assert(stale, "stale");

  await nc.close();
  await srv.stop();
});

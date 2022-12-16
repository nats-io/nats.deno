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
} from "https://deno.land/std@0.168.0/testing/asserts.ts";
import {
  connect,
  createInbox,
  ErrorCode,
  Events,
  NatsError,
} from "../src/mod.ts";
import { assertErrorCode, Lock, NatsServer } from "./helpers/mod.ts";
import {
  DataBuffer,
  DebugEvents,
  deferred,
  delay,
  NatsConnectionImpl,
} from "../nats-base-client/internal_mod.ts";
import { cleanup, setup } from "./jstest_util.ts";
import { deadline } from "https://deno.land/std@0.168.0/async/deadline.ts";
import Conn = Deno.Conn;

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
        case Events.Disconnect:
          disconnects++;
          break;
        case DebugEvents.Reconnecting:
          reconnecting++;
          break;
      }
    }
  })().then();
  await srv.stop();
  try {
    await nc.closed();
  } catch (err) {
    assertErrorCode(err, ErrorCode.ConnectionRefused);
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
        case Events.Disconnect:
          disconnects++;
          break;
        case DebugEvents.Reconnecting:
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
        case DebugEvents.Reconnecting: {
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
        case Events.Disconnect:
          disconnects++;
          break;
        case Events.Reconnect:
          reconnect = true;
          nc.close().then().catch();
          break;
        case DebugEvents.Reconnecting:
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
        case DebugEvents.StaleConnection:
          stale = true;
          break;
        case Events.Disconnect:
          disconnect = true;
          break;
        case Events.Reconnect:
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

Deno.test("reconnect - wait on first connect", async () => {
  let srv = await NatsServer.start({});
  const port = srv.port;
  await delay(500);
  await srv.stop();
  await delay(1000);
  const pnc = connect({
    port: port,
    waitOnFirstConnect: true,
    reconnectTimeWait: 100,
    maxReconnectAttempts: 10,
  });
  await delay(3000);
  srv = await srv.restart();

  const nc = await pnc;
  const subj = createInbox();
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      msg.respond();
    },
  });
  await nc.request(subj);

  // stop the server
  await srv.stop();
  // no reconnect, will quit the client
  const what = await nc.closed() as NatsError;
  assertEquals(what.code, ErrorCode.ConnectionRefused);
});

Deno.test("reconnect - wait on first connect off", async () => {
  const srv = await NatsServer.start({});
  const port = srv.port;
  await delay(500);
  await srv.stop();
  await delay(1000);
  const pnc = connect({
    port: port,
  });

  try {
    // should fail
    await pnc;
  } catch (err) {
    const nerr = err as NatsError;
    assertEquals(nerr.code, ErrorCode.ConnectionRefused);
  }
});

Deno.test("reconnect - close stops reconnects", async () => {
  const { ns, nc } = await setup({}, {
    maxReconnectAttempts: -1,
    reconnectTimeWait: 500,
  });
  const reconnects = deferred();
  (async () => {
    let c = 0;
    for await (const s of nc.status()) {
      if (s.type === DebugEvents.Reconnecting) {
        c++;
        if (c === 5) {
          reconnects.resolve();
        }
      }
    }
  })().then();

  setTimeout(() => {
    ns.stop();
  }, 1000);

  await reconnects;
  await deadline(nc.close(), 5000)
    .catch((err) => {
      // the promise will reject if deadline exceeds
      fail(err);
    });
});

Deno.test("reconnect - stale connections don't close", async () => {
  const listener = Deno.listen({ port: 0, transport: "tcp" });
  const { port } = listener.addr as Deno.NetAddr;
  const connections: Conn[] = [];

  const TE = new TextEncoder();

  const INFO = TE.encode(
    "INFO " + JSON.stringify({
      server_id: "TEST",
      version: "0.0.0",
      host: "127.0.0.1",
      port: port,
    }) + "\r\n",
  );

  const PING = { re: /^PING\r\n/im, out: TE.encode("PONG\r\n") };
  const CONNECT = { re: /^CONNECT\s+([^\r\n]+)\r\n/im, out: TE.encode("") };
  const CMDS = [PING, CONNECT];

  const connClosed: Promise<void>[] = [];

  const startReading = (conn: Conn) => {
    const buf = new Uint8Array(1024 * 8);
    const inbound = new DataBuffer();
    (async () => {
      const closed = deferred<void>();
      connClosed.push(closed);
      while (true) {
        const count = await conn.read(buf);
        if (count === null) {
          closed.resolve();
          break;
        }
        if (count) {
          inbound.fill(DataBuffer.concat(buf.subarray(0, count)));
          const lines = DataBuffer.toAscii(inbound.peek());
          for (let i = 0; i < CMDS.length; i++) {
            const m = CMDS[i].re.exec(lines);
            if (m) {
              const len = m[0].length;
              if (len) {
                inbound.drain(len);
                await conn.write(CMDS[i].out);
              }
              if (i === 0) {
                // sent the PONG we are done.
                return;
              }
            }
          }
        }
      }
    })();
  };

  (async () => {
    for await (const conn of listener) {
      connections.push(conn);
      try {
        await conn.write(INFO);
        startReading(conn);
      } catch (_err) {
        console.log(_err);
        return;
      }
    }
  })().then();

  const nc = await connect({
    port,
    maxReconnectAttempts: -1,
    pingInterval: 2000,
    reconnectTimeWait: 500,
    ignoreAuthErrorAbort: true,
  });

  let stales = 0;
  (async () => {
    for await (const s of nc.status()) {
      console.log(s);
      if (s.type === DebugEvents.StaleConnection) {
        stales++;
        if (stales === 3) {
          await nc.close();
        }
      }
    }
  })().then();

  await nc.closed();
  connections.forEach((c) => {
    return c.close();
  });
  listener.close();
  await Promise.all(connClosed);
  assert(stales >= 3, `stales ${stales}`);
});

Deno.test("reconnect - protocol errors don't close client", async () => {
  const { ns, nc } = await setup({}, {
    maxReconnectAttempts: -1,
    reconnectTimeWait: 500,
  });
  const nci = nc as NatsConnectionImpl;

  let reconnects = 0;
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === Events.Reconnect) {
        reconnects++;
        if (reconnects < 3) {
          setTimeout(() => {
            nci.protocol.sendCommand(`X\r\n`);
          });
        }
        if (reconnects === 3) {
          await nc.close();
        }
      }
    }
  })().then();

  nci.protocol.sendCommand(`X\r\n`);

  const err = await nc.closed();
  assertEquals(err, undefined);

  await cleanup(ns, nc);
});

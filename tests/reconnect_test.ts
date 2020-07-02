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
  const lock = Lock(1, 5000);
  const servers = ["nats://localhost:7", "demo.nats.io:4222"];
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

Deno.test("reconnect events", async () => {
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
//
// test('reconnect not emitted if suppressed', async (t) => {
//   t.plan(2);
//   let lock = new Lock();
//
//   let server = await startServer();
//   registerServer(server, t);
//
//   let nc = await connect({
//     url: server.nats,
//     reconnect: false
//   });
//
//   nc.on('connect', () => {
//     setTimeout(() => {
//       stopServer(server);
//     }, 100);
//   });
//
//   let disconnects = 0;
//   nc.on('disconnect', () => {
//     disconnects++;
//   });
//
//   let reconnecting = false;
//   nc.on('reconnecting', () => {
//     reconnecting = true;
//   });
//
//   nc.on('close', () => {
//     t.is(disconnects, 1);
//     t.false(reconnecting);
//     lock.unlock();
//   });
//
//   return lock.latch;
// });
//
// test('reconnecting after proper delay', async (t) => {
//   t.plan(2);
//   let lock = new Lock();
//
//   let server = await startServer();
//   registerServer(server, t);
//   let nc = await connect({
//     url: server.nats,
//     reconnectTimeWait: 500,
//     maxReconnectAttempts: 1
//   });
//
//   let serverLastConnect = 0;
//   nc.on('connect', () => {
//     //@ts-ignore
//     serverLastConnect = nc.protocolHandler.servers.getCurrentServer().lastConnect;
//     setTimeout(() => {
//       stopServer(server);
//     }, 100);
//   });
//
//   let disconnect = 0;
//   nc.on('disconnect', () => {
//     disconnect = Date.now();
//   });
//
//   nc.on('reconnecting', () => {
//     let elapsed = Date.now() - serverLastConnect;
//     t.true(elapsed >= 485);
//     t.true(elapsed <= 600);
//     nc.close();
//     lock.unlock();
//   });
//
//   return lock.latch;
// });
//
// test('indefinite reconnects', async (t) => {
//   let lock = new Lock();
//   t.plan(3);
//
//   let server: Server | null;
//   server = await startServer();
//   registerServer(server, t);
//
//   let u = new url.URL(server.nats);
//   let port = parseInt(u.port, 10);
//
//   let nc = await connect({
//     url: server.nats,
//     reconnectTimeWait: 100,
//     maxReconnectAttempts: -1
//   });
//
//   nc.on('connect', () => {
//     setTimeout(() => {
//       stopServer(server, () => {
//         server = null;
//       });
//     }, 100);
//   });
//
//   setTimeout(async () => {
//     server = await startServer(['-p', port.toString()]);
//     registerServer(server, t);
//   }, 1000);
//
//
//   let disconnects = 0;
//   nc.on('disconnect', () => {
//     disconnects++;
//   });
//
//   let reconnectings = 0;
//   nc.on('reconnecting', () => {
//     reconnectings++;
//   });
//
//   let reconnects = 0;
//   nc.on('reconnect', () => {
//     reconnects++;
//     nc.flush(() => {
//       nc.close();
//       t.true(reconnectings >= 5);
//       t.is(reconnects, 1);
//       t.is(disconnects, 1);
//       lock.unlock();
//     });
//   });
//
//   return lock.latch;
// });
//
// test('jitter', async(t) => {
//   t.plan(2)
//   let lock = new Lock();
//   let socket: Socket | null = null
//   const srv = createServer((c: Socket) => {
//     // @ts-ignore
//     socket = c
//     c.write('INFO ' + JSON.stringify({
//       server_id: 'TEST',
//       version: '0.0.0',
//       host: '127.0.0.1',
//       // @ts-ignore
//       port: srv.address.port,
//       auth_required: false
//     }) + '\r\n')
//     c.on('data', (d: string) => {
//       const r = d.toString()
//       const lines = r.split('\r\n')
//       lines.forEach((line) => {
//         if (line === '\r\n') {
//           return
//         }
//         if (/^CONNECT\s+/.test(line)) {
//         } else if (/^PING/.test(line)) {
//           c.write('PONG\r\n')
//
//           process.nextTick(()=> {
//             // @ts-ignore
//             socket.destroy()
//           })
//         } else if (/^SUB\s+/i.test(line)) {
//         } else if (/^PUB\s+/i.test(line)) {
//         } else if (/^UNSUB\s+/i.test(line)) {
//         } else if (/^MSG\s+/i.test(line)) {
//         } else if (/^INFO\s+/i.test(line)) {
//         }
//       })
//     })
//     c.on('error', () => {
//       // we are messing with the server so this will raise connection reset
//     })
//   })
//   // @ts-ignore
//   srv.sockets = []
//   srv.listen(0, async () => {
//     // @ts-ignore
//     const p = srv.address().port
//     connect({
//       url: 'nats://localhost:' + p,
//       reconnect: true,
//       reconnectTimeWait: 100,
//       reconnectJitter: 100
//     }).then(async (nc) => {
//       const durations: number[] = []
//       let startTime: number
//
//       nc.on('reconnect', () => {
//         const elapsed = Date.now() - startTime
//         durations.push(elapsed)
//         if (durations.length === 10) {
//           const sum = durations.reduce((a, b) => {
//             return a + b
//           }, 0)
//           t.true(sum >= 1000 && 2000 >= sum, `${sum} >= 1000 && 2000 >= ${sum}`)
//           const extra = sum - 1000
//           t.true(extra >= 100 && 900 >= extra, `${extra} >= 100 && 900 >= ${extra}`)
//           srv.close(() => {
//             nc.close()
//             lock.unlock()
//           })
//         }
//       })
//       nc.on('disconnect', () => {
//         startTime = Date.now()
//       })
//     })
//
//   })
//   return lock.latch
// })

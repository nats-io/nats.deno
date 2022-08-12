/*
 * Copyright 2020-2021 The NATS Authors
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
  assertRejects,
  assertThrows,
  fail,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";

import { assertThrowsAsyncErrorCode } from "./helpers/asserts.ts";

import {
  connect,
  createInbox,
  Empty,
  ErrorCode,
  JSONCodec,
  jwtAuthenticator,
  Msg,
  NatsError,
  StringCodec,
} from "../src/mod.ts";
import {
  assertErrorCode,
  Connection,
  disabled,
  Lock,
  NatsServer,
  TestServer,
} from "./helpers/mod.ts";
import {
  deferred,
  delay,
  headers,
  isIP,
  NatsConnectionImpl,
  SubscriptionImpl,
} from "../nats-base-client/internal_mod.ts";
import { cleanup, setup } from "./jstest_util.ts";
import { RequestStrategy } from "../nats-base-client/types.ts";
import { Feature } from "../nats-base-client/semver.ts";

Deno.test("basics - connect port", async () => {
  const ns = await NatsServer.start();
  const nc = await connect({ port: ns.port });
  await cleanup(ns, nc);
});

Deno.test("basics - connect default", async () => {
  const ns = await NatsServer.start({ port: 4222 });
  const nc = await connect({});
  await cleanup(ns, nc);
});

Deno.test("basics - connect host", async () => {
  const nc = await connect({ servers: "demo.nats.io" });
  await nc.close();
});

Deno.test("basics - connect hostport", async () => {
  const nc = await connect({ servers: "demo.nats.io:4222" });
  await nc.close();
});

Deno.test("basics - connect servers", async () => {
  const ns = await NatsServer.start();
  const nc = await connect({ servers: [`${ns.hostname}:${ns.port}`] });
  await cleanup(ns, nc);
});

Deno.test("basics - fail connect", async () => {
  await connect({ servers: `127.0.0.1:32001` })
    .then(() => {
      fail();
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.ConnectionRefused);
    });
});

Deno.test("basics - publish", async () => {
  const { ns, nc } = await setup();
  nc.publish(createInbox());
  await nc.flush();
  await cleanup(ns, nc);
});

Deno.test("basics - no publish without subject", async () => {
  const { ns, nc } = await setup();
  try {
    nc.publish("");
    fail("should not be able to publish without a subject");
  } catch (err) {
    assertEquals(err.code, ErrorCode.BadSubject);
  } finally {
    await cleanup(ns, nc);
  }
});

Deno.test("basics - pubsub", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  const iter = (async () => {
    for await (const _m of sub) {
      break;
    }
  })();

  nc.publish(subj);
  await iter;
  assertEquals(sub.getProcessed(), 1);
  await cleanup(ns, nc);
});

Deno.test("basics - subscribe and unsubscribe", async () => {
  const { ns, nc } = await setup();
  const nci = nc as NatsConnectionImpl;
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 1000, queue: "aaa" });

  // check the subscription
  assertEquals(nci.protocol.subscriptions.size(), 1);
  let s = nci.protocol.subscriptions.get(1);
  assert(s);
  assertEquals(s.getReceived(), 0);
  assertEquals(s.subject, subj);
  assert(s.callback);
  assertEquals(s.max, 1000);
  assertEquals(s.queue, "aaa");

  // modify the subscription
  sub.unsubscribe(10);
  s = nci.protocol.subscriptions.get(1);
  assert(s);
  assertEquals(s.max, 10);

  // verify subscription updates on message
  nc.publish(subj);
  await nc.flush();
  s = nci.protocol.subscriptions.get(1);
  assert(s);
  assertEquals(s.getReceived(), 1);

  // verify cleanup
  sub.unsubscribe();
  assertEquals(nci.protocol.subscriptions.size(), 0);
  await cleanup(ns, nc);
});

Deno.test("basics - subscriptions iterate", async () => {
  const { ns, nc } = await setup();
  const lock = Lock();
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  (async () => {
    for await (const _m of sub) {
      lock.unlock();
    }
  })().then();
  nc.publish(subj);
  await nc.flush();
  await lock;
  await cleanup(ns, nc);
});

Deno.test("basics - subscriptions pass exact subject to cb", async () => {
  const { ns, nc } = await setup();
  const s = createInbox();
  const subj = `${s}.foo.bar.baz`;
  const sub = nc.subscribe(`${s}.*.*.*`);
  const sp = deferred<string>();
  (async () => {
    for await (const m of sub) {
      sp.resolve(m.subject);
      break;
    }
  })().then();
  nc.publish(subj);
  assertEquals(await sp, subj);
  await cleanup(ns, nc);
});

Deno.test("basics - subscribe returns Subscription", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj) as SubscriptionImpl;
  assertEquals(sub.sid, 1);
  await cleanup(ns, nc);
});

Deno.test("basics - wildcard subscriptions", async () => {
  const { ns, nc } = await setup();

  const single = 3;
  const partial = 2;
  const full = 5;

  const s = createInbox();
  const sub = nc.subscribe(`${s}.*`);
  const sub2 = nc.subscribe(`${s}.foo.bar.*`);
  const sub3 = nc.subscribe(`${s}.foo.>`);

  nc.publish(`${s}.bar`);
  nc.publish(`${s}.baz`);
  nc.publish(`${s}.foo.bar.1`);
  nc.publish(`${s}.foo.bar.2`);
  nc.publish(`${s}.foo.baz.3`);
  nc.publish(`${s}.foo.baz.foo`);
  nc.publish(`${s}.foo.baz`);
  nc.publish(`${s}.foo`);

  await nc.drain();
  assertEquals(sub.getReceived(), single, "single");
  assertEquals(sub2.getReceived(), partial, "partial");
  assertEquals(sub3.getReceived(), full, "full");
  await ns.stop();
});

Deno.test("basics - correct data in message", async () => {
  const { ns, nc } = await setup();
  const sc = StringCodec();
  const subj = createInbox();
  const mp = deferred<Msg>();
  const sub = nc.subscribe(subj);
  (async () => {
    for await (const m of sub) {
      mp.resolve(m);
      break;
    }
  })().then();

  nc.publish(subj, sc.encode(subj));
  const m = await mp;
  assertEquals(m.subject, subj);
  assertEquals(sc.decode(m.data), subj);
  assertEquals(m.reply, "");
  await cleanup(ns, nc);
});

Deno.test("basics - correct reply in message", async () => {
  const { ns, nc } = await setup();
  const s = createInbox();
  const r = createInbox();

  const rp = deferred<string>();
  const sub = nc.subscribe(s);
  (async () => {
    for await (const m of sub) {
      rp.resolve(m.reply);
      break;
    }
  })().then();
  nc.publish(s, Empty, { reply: r });
  assertEquals(await rp, r);
  await cleanup(ns, nc);
});

Deno.test("basics - respond returns false if no reply subject set", async () => {
  const { ns, nc } = await setup();
  const s = createInbox();
  const dr = deferred<boolean>();
  const sub = nc.subscribe(s);
  (async () => {
    for await (const m of sub) {
      dr.resolve(m.respond());
      break;
    }
  })().then();
  nc.publish(s);
  const failed = await dr;
  assert(!failed);
  await cleanup(ns, nc);
});

Deno.test("basics - closed cannot subscribe", async () => {
  const { ns, nc } = await setup();
  await nc.close();
  let failed = false;
  try {
    nc.subscribe(createInbox());
    fail("should have not been able to subscribe");
  } catch (_err) {
    failed = true;
  }
  assert(failed);
  await ns.stop();
});

Deno.test("basics - close cannot request", async () => {
  const { ns, nc } = await setup();
  await nc.close();
  let failed = false;
  try {
    await nc.request(createInbox());
    fail("should have not been able to request");
  } catch (_err) {
    failed = true;
  }
  assert(failed);
  await ns.stop();
});

Deno.test("basics - flush returns promise", async () => {
  const { ns, nc } = await setup();
  const p = nc.flush();
  if (!p) {
    fail("should have returned a promise");
  }
  await p;
  await cleanup(ns, nc);
});

Deno.test("basics - unsubscribe after close", async () => {
  const { ns, nc } = await setup();
  const sub = nc.subscribe(createInbox());
  await nc.close();
  sub.unsubscribe();
  await ns.stop();
});

Deno.test("basics - unsubscribe stops messages", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  // in this case we use a callback otherwise messages are buffered.
  const sub = nc.subscribe(subj, {
    callback: () => {
      sub.unsubscribe();
    },
  });
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);

  await nc.flush();
  assertEquals(sub.getReceived(), 1);
  await cleanup(ns, nc);
});

Deno.test("basics - request", async () => {
  const { ns, nc } = await setup();
  const sc = StringCodec();
  const s = createInbox();
  const sub = nc.subscribe(s);
  (async () => {
    for await (const m of sub) {
      m.respond(sc.encode("foo"));
    }
  })().then();
  const msg = await nc.request(s);
  assertEquals(sc.decode(msg.data), "foo");
  await cleanup(ns, nc);
});

Deno.test("basics - request no responders", async () => {
  const { ns, nc } = await setup();
  const s = createInbox();
  await assertThrowsAsyncErrorCode(async () => {
    await nc.request(s, Empty, { timeout: 100 });
  }, ErrorCode.NoResponders);
  await cleanup(ns, nc);
});

Deno.test("basics - request timeout", async () => {
  const { ns, nc } = await setup();
  const s = createInbox();
  nc.subscribe(s, { callback: () => {} });
  await assertThrowsAsyncErrorCode(async () => {
    await nc.request(s, Empty, { timeout: 100 });
  }, ErrorCode.Timeout);
  await cleanup(ns, nc);
});

Deno.test("basics - request cancel rejects", async () => {
  const { ns, nc } = await setup();
  const nci = nc as NatsConnectionImpl;
  const s = createInbox();
  const lock = Lock();

  nc.request(s, Empty, { timeout: 1000 })
    .then(() => {
      fail();
    })
    .catch((err) => {
      assertEquals(err.code, ErrorCode.Cancelled);
      lock.unlock();
    });

  nci.protocol.muxSubscriptions.reqs.forEach((v) => {
    v.cancel();
  });
  await lock;
  await cleanup(ns, nc);
});

Deno.test("basics - old style requests", async () => {
  const { ns, nc } = await setup();
  const sc = StringCodec();
  nc.subscribe("q", {
    callback: (_err, msg) => {
      msg.respond(sc.encode("hello"));
    },
  });

  const m = await nc.request(
    "q",
    Empty,
    { reply: "bar", noMux: true, timeout: 1000 },
  );
  assertEquals("hello", sc.decode(m.data));
  assertEquals("bar", m.subject);

  await cleanup(ns, nc);
});

Deno.test("basics - request with custom subject", async () => {
  const { ns, nc } = await setup();
  const sc = StringCodec();
  nc.subscribe("q", {
    callback: (_err, msg) => {
      msg.respond(sc.encode("hello"));
    },
  });

  try {
    await nc.request(
      "q",
      Empty,
      { reply: "bar", timeout: 1000 },
    );

    fail("should have failed");
  } catch (err) {
    const nerr = err as NatsError;
    assertEquals(ErrorCode.InvalidOption, nerr.code);
  }
  await cleanup(ns, nc);
});

Deno.test("basics - request with headers", async () => {
  const { ns, nc } = await setup();
  const sc = StringCodec();
  const s = createInbox();
  const sub = nc.subscribe(s);
  (async () => {
    for await (const m of sub) {
      const headerContent = m.headers?.get("test-header");
      m.respond(sc.encode(`header content: ${headerContent}`));
    }
  })().then();
  const requestHeaders = headers();
  requestHeaders.append("test-header", "Hello, world!");
  const msg = await nc.request(s, Empty, {
    headers: requestHeaders,
    timeout: 5000,
  });
  assertEquals(sc.decode(msg.data), "header content: Hello, world!");
  await cleanup(ns, nc);
});

Deno.test("basics - request with headers and custom subject", async () => {
  const { ns, nc } = await setup();
  const sc = StringCodec();
  const s = createInbox();
  const sub = nc.subscribe(s);
  (async () => {
    for await (const m of sub) {
      const headerContent = m.headers?.get("test-header");
      m.respond(sc.encode(`header content: ${headerContent}`));
    }
  })().then();
  const requestHeaders = headers();
  requestHeaders.append("test-header", "Hello, world!");
  const msg = await nc.request(s, Empty, {
    headers: requestHeaders,
    timeout: 5000,
    reply: "reply-subject",
    noMux: true,
  });
  assertEquals(sc.decode(msg.data), "header content: Hello, world!");
  await cleanup(ns, nc);
});

Deno.test("basics - request requires a subject", async () => {
  const { ns, nc } = await setup();
  await assertRejects(
    async () => {
      //@ts-ignore: subject missing on purpose
      await nc.request();
    },
    NatsError,
    "BAD_SUBJECT",
    undefined,
  );
  await cleanup(ns, nc);
});

Deno.test("basics - close promise resolves", async () => {
  const lock = Lock();
  const cs = new TestServer(false, (ca: Connection) => {
    setTimeout(() => {
      ca.close();
    }, 0);
  });
  const nc = await connect(
    { port: cs.getPort(), reconnect: false },
  );
  nc.closed().then(() => {
    lock.unlock();
  });

  await lock;
  await cs.stop();
  await nc.close();
});

Deno.test("basics - closed returns error", async () => {
  const lock = Lock(1);
  const cs = new TestServer(false, (ca: Connection) => {
    setTimeout(async () => {
      await ca.write(new TextEncoder().encode("-ERR 'here'\r\n"));
    }, 500);
  });

  const nc = await connect(
    { servers: `127.0.0.1:${cs.getPort()}` },
  );
  await nc.closed()
    .then((v) => {
      assertEquals((v as Error).message, "'here'");
      lock.unlock();
    });
  assertEquals(nc.isClosed(), true);
  await cs.stop();
});

Deno.test("basics - subscription with timeout", async () => {
  const { ns, nc } = await setup();
  const lock = Lock(1);
  const sub = nc.subscribe(createInbox(), { max: 1, timeout: 250 });
  (async () => {
    for await (const _m of sub) {
      // ignored
    }
  })().catch((err) => {
    assertErrorCode(err, ErrorCode.Timeout);
    lock.unlock();
  });
  await lock;
  await cleanup(ns, nc);
});

Deno.test("basics - subscription expecting 2 doesn't fire timeout", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 2, timeout: 500 });
  (async () => {
    for await (const _m of sub) {
      // ignored
    }
  })().catch((err) => {
    fail(err);
  });

  nc.publish(subj);
  await nc.flush();
  await delay(1000);

  assertEquals(sub.getReceived(), 1);
  await cleanup(ns, nc);
});

Deno.test("basics - subscription timeout auto cancels", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  let c = 0;
  const sub = nc.subscribe(subj, { max: 2, timeout: 300 });
  (async () => {
    for await (const _m of sub) {
      c++;
    }
  })().catch((err) => {
    fail(err);
  });

  nc.publish(subj);
  nc.publish(subj);
  await delay(500);
  assertEquals(c, 2);
  await cleanup(ns, nc);
});

Deno.test("basics - no mux requests create normal subs", async () => {
  const { ns, nc } = await setup();
  const nci = nc as NatsConnectionImpl;
  nc.request(createInbox(), Empty, { timeout: 1000, noMux: true }).then();
  assertEquals(nci.protocol.subscriptions.size(), 1);
  assertEquals(nci.protocol.muxSubscriptions.size(), 0);
  const sub = nci.protocol.subscriptions.get(1);
  assert(sub);
  assertEquals(sub.max, 1);
  sub.unsubscribe();
  assertEquals(nci.protocol.subscriptions.size(), 0);
  await cleanup(ns, nc);
});

Deno.test("basics - no mux requests timeout", async () => {
  const { ns, nc } = await setup();
  const lock = Lock();
  const subj = createInbox();
  nc.subscribe(subj, { callback: () => {} });

  await nc.request(
    subj,
    Empty,
    { timeout: 1000, noMux: true },
  )
    .catch((err) => {
      assertErrorCode(err, ErrorCode.Timeout);
      lock.unlock();
    });
  await lock;
  await cleanup(ns, nc);
});

Deno.test("basics - no mux requests", async () => {
  const { ns, nc } = await setup({ max_payload: 2048 });
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  const data = Uint8Array.from([1234]);
  (async () => {
    for await (const m of sub) {
      m.respond(data);
    }
  })().then();

  const m = await nc.request(subj, Empty, { timeout: 1000, noMux: true });
  assertEquals(m.data, data);
  await cleanup(ns, nc);
});

Deno.test("basics - no max_payload messages", async () => {
  const { ns, nc } = await setup({ max_payload: 2048 });
  const nci = nc as NatsConnectionImpl;
  assert(nci.protocol.info);
  const big = new Uint8Array(nci.protocol.info.max_payload + 1);

  const subj = createInbox();
  try {
    nc.publish(subj, big);
    fail();
  } catch (err) {
    assertErrorCode(err, ErrorCode.MaxPayloadExceeded);
  }

  try {
    await nc.request(subj, big).then();
    fail();
  } catch (err) {
    assertErrorCode(err, ErrorCode.MaxPayloadExceeded);
  }

  const sub = nc.subscribe(subj);
  (async () => {
    for await (const m of sub) {
      m.respond(big);
      fail();
    }
  })().catch((err) => {
    assertErrorCode(err, ErrorCode.MaxPayloadExceeded);
  });

  await nc.request(subj).then(() => {
    fail();
  }).catch((err) => {
    assertErrorCode(err, ErrorCode.Timeout);
  });

  await cleanup(ns, nc);
});

Deno.test("basics - empty message", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const mp = deferred<Msg>();
  const sub = nc.subscribe(subj);
  (async () => {
    for await (const m of sub) {
      mp.resolve(m);
      break;
    }
  })().then();

  nc.publish(subj);
  const m = await mp;
  assertEquals(m.subject, subj);
  assertEquals(m.data.length, 0);
  await cleanup(ns, nc);
});

Deno.test("basics - msg buffers dont overwrite", async () => {
  const { ns, nc } = await setup();
  const nci = nc as NatsConnectionImpl;
  const N = 100;
  const sub = nc.subscribe(">");
  const msgs: Msg[] = [];
  (async () => {
    for await (const m of sub) {
      msgs.push(m);
    }
  })().then();

  const a = "a".charCodeAt(0);
  const fill = (n: number, b: Uint8Array) => {
    const v = n % 26 + a;
    for (let i = 0; i < b.length; i++) {
      b[i] = v;
    }
  };
  const td = new TextDecoder();
  assert(nci.protocol.info);
  const buf = new Uint8Array(nci.protocol.info.max_payload);
  for (let i = 0; i < N; i++) {
    fill(i, buf);
    const subj = td.decode(buf.subarray(0, 26));
    nc.publish(subj, buf, { reply: subj });
    await nc.flush();
  }

  await nc.drain();
  await ns.stop();

  const check = (n: number, m: Msg) => {
    const v = n % 26 + a;
    assert(nci.protocol.info);
    assertEquals(m.data.length, nci.protocol.info.max_payload);
    for (let i = 0; i < m.data.length; i++) {
      if (m.data[i] !== v) {
        fail(
          `failed on iteration ${i} - expected ${String.fromCharCode(v)} got ${
            String.fromCharCode(m.data[i])
          }`,
        );
      }
    }
    assertEquals(m.subject, td.decode(m.data.subarray(0, 26)), "subject check");
    assertEquals(m.reply, td.decode(m.data.subarray(0, 26)), "reply check");
  };

  assertEquals(msgs.length, N);
  for (let i = 0; i < N; i++) {
    check(i, msgs[i]);
  }
});

Deno.test("basics - get client ip", async () => {
  const ns = await NatsServer.start();
  const nc = await connect({ servers: `localhost:${ns.port}` });
  const ip = nc.info?.client_ip || "";
  assertEquals(isIP(ip), true);
  await nc.close();
  assert(nc.info === undefined);
  await ns.stop();
});

Deno.test("basics - subs pending count", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();

  const sub = nc.subscribe(subj, { max: 10 });
  const done = (async () => {
    let count = 0;
    for await (const _m of sub) {
      count++;
      assertEquals(count, sub.getProcessed());
      assertEquals(sub.getProcessed() + sub.getPending(), 11);
    }
  })();

  for (let i = 0; i < 10; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  await done;
  await cleanup(ns, nc);
});

Deno.test("basics - create inbox", () => {
  type inout = [string, string, boolean?];
  const t: inout[] = [];
  t.push(["", "_INBOX."]);
  //@ts-ignore testing
  t.push([undefined, "_INBOX."]);
  //@ts-ignore testing
  t.push([null, "_INBOX."]);
  //@ts-ignore testing
  t.push([5, "5.", true]);
  t.push(["hello", "hello."]);

  t.forEach((v, index) => {
    if (v[2]) {
      assertThrows(() => {
        createInbox(v[0]);
      });
    } else {
      const out = createInbox(v[0]);
      assert(out.startsWith(v[1]), `test ${index}`);
    }
  });
});

Deno.test("basics - custom prefix", async () => {
  const { ns, nc } = await setup({}, { inboxPrefix: "_x" });
  const jc = JSONCodec();
  const subj = createInbox();
  nc.subscribe(subj, {
    max: 1,
    callback: (_err, msg) => {
      msg.respond(jc.encode(msg.reply!.startsWith("_x.")));
    },
  });

  const v = await nc.request(subj);
  assert(jc.decode(v.data));
  await cleanup(ns, nc);
});

Deno.test("basics - custom prefix noMux", async () => {
  const { ns, nc } = await setup({}, { inboxPrefix: "_y" });
  const jc = JSONCodec();
  const subj = createInbox();
  nc.subscribe(subj, {
    max: 1,
    callback: (_err, msg) => {
      msg.respond(jc.encode(msg.reply!.startsWith("_y.")));
    },
  });

  const v = await nc.request(subj);
  assert(jc.decode(v.data));
  await cleanup(ns, nc);
});

Deno.test("basics - debug", async () => {
  const { ns, nc } = await setup({}, { debug: true });
  await nc.flush();
  await cleanup(ns, nc);
  assertEquals(nc.isClosed(), true);
});

Deno.test("basics - subscription with timeout cancels on message", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 1, timeout: 500 }) as SubscriptionImpl;
  assert(sub.timer !== undefined);
  const done = (async () => {
    for await (const _m of sub) {
      assertEquals(sub.timer, undefined);
    }
  })();
  nc.publish(subj);
  await done;
  await cleanup(ns, nc);
});

Deno.test("basics - subscription cb with timeout cancels on message", async () => {
  const { ns, nc } = await setup();
  const subj = createInbox();
  const done = Lock();
  const sub = nc.subscribe(subj, {
    max: 1,
    timeout: 500,
    callback: () => {
      done.unlock();
    },
  }) as SubscriptionImpl;
  assert(sub.timer !== undefined);
  nc.publish(subj);
  await done;
  assertEquals(sub.timer, undefined);
  await cleanup(ns, nc);
});

Deno.test("basics - resolve", async () => {
  const token = Deno.env.get("NGS_CI_USER");
  if (token === undefined) {
    disabled(
      `skipping: NGS_CI_USER is not available in the environment`,
    );
    return;
  }

  const nci = await connect({
    servers: "connect.ngs.global",
    authenticator: jwtAuthenticator(token),
  }) as NatsConnectionImpl;

  await nci.flush();
  const srv = nci.protocol.servers.getCurrentServer();
  assert(srv.resolves && srv.resolves.length > 1);
  await nci.close();
});

Deno.test("basics - port and server are mutually exclusive", async () => {
  await assertRejects(
    async () => {
      await connect({ servers: "localhost", port: 4222 });
    },
    NatsError,
    "port and servers options are mutually exclusive",
    undefined,
  );
});

Deno.test("basics - rtt", async () => {
  const { ns, nc } = await setup({}, {
    maxReconnectAttempts: 5,
    reconnectTimeWait: 250,
  });
  const rtt = await nc.rtt();
  assert(rtt > 0);

  await ns.stop();
  await delay(500);
  await assertRejects(
    async () => {
      await nc.rtt();
    },
    Error,
    ErrorCode.Disconnect,
  );

  await nc.closed();

  await assertRejects(
    async () => {
      await nc.rtt();
    },
    Error,
    ErrorCode.ConnectionClosed,
  );
});

Deno.test("basics - request many count", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 5; i++) {
        msg.respond();
      }
    },
  });

  const lock = Lock(5, 2000);

  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Count,
    maxWait: 2000,
    maxMessages: 5,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    lock.unlock();
  }
  await lock;

  await cleanup(ns, nc);
});

Deno.test("basics - request many jitter", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 10; i++) {
        msg.respond();
      }
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.JitterTimer,
    maxWait: 5000,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(500 > time);
  assertEquals(count, 10);
  await cleanup(ns, nc);
});

Deno.test("basics - request many sentinel", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  const sc = StringCodec();
  const payload = sc.encode("hello");
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 10; i++) {
        msg.respond(payload);
      }
      msg.respond();
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.SentinelMsg,
    maxWait: 2000,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(500 > time);
  assertEquals(count, 11);
  await cleanup(ns, nc);
});

Deno.test("basics - request many sentinel - partial response", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  const sc = StringCodec();
  const payload = sc.encode("hello");
  nc.subscribe(subj, {
    callback: (_err, msg) => {
      for (let i = 0; i < 10; i++) {
        msg.respond(payload);
      }
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.SentinelMsg,
    maxWait: 2000,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(time >= 2000);
  assertEquals(count, 10);
  await cleanup(ns, nc);
});

Deno.test("basics - request many wait for timer - no respone", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: () => {
      // ignore it
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Timer,
    maxWait: 2000,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(time >= 2000);
  assertEquals(count, 0);
  await cleanup(ns, nc);
});

Deno.test("basics - request many waits for timer late response", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();
  nc.subscribe(subj, {
    callback: async (_err, msg) => {
      await delay(1750);
      msg.respond();
    },
  });

  let count = 0;
  const start = Date.now();
  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Timer,
    maxWait: 2000,
  });
  for await (const mer of iter) {
    if (mer instanceof Error) {
      fail(mer.message);
    }
    count++;
  }
  const time = Date.now() - start;
  assert(time >= 2000);
  assertEquals(count, 1);
  await cleanup(ns, nc);
});

Deno.test("basics - request many stops on error", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;

  const subj = createInbox();

  const iter = await nci.requestMany(subj, Empty, {
    strategy: RequestStrategy.Timer,
    maxWait: 2000,
  });
  const d = deferred<Error>();
  for await (const mer of iter) {
    if (mer instanceof Error) {
      d.resolve(mer);
    }
  }
  const err = await d;
  assertErrorCode(err, ErrorCode.NoResponders);
  await cleanup(ns, nc);
});

Deno.test("basics - server version", async () => {
  const { ns, nc } = await setup({});
  const nci = nc as NatsConnectionImpl;
  assertEquals(nci.protocol.features.require("3.0.0"), false);
  assertEquals(nci.protocol.features.require("2.8.2"), true);

  const ok = nci.protocol.features.require("2.8.3");
  const bytes = nci.protocol.features.get(Feature.JS_PULL_MAX_BYTES);
  assertEquals(ok, bytes.ok);
  assertEquals(bytes.min, "2.8.3");
  assertEquals(ok, nci.protocol.features.supports(Feature.JS_PULL_MAX_BYTES));

  await cleanup(ns, nc);
});

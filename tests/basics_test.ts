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
  assert,
  assertEquals,
  fail,
} from "https://deno.land/std@0.80.0/testing/asserts.ts";
import {
  connect,
  createInbox,
  Empty,
  ErrorCode,
  Msg,
  NatsError,
  StringCodec,
} from "../src/mod.ts";
import {
  assertErrorCode,
  Connection,
  Lock,
  NatsServer,
  TestServer,
} from "./helpers/mod.ts";
import {
  deferred,
  delay,
  NatsConnectionImpl,
  SubscriptionImpl,
} from "../nats-base-client/internal_mod.ts";

const u = "demo.nats.io:4222";

Deno.test("basics - connect port", async () => {
  const ns = await NatsServer.start();
  const nc = await connect({ port: ns.port });
  await nc.close();
  await ns.stop();
});

Deno.test("basics - connect default", async () => {
  const ns = await NatsServer.start({ port: 4222 });
  const nc = await connect({});
  await nc.close();
  await ns.stop();
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
  const nc = await connect({ servers: u });
  await nc.close();
});

Deno.test("basics - fail connect", async () => {
  await connect({ servers: `127.0.0.1:32001` })
    .then(() => {
      fail();
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.CONNECTION_REFUSED);
    });
});

Deno.test("basics - publish", async () => {
  const nc = await connect({ servers: u });
  nc.publish(createInbox());
  await nc.flush();
  await nc.close();
});

Deno.test("basics - no publish without subject", async () => {
  const nc = await connect({ servers: u });
  try {
    nc.publish("");
    fail("should not be able to publish without a subject");
  } catch (err) {
    assertEquals(err.code, ErrorCode.BAD_SUBJECT);
  } finally {
    await nc.close();
  }
});

Deno.test("basics - pubsub", async () => {
  const subj = createInbox();
  const nc = await connect({ servers: u });
  const sub = nc.subscribe(subj);
  const iter = (async () => {
    for await (const m of sub) {
      break;
    }
  })();

  nc.publish(subj);
  await iter;
  assertEquals(sub.getProcessed(), 1);
  await nc.close();
});

Deno.test("basics - subscribe and unsubscribe", async () => {
  const subj = createInbox();
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const sub = nc.subscribe(subj, { max: 1000, queue: "aaa" });

  // check the subscription
  assertEquals(nc.protocol.subscriptions.size(), 1);
  let s = nc.protocol.subscriptions.get(1);
  assert(s);
  assertEquals(s.getReceived(), 0);
  assertEquals(s.subject, subj);
  assert(s.callback);
  assertEquals(s.max, 1000);
  assertEquals(s.queue, "aaa");

  // modify the subscription
  sub.unsubscribe(10);
  s = nc.protocol.subscriptions.get(1);
  assert(s);
  assertEquals(s.max, 10);

  // verify subscription updates on message
  nc.publish(subj);
  await nc.flush();
  s = nc.protocol.subscriptions.get(1);
  assert(s);
  assertEquals(s.getReceived(), 1);

  // verify cleanup
  sub.unsubscribe();
  assertEquals(nc.protocol.subscriptions.size(), 0);
  await nc.close();
});

Deno.test("basics - subscriptions iterate", async () => {
  const lock = Lock();
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  (async () => {
    for await (const m of sub) {
      lock.unlock();
    }
  })().then();
  nc.publish(subj);
  await nc.flush();
  await lock;
  await nc.close();
});

Deno.test("basics - subscriptions pass exact subject to cb", async () => {
  const s = createInbox();
  const subj = `${s}.foo.bar.baz`;
  const nc = await connect({ servers: u });
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
  await nc.close();
});

Deno.test("basics - subscribe returns Subscription", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj) as SubscriptionImpl;
  assertEquals(sub.sid, 1);
  await nc.close();
});

Deno.test("basics - wildcard subscriptions", async () => {
  const single = 3;
  const partial = 2;
  const full = 5;

  let nc = await connect({ servers: u });
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
});

Deno.test("basics - correct data in message", async () => {
  const sc = StringCodec();
  const nc = await connect({ servers: u });
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
  await nc.close();
});

Deno.test("basics - correct reply in message", async () => {
  const nc = await connect({ servers: u });
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
  await nc.close();
});

Deno.test("basics - respond returns false if no reply subject set", async () => {
  let nc = await connect({ servers: u });
  let s = createInbox();
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
  await nc.close();
});

Deno.test("basics - closed cannot subscribe", async () => {
  let nc = await connect({ servers: u });
  await nc.close();
  let failed = false;
  try {
    nc.subscribe(createInbox());
    fail("should have not been able to subscribe");
  } catch (err) {
    failed = true;
  }
  assert(failed);
});

Deno.test("basics - close cannot request", async () => {
  let nc = await connect({ servers: u });
  await nc.close();
  let failed = false;
  try {
    await nc.request(createInbox());
    fail("should have not been able to request");
  } catch (err) {
    failed = true;
  }
  assert(failed);
});

Deno.test("basics - flush returns promise", async () => {
  const nc = await connect({ servers: u });
  let p = nc.flush();
  if (!p) {
    fail("should have returned a promise");
  }
  await p;
  await nc.close();
});

Deno.test("basics - unsubscribe after close", async () => {
  let nc = await connect({ servers: u });
  let sub = nc.subscribe(createInbox());
  await nc.close();
  sub.unsubscribe();
});

Deno.test("basics - unsubscribe stops messages", async () => {
  const nc = await connect({ servers: u });
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
  await nc.close();
});

Deno.test("basics - request", async () => {
  const sc = StringCodec();
  const nc = await connect({ servers: u });
  const s = createInbox();
  const sub = nc.subscribe(s);
  (async () => {
    for await (const m of sub) {
      m.respond(sc.encode("foo"));
    }
  })().then();
  const msg = await nc.request(s);
  await nc.close();
  assertEquals(sc.decode(msg.data), "foo");
});

Deno.test("basics - request timeout", async () => {
  const nc = await connect({ servers: u });
  const s = createInbox();
  const lock = Lock();

  nc.request(s, Empty, { timeout: 100 })
    .then(() => {
      fail();
    })
    .catch((err) => {
      assertEquals(err.code, ErrorCode.TIMEOUT);
      lock.unlock();
    });

  await lock;
  await nc.close();
});

Deno.test("basics - request cancel rejects", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const s = createInbox();
  const lock = Lock();

  nc.request(s, Empty, { timeout: 1000 })
    .then(() => {
      fail();
    })
    .catch((err) => {
      assertEquals(err.code, ErrorCode.CANCELLED);
      lock.unlock();
    });

  nc.protocol.muxSubscriptions.reqs.forEach((v) => {
    v.cancel();
  });
  await lock;
  await nc.close();
});

Deno.test("basics - old style requests", async () => {
  const sc = StringCodec();
  const nc = await connect({ servers: u });
  nc.subscribe("q", {
    callback: (err, msg) => {
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

  await nc.close();
});

Deno.test("basics - request with custom subject", async () => {
  const sc = StringCodec();
  const nc = await connect({ servers: u });
  nc.subscribe("q", {
    callback: (err, msg) => {
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
    assertEquals(ErrorCode.INVALID_OPTION, nerr.code);
  }

  await nc.close();
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
  const lock = Lock(1);
  const nc = await connect({ servers: u });
  const sub = nc.subscribe(createInbox(), { max: 1, timeout: 250 });
  (async () => {
    for await (const m of sub) {}
  })().catch((err) => {
    assertErrorCode(err, ErrorCode.TIMEOUT);
    lock.unlock();
  });
  await lock;
  await nc.close();
});

Deno.test("basics - subscription expecting 2 doesn't fire timeout", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 2, timeout: 500 });
  (async () => {
    for await (const m of sub) {}
  })().catch((err) => {
    fail(err);
  });

  nc.publish(subj);
  await nc.flush();
  await delay(1000);

  assertEquals(sub.getReceived(), 1);
  await nc.close();
});

Deno.test("basics - subscription timeout auto cancels", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();
  let c = 0;
  const sub = nc.subscribe(subj, { max: 2, timeout: 300 });
  (async () => {
    for await (const m of sub) {
      c++;
    }
  })().catch((err) => {
    fail(err);
  });

  nc.publish(subj);
  nc.publish(subj);
  await delay(500);
  assertEquals(c, 2);
  await nc.close();
});

Deno.test("basics - no mux requests create normal subs", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  nc.request(createInbox(), Empty, { timeout: 1000, noMux: true }).then();
  assertEquals(nc.protocol.subscriptions.size(), 1);
  assertEquals(nc.protocol.muxSubscriptions.size(), 0);
  const sub = nc.protocol.subscriptions.get(1);
  assert(sub);
  assertEquals(sub.max, 1);
  sub.unsubscribe();
  assertEquals(nc.protocol.subscriptions.size(), 0);
  await nc.close();
});

Deno.test("basics - no mux requests timeout", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  const lock = Lock();
  nc.request(createInbox(), Empty, { timeout: 250, noMux: true })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.TIMEOUT);
      lock.unlock();
    });
  await lock;
  await nc.close();
});

Deno.test("basics - no mux requests", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
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
  await nc.close();
});

Deno.test("basics - no max_payload messages", async () => {
  const nc = await connect({ servers: u }) as NatsConnectionImpl;
  assert(nc.protocol.info.max_payload);
  const big = new Uint8Array(nc.protocol.info.max_payload + 1);

  const subj = createInbox();
  try {
    nc.publish(subj, big);
    fail();
  } catch (err) {
    assertErrorCode(err, ErrorCode.MAX_PAYLOAD_EXCEEDED);
  }

  try {
    await nc.request(subj, big).then();
    fail();
  } catch (err) {
    assertErrorCode(err, ErrorCode.MAX_PAYLOAD_EXCEEDED);
  }

  const sub = nc.subscribe(subj);
  (async () => {
    for await (const m of sub) {
      m.respond(big);
      fail();
    }
  })().catch((err) => {
    assertErrorCode(err, ErrorCode.MAX_PAYLOAD_EXCEEDED);
  });

  await nc.request(subj).then(() => {
    fail();
  }).catch((err) => {
    assertErrorCode(err, ErrorCode.TIMEOUT);
  });

  await nc.close();
});

Deno.test("basics - empty message", async () => {
  const nc = await connect({ servers: u });
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
  await nc.close();
});

Deno.test("basics - msg buffers dont overwrite", async () => {
  const N = 100;
  const ns = await NatsServer.start();
  const nc = await connect({ port: ns.port }) as NatsConnectionImpl;
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
  const buf = new Uint8Array(nc.protocol.info.max_payload);
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
    assertEquals(m.data.length, nc.protocol.info.max_payload);
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
  const nc = await connect({ servers: u });
  assert(nc.info?.client_id);
  await nc.close();
  assert(nc.info === undefined);
});

Deno.test("basics - subs pending count", async () => {
  const nc = await connect({ servers: u });
  const subj = createInbox();

  const sub = nc.subscribe(subj, { max: 10 });
  const done = (async () => {
    let count = 0;
    for await (const m of sub) {
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
  await nc.close();
});

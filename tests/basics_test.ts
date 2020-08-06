import {
  assert,
  assertEquals,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std@0.63.0/testing/asserts.ts";
import {
  connect,
  Msg,
  ErrorCode,
  createInbox,
  Empty,
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
  await assertThrowsAsync(async (): Promise<void> => {
    await connect({ servers: `127.0.0.1:32001` });
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
  const _ = (async () => {
    for await (const m of sub) {
      lock.unlock();
    }
  })();
  nc.publish(subj);
  await nc.flush();
  await nc.close();
  await lock;
});

Deno.test("basics - subscriptions pass exact subject to cb", async () => {
  const s = createInbox();
  const subj = `${s}.foo.bar.baz`;
  const nc = await connect({ servers: u });
  const sub = nc.subscribe(`${s}.*.*.*`);
  const sp = deferred<string>();
  const _ = (async () => {
    for await (const m of sub) {
      sp.resolve(m.subject);
      break;
    }
  })();
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
  const _ = (async () => {
    for await (const m of sub) {
      mp.resolve(m);
      break;
    }
  })();

  nc.publish(subj, sc.encode(subj));
  const m = await mp;
  assertEquals(m.subject, subj);
  assertEquals(sc.decode(m.data), subj);
  assertEquals(m.reply, undefined);
  await nc.close();
});

Deno.test("basics - correct reply in message", async () => {
  const nc = await connect({ servers: u });
  const s = createInbox();
  const r = createInbox();

  const rp = deferred<string>();
  const sub = nc.subscribe(s);
  const _ = (async () => {
    for await (const m of sub) {
      rp.resolve(m.reply);
      break;
    }
  })();
  nc.publish(s, Empty, { reply: r });
  assertEquals(await rp, r);
  await nc.close();
});

Deno.test("basics - respond returns false if no reply subject set", async () => {
  let nc = await connect({ servers: u });
  let s = createInbox();
  const dr = deferred<boolean>();
  const sub = nc.subscribe(s);
  const _ = (async () => {
    for await (const m of sub) {
      dr.resolve(m.respond());
      break;
    }
  })();
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
  const _ = (async () => {
    for await (const m of sub) {
      m.respond(sc.encode("foo"));
    }
  })();
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

  const nc = await connect({ servers: `127.0.0.1:${cs.getPort()}` });
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

  assertEquals(1, sub.getReceived());
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

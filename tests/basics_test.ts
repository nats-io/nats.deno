import {
  assert,
  assertEquals,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";
import {
  connect,
  ErrorCode,
  NatsConnection,
  Nuid,
  Payload,
} from "../src/mod.ts";
import {
  assertErrorCode,
  Connection,
  Lock,
  NatsServer,
  TestServer,
} from "./helpers/mod.ts";
import { delay } from "../nats-base-client/util.ts";
import { SubscriptionImpl } from "../nats-base-client/mod.ts";

const u = "demo.nats.io:4222";

const nuid = new Nuid();

Deno.test("basics - connect port", async () => {
  const ns = await NatsServer.start();
  let nc = await connect({ port: ns.port });
  await nc.close();
  await ns.stop();
});

Deno.test("basics - connect default", async () => {
  const ns = await NatsServer.start({ port: 4222 });
  let nc = await connect({});
  await nc.close();
  await ns.stop();
});

Deno.test("basics - connect host", async () => {
  let nc = await connect({ url: "demo.nats.io" });
  await nc.close();
});

Deno.test("basics - connect hostport", async () => {
  let nc = await connect({ url: "demo.nats.io:4222" });
  await nc.close();
});

Deno.test("basics - connect url", async () => {
  const nc = await connect({ url: u });
  await nc.close();
});

Deno.test("basics - fail connect", async () => {
  await assertThrowsAsync(async (): Promise<void> => {
    await connect({ url: `127.0.0.1:32001` });
  });
});

Deno.test("basics - publish", async () => {
  const nc = await connect({ url: u });
  nc.publish(nuid.next());
  await nc.flush();
  await nc.close();
});

Deno.test("basics - no publish without subject", async () => {
  const nc = await connect({ url: u });
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
  const lock = Lock();
  const subj = nuid.next();
  connect({ url: u })
    .then((nc) => {
      nc.subscribe(subj, {
        callback: async () => {
          await nc.close();
          lock.unlock();
        },
      });
      nc.publish(subj);
    });
  await lock;
});

Deno.test("basics - subscribe and unsubscribe", async () => {
  const subj = nuid.next();
  const nc = await connect({ url: u });
  const sub = nc.subscribe(
    subj,
    { max: 1000, queue: "aaa", callback: () => {} },
  );

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

Deno.test("basics - subscriptions fire callbacks", async () => {
  const lock = Lock();
  const nc = await connect({ url: u });
  const subj = nuid.next();
  nc.subscribe(subj, {
    callback: () => {
      lock.unlock();
    },
  });
  nc.publish(subj);
  await nc.flush();
  await nc.close();
  await lock;
});

Deno.test("basics - subscriptions pass exact subject to cb", async () => {
  const s = nuid.next();
  const subj = `${s}.foo.bar.baz`;
  const nc = await connect({ url: u });
  let received = "";
  nc.subscribe(`${s}.*.*.*`, {
    callback: (err, msg) => {
      received = msg.subject;
    },
  });
  nc.publish(subj);
  await nc.flush();
  await nc.close();
  assertEquals(received, subj);
});

Deno.test("basics - subscribe returns Subscription", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj) as SubscriptionImpl;
  assertEquals(sub.sid, 1);
  await nc.close();
});

Deno.test("basics - wildcard subscriptions", async () => {
  let single = 3;
  let partial = 2;
  let full = 5;

  let singleCounter = 0;
  let partialCounter = 0;
  let fullCounter = 0;

  let nc = await connect({ url: u });

  let s = nuid.next();
  nc.subscribe(`${s}.*`, {
    callback: () => {
      singleCounter++;
    },
  });
  nc.subscribe(`${s}.foo.bar.*`, {
    callback: () => {
      partialCounter++;
    },
  });
  nc.subscribe(`${s}.foo.>`, {
    callback: () => {
      fullCounter++;
    },
  });

  nc.publish(`${s}.bar`);
  nc.publish(`${s}.baz`);
  nc.publish(`${s}.foo.bar.1`);
  nc.publish(`${s}.foo.bar.2`);
  nc.publish(`${s}.foo.baz.3`);
  nc.publish(`${s}.foo.baz.foo`);
  nc.publish(`${s}.foo.baz`);
  nc.publish(`${s}.foo`);

  await nc.flush();
  assertEquals(singleCounter, single);
  assertEquals(partialCounter, partial);
  assertEquals(fullCounter, full);

  await nc.close();
});

Deno.test("basics - correct data in message", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();

  let called = false;
  nc.subscribe(subj, {
    callback: (_, m) => {
      called = true;
      assertEquals(m.subject, subj);
      assertEquals(m.data, subj);
      assertEquals(m.reply, undefined);
    },
    max: 1,
  });

  nc.publish(subj, subj);
  await nc.flush();
  await nc.close();
  assert(called);
});

Deno.test("basics - correct reply in message", async () => {
  let nc = await connect({ url: u });
  let s = nuid.next();
  let r = nuid.next();

  let rsubj = null;
  nc.subscribe(s, {
    callback: (_, m) => {
      rsubj = m.reply;
    },
  });

  nc.publish(s, "", r);
  await nc.flush();
  await nc.close();
  assertEquals(rsubj, r);
});

Deno.test("basics - respond returns false if no reply subject set", async () => {
  let nc = await connect({ url: u });
  let s = nuid.next();

  let called = false;
  nc.subscribe(s, {
    callback: (_, m) => {
      if (m.respond()) {
        fail("should have not been able to respond");
      }
      called = true;
    },
  });

  nc.publish(s);
  await nc.flush();
  await nc.close();
  assert(called);
});

Deno.test("basics - closed cannot subscribe", async () => {
  let nc = await connect({ url: u });
  await nc.close();
  let failed = false;
  try {
    nc.subscribe(nuid.next());
    fail("should have not been able to subscribe");
  } catch (err) {
    failed = true;
  }
  assert(failed);
});

Deno.test("basics - close cannot request", async () => {
  let nc = await connect({ url: u });
  await nc.close();
  let failed = false;
  try {
    await nc.request(nuid.next());
    fail("should have not been able to request");
  } catch (err) {
    failed = true;
  }
  assert(failed);
});

Deno.test("basics - flush returns promise", async () => {
  const nc = await connect({ url: u });
  let p = nc.flush();
  if (!p) {
    fail("should have returned a promise");
  }
  await p;
  await nc.close();
});

Deno.test("basics - unsubscribe after close", async () => {
  let nc = await connect({ url: u });
  let sub = nc.subscribe(nuid.next());
  await nc.close();
  sub.unsubscribe();
});

Deno.test("basics - unsubscribe stops messages", async () => {
  let received = 0;
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj, {
    callback: () => {
      received++;
      sub.unsubscribe();
    },
  });
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);

  await nc.flush();
  assertEquals(received, 1);
  await nc.close();
});

Deno.test("basics - request", async () => {
  let nc = await connect({ url: u });
  let s = nuid.next();
  nc.subscribe(s, {
    callback: (_, msg) => {
      msg.respond("foo");
    },
  });
  let msg = await nc.request(s, 1000, "test");
  await nc.close();
  assertEquals(msg.data, "foo");
});

Deno.test("basics - request timeout", async () => {
  const nc = await connect({ url: u });
  const s = nuid.next();
  let timedOut = false;
  try {
    await nc.request(s, 100, "test");
  } catch (err) {
    assertEquals(err.code, ErrorCode.TIMEOUT);
    timedOut = true;
  }
  await nc.close();
  assert(timedOut);
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
  nc.closed().then((err) => {
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

  const nc = await connect({ url: `127.0.0.1:${cs.getPort()}` });
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
  const nc = await connect({ url: u });
  const sub = nc.subscribe(nuid.next(), { max: 1, timeout: 250 });
  (async () => {
    try {
      for await (const m of sub) {}
    } catch (err) {
      assertErrorCode(err, ErrorCode.TIMEOUT);
      lock.unlock();
    }
  })().then();

  await nc.flush();
  await lock;
  await nc.close();
});

Deno.test("basics - subscription expecting 2 doesn't fire timeout", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj, { max: 2, timeout: 100 });
  (async () => {
    for await (const m of sub) {}
  })().catch((err) => {
    fail(err);
  });

  nc.publish(subj);
  await nc.flush();

  assertEquals(1, sub.getReceived());
  await nc.close();
});

Deno.test("basics - subscription timeout autocancels", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
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

Deno.test("basics - subscription timeout cancel", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj, { max: 2, timeout: 100 });
  (async () => {
    for await (const m of sub) {}
  })().catch(() => {
    fail();
  });
  sub.cancelTimeout();
  const lock = Lock(1, 300);
  setTimeout(lock.unlock, 200);
  await lock;
  await nc.flush();
  await nc.close();
});

Deno.test("basics - subscription received", async () => {
  const lock = Lock(3);
  const nc = await connect({ url: u });
  const subj = nuid.next();
  nc.subscribe(subj, {
    callback: () => {
      lock.unlock();
    },
  });
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);

  await lock;
  await nc.close();
});

Deno.test("basics - json payload", async () => {
  const nc = await connect({ url: u, payload: Payload.JSON });
  await nc.close();
});

Deno.test("basics - binary payload", async () => {
  const nc = await connect({ url: u, payload: Payload.BINARY });
  await nc.close();
});

Deno.test("basics - string payload", async () => {
  const nc = await connect({ url: u, payload: Payload.STRING });
  await nc.close();
});

Deno.test("basics - bad payload option", async () => {
  await connect({ url: u, payload: "TEXT" as Payload })
    .then((nc: NatsConnection) => {
      fail("didn't expect to connect with bad payload");
      nc.close();
    })
    .catch(() => {
      // this is expected
    });
});

import {
  assert,
  assertEquals,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std/testing/asserts.ts";
import {
  connect,
  ErrorCode,
  NatsConnection,
  NatsError,
  Nuid,
  Payload,
} from "../src/mod.ts";
import { Connection, Lock, NatsServer, TestServer } from "./helpers/mod.ts";
import { delay } from "../nats-base-client/util.ts";

const u = "https://demo.nats.io:4222";

const nuid = new Nuid();

Deno.test("connect with port", async () => {
  const ns = await NatsServer.start();
  let nc = await connect({ port: ns.port });
  await nc.close();
  await ns.stop();
});

Deno.test("connect default", async () => {
  const ns = await NatsServer.start({ port: 4222 });
  let nc = await connect({});
  await nc.close();
  await ns.stop();
});

Deno.test("connect host", async () => {
  let nc = await connect({ url: "demo.nats.io" });
  await nc.close();
});

Deno.test("connect hostport", async () => {
  let nc = await connect({ url: "demo.nats.io:4222" });
  await nc.close();
});

Deno.test("connect url", async () => {
  const nc = await connect({ url: u });
  await nc.close();
});

Deno.test("fail connect", async () => {
  await assertThrowsAsync(async (): Promise<void> => {
    await connect({ url: `https://localhost:32001` });
  });
});

Deno.test("publish", async () => {
  const nc = await connect({ url: u });
  nc.publish("foo");
  await nc.flush();
  await nc.close();
});

Deno.test("no publish without subject", async () => {
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

Deno.test("pubsub", async () => {
  const lock = Lock();
  const subj = nuid.next();
  connect({ url: u })
    .then((nc) => {
      nc.subscribe(subj, async () => {
        await nc.close();
        lock.unlock();
      });
      nc.publish(subj);
    });
  await lock;
});

Deno.test("subscribe and unsubscribe", async () => {
  const subj = nuid.next();
  const nc = await connect({ url: u });
  const sub = nc.subscribe(subj, () => {}, { max: 1000, queue: "aaa" });

  // check the subscription
  assertEquals(nc.protocol.subscriptions.length, 1);
  let s = nc.protocol.subscriptions.get(1);
  assert(s);
  assertEquals(s.received, 0);
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
  assertEquals(s.received, 1);

  // verify cleanup
  sub.unsubscribe();
  assertEquals(nc.protocol.subscriptions.length, 0);
  await nc.close();
});

Deno.test("subscriptions fire callbacks", async () => {
  const lock = Lock();
  const nc = await connect({ url: u });
  const subj = nuid.next();
  nc.subscribe(subj, () => {
    lock.unlock();
  });
  nc.publish(subj);
  await nc.flush();
  await nc.close();
  await lock;
});

Deno.test("subscriptions pass exact subject to cb", async () => {
  const s = nuid.next();
  const subj = `${s}.foo.bar.baz`;
  const nc = await connect({ url: u });
  let received = "";
  nc.subscribe(`${s}.*.*.*`, (err, msg) => {
    received = msg.subject;
  });
  nc.publish(subj);
  await nc.flush();
  await nc.close();
  assertEquals(received, subj);
});

Deno.test("subscribe returns Subscription", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj, () => {});
  assertEquals(sub.sid, 1);
  await nc.close();
});

Deno.test("wildcard subscriptions", async () => {
  let single = 3;
  let partial = 2;
  let full = 5;

  let singleCounter = 0;
  let partialCounter = 0;
  let fullCounter = 0;

  let nc = await connect({ url: u });

  let s = nuid.next();
  nc.subscribe(`${s}.*`, () => {
    singleCounter++;
  });
  nc.subscribe(`${s}.foo.bar.*`, () => {
    partialCounter++;
  });
  nc.subscribe(`${s}.foo.>`, () => {
    fullCounter++;
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

Deno.test("correct data in message", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();

  let called = false;
  nc.subscribe(subj, (_, m) => {
    called = true;
    assertEquals(m.subject, subj);
    assertEquals(m.data, subj);
    assertEquals(m.reply, undefined);
  }, { max: 1 });

  nc.publish(subj, subj);
  await nc.flush();
  await nc.close();
  assert(called);
});

Deno.test("correct reply in message", async () => {
  let nc = await connect({ url: u });
  let s = nuid.next();
  let r = nuid.next();

  let rsubj = null;
  nc.subscribe(s, (_, m) => {
    rsubj = m.reply;
  });

  nc.publish(s, "", r);
  await nc.flush();
  await nc.close();
  assertEquals(rsubj, r);
});

Deno.test("respond throws if no reply subject set", async () => {
  let nc = await connect({ url: u });
  let s = nuid.next();

  let called = false;
  nc.subscribe(s, (_, m) => {
    try {
      m.respond();
      fail("should have not been able to respond");
    } catch (err) {
      assertEquals(err.code, ErrorCode.BAD_SUBJECT);
      called = true;
    }
  });

  nc.publish(s);
  await nc.flush();
  await nc.close();
  assert(called);
});

Deno.test("closed cannot subscribe", async () => {
  let nc = await connect({ url: u });
  await nc.close();
  let failed = false;
  try {
    nc.subscribe("foo", () => {});
    fail("should have not been able to subscribe");
  } catch (err) {
    failed = true;
  }
  assert(failed);
});

Deno.test("close cannot request", async () => {
  let nc = await connect({ url: u });
  nc.close();
  let failed = false;
  try {
    await nc.request("foo");
    fail("should have not been able to request");
  } catch (err) {
    failed = true;
  }
  assert(failed);
});

Deno.test("flush returns promise", async () => {
  const nc = await connect({ url: u });
  let p = nc.flush();
  if (!p) {
    fail("should have returned a promise");
  }
  await p;
  await nc.close();
});

Deno.test("unsubscribe after close", async () => {
  let nc = await connect({ url: u });
  let sub = nc.subscribe(nuid.next(), () => {});
  await nc.close();
  sub.unsubscribe();
});

Deno.test("unsubscribe stops messages", async () => {
  let received = 0;
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj, () => {
    received++;
    sub.unsubscribe();
  });
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);

  await nc.flush();
  assertEquals(received, 1);
  await nc.close();
});

Deno.test("request", async () => {
  let nc = await connect({ url: u });
  let s = nuid.next();
  nc.subscribe(s, (_, msg) => {
    msg.respond("foo");
  });
  let msg = await nc.request(s, 1000, "test");
  await nc.close();
  assertEquals(msg.data, "foo");
});

Deno.test("request timeout", async () => {
  const nc = await connect({ url: u });
  const s = nuid.next();
  let timedOut = false;
  try {
    await nc.request(s, 100, "test");
  } catch (err) {
    assertEquals(err.code, ErrorCode.CONNECTION_TIMEOUT);
    timedOut = true;
  }
  await nc.close();
  assert(timedOut);
});

Deno.test("close listener is called", async () => {
  const lock = Lock();
  const cs = new TestServer(false, (ca: Connection) => {
    setTimeout(() => {
      ca.close();
    }, 0);
  });
  const nc = await connect(
    { url: `https://localhost:${cs.getPort()}`, reconnect: false },
  );
  nc.addEventListener("close", async () => {
    lock.unlock();
  });

  await lock;
  await cs.stop();
  await nc.close();
});

Deno.test("error listener is called", async () => {
  const lock = Lock(1, 3000);
  const cs = new TestServer(false, (ca: Connection) => {
    setTimeout(async () => {
      await ca.write(new TextEncoder().encode("-ERR 'here'\r\n"));
    }, 500);
  });
  const nc = await connect({ url: `https://localhost:${cs.getPort()}` });
  nc.addEventListener("error", (err) => {
    const ne = err as NatsError;
    assertEquals(ne.message, "'here'");
    lock.unlock();
  });

  await lock;
  assertEquals(nc.isClosed(), true);
  await cs.stop();
});

Deno.test("subscription with timeout", async () => {
  const lock = Lock(1, 3000);
  const nc = await connect({ url: u });
  const sub = nc.subscribe(nuid.next(), () => {
  }, { max: 1 });

  sub.setTimeout(1000, () => {
    lock.unlock();
  });

  await nc.flush();
  await lock;
  await nc.close();
});

Deno.test("subscription expecting 2 fires timeout", async () => {
  const lock = Lock();
  const nc = await connect({ url: u });
  const subj = nuid.next();
  let c = 0;
  const sub = nc.subscribe(subj, () => {
    c++;
  }, { max: 2 });
  sub.setTimeout(100, () => {
    lock.unlock();
  });
  nc.publish(subj);
  await nc.flush();
  await lock;
  assertEquals(c, 1);
  await nc.close();
});

Deno.test("subscription timeout autocancels", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  let c = 0;
  const sub = nc.subscribe(subj, () => {
    c++;
  }, { max: 2 });
  sub.setTimeout(300, () => {
    fail();
  });
  nc.publish(subj);
  nc.publish(subj);
  await delay(500);
  assertEquals(c, 2);
  await nc.close();
});

Deno.test("subscription timeout cancel", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj, () => {}, { max: 2 });
  sub.setTimeout(100, () => {
    fail();
  });
  sub.cancelTimeout();
  const lock = Lock(1, 300);
  setTimeout(lock.unlock, 200);
  await lock;
  await nc.flush();
  await nc.close();
});

Deno.test("subscription received", async () => {
  let lock = Lock();
  let nc = await connect({ url: u });
  let subj = nuid.next();
  let sub = nc.subscribe(subj, () => {
    if (sub.getReceived() === 3) {
      lock.unlock();
    }
  });
  nc.publish(subj);
  nc.publish(subj);
  nc.publish(subj);

  await lock;
  await nc.close();
});

Deno.test("payload - json", async () => {
  const nc = await connect({ url: u, payload: Payload.JSON });
  await nc.close();
});

Deno.test("payload - binary", async () => {
  const nc = await connect({ url: u, payload: Payload.BINARY });
  await nc.close();
});

Deno.test("payload - string", async () => {
  const nc = await connect({ url: u, payload: Payload.STRING });
  await nc.close();
});

Deno.test("payload - test", async () => {
  await connect({ url: u, payload: "TEXT" as Payload })
    .then((nc: NatsConnection) => {
      fail("didn't expect to connect with bad payload");
      nc.close();
    })
    .catch(() => {
      // this is expected
    });
});

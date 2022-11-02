import {
  cleanup,
  initStream,
  jetstreamServerConf,
  setup,
} from "./jstest_util.ts";
import { assertEquals } from "https://deno.land/std@0.136.0/testing/asserts.ts";
import {
  AckPolicy,
  createInbox,
  deferred,
  DeliverPolicy,
  JsMsg,
  millis,
  NatsError,
  nuid,
  PubAck,
  StringCodec,
} from "../nats-base-client/mod.ts";
import { assert } from "../nats-base-client/denobuffer.ts";
import {
  assertExists,
  assertRejects,
} from "https://deno.land/std@0.125.0/testing/asserts.ts";
import { QueuedIterator } from "../nats-base-client/queued_iterator.ts";
import { connect } from "../src/mod.ts";
import { JetStreamReader } from "../nats-base-client/types.ts";
import { assertBetween } from "./helpers/mod.ts";
import { delay } from "../nats-base-client/util.ts";

Deno.test("consumer - create", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await assertRejects(
    async () => {
      await jsm.consumers.get(stream, "me");
    },
    Error,
    "consumer not found",
  );

  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, "me");
  assert(consumer);

  await cleanup(ns, nc);
});

Deno.test("consumer - rejects push consumer", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();

  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
    deliver_subject: "foo",
  });

  const consumer = await jsm.consumers.get(stream, "me");
  await assertRejects(
    async () => {
      await consumer.next();
    },
    Error,
    "consumer configuration is not a pull consumer",
  );

  await cleanup(ns, nc);
});

Deno.test("consumer - next", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, "me");

  await assertRejects(
    async () => {
      await consumer!.next();
    },
    Error,
    "no messages",
  );

  const js = nc.jetstream();
  await js.publish(subj);

  const m = await consumer.next();
  assertEquals(m.subject, subj);
  assertEquals(m.seq, 1);

  await cleanup(ns, nc);
});

Deno.test("consumer - info durable", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, "me");
  const info = await consumer.info();
  assertEquals(info.name, "me");
  assertEquals(info.stream_name, stream);

  await cleanup(ns, nc);
});

Deno.test("consumer - info ephemeral", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  const ci = await jsm.consumers.add(stream, {
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, ci.name);
  const info = await consumer.info();
  assertEquals(info.name, ci.name);
  assertEquals(info.stream_name, stream);

  await cleanup(ns, nc);
});

Deno.test("consumer - read push", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  const ci = await jsm.consumers.add(stream, {
    deliver_subject: createInbox(),
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, ci.name);

  const iter = await consumer.read() as QueuedIterator<JsMsg>;
  const d = deferred<JsMsg>();
  (async () => {
    for await (const m of iter) {
      m.ack();
      d.resolve(m);
      break;
    }
  })().then();

  const js = nc.jetstream();
  await js.publish(subj);

  const m = await d;
  assertEquals(m.subject, subj);

  await cleanup(ns, nc);
});

Deno.test("consumer - read pull", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const consumer = await jsm.consumers.get(stream, "me");

  const iter = await consumer.read() as QueuedIterator<JsMsg>;
  let interval = 0;
  const msgs: JsMsg[] = [];
  const d = deferred<JsMsg[]>();
  (async () => {
    for await (const m of iter) {
      m.ack();
      msgs.push(m);
      if (msgs.length === 10) {
        d.resolve(msgs);
        clearInterval(interval);
        break;
      }
    }
  })().then();

  const js = nc.jetstream();
  interval = setInterval(async () => {
    await js.publish(subj);
  }, 300);

  const m = await d;
  assertEquals(m.length, 10);

  await cleanup(ns, nc);
});

Deno.test("consumer - read pull callback", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const d = deferred<JsMsg[]>();
  const msgs: JsMsg[] = [];
  const consumer = await jsm.consumers.get(stream, "me");
  await consumer.read({
    callback: (m) => {
      m.ack();
      msgs.push(m);
      if (msgs.length >= 10) {
        d.resolve(msgs);
        clearInterval(interval);
      }
    },
  });

  const js = nc.jetstream();
  const interval = setInterval(async () => {
    await js.publish(subj);
  }, 300);

  const m = await d;
  assertEquals(m.length, 10);

  await cleanup(ns, nc);
});

Deno.test("consumer - read pull callback batch", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  let pulls = 0;
  nc.subscribe(`$JS.API.CONSUMER.MSG.NEXT.${stream}.me`, {
    callback: (_err, _msg) => {
      pulls++;
    },
  });

  const d = deferred<JsMsg[]>();
  const msgs: JsMsg[] = [];
  const consumer = await jsm.consumers.get(stream, "me");
  await consumer.read({
    inflight_limit: {
      batch: 2,
    },
    callback: (m) => {
      m.ack();
      msgs.push(m);
      if (msgs.length === 4) {
        d.resolve(msgs);
      }
    },
  });

  const js = nc.jetstream();
  const proms: Promise<PubAck>[] = [];
  proms.push(js.publish(subj, new Uint8Array(256)));
  proms.push(js.publish(subj, new Uint8Array(256)));
  proms.push(js.publish(subj, new Uint8Array(256)));
  proms.push(js.publish(subj, new Uint8Array(256)));

  const m = await d;
  assertEquals(m.length, 4);
  assertBetween(pulls, 2, 3);

  await cleanup(ns, nc);
});

Deno.test("consumer - read pull callback max_bytes", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true), {});
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const proms: Promise<PubAck>[] = [];
  const sc = StringCodec();
  proms.push(js.publish(subj, sc.encode("a".repeat(256))));
  proms.push(js.publish(subj, sc.encode("b".repeat(256))));
  proms.push(js.publish(subj, sc.encode("c".repeat(256))));
  proms.push(js.publish(subj, sc.encode("d".repeat(256))));
  proms.push(js.publish(subj, sc.encode("e".repeat(256))));
  proms.push(js.publish(subj, sc.encode("f".repeat(256))));

  let pulls = 0;
  nc.subscribe(`$JS.API.CONSUMER.MSG.NEXT.${stream}.me`, {
    callback: (_err, _msg) => {
      pulls++;
    },
  });

  const d = deferred<JsMsg[]>();
  const msgs: JsMsg[] = [];

  const consumer = await jsm.consumers.get(stream, "me");
  await consumer.read({
    inflight_limit: {
      max_bytes: 1024,
    },
    callback: (m) => {
      m.ack();
      msgs.push(m);
      if (msgs.length === 6) {
        d.resolve(msgs);
      }
    },
  });

  const m = await d;
  assertEquals(m.length, 6);
  assertBetween(pulls, 2, 7);

  await cleanup(ns, nc);
});

Deno.test("consumer - reader.stop()", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const msgs: JsMsg[] = [];
  const consumer = await jsm.consumers.get(stream, "me");
  const reader = await consumer.read({
    callback: (m) => {
      m.ack();
      msgs.push(m);
      reader.stop();
      clearInterval(interval);
    },
  }) as JetStreamReader;

  const js = nc.jetstream();
  const interval = setInterval(async () => {
    await js.publish(subj);
  }, 300);

  await reader.closed;
  assertEquals(msgs.length, 1);

  await cleanup(ns, nc);
});

Deno.test("consumer - exported consumer", async () => {
  const stream = nuid.next();
  const durable = `dur_${nuid.next()}`;

  const template = {
    accounts: {
      JS: {
        jetstream: "enabled",
        users: [{ user: "service", password: "service" }],
        exports: [
          {
            service: `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`,
            response: "stream",
            // accounts: ["A"],
          },
          {
            service: `$JS.ACK.${stream}.${durable}.>`,
            // accounts: ["A"],
          },
        ],
      },
      A: {
        users: [{ user: "a", password: "a" }],
        imports: [
          {
            service: {
              subject: `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`,
              account: "JS",
            },
            to: "next",
          },
          {
            service: {
              subject: `$JS.ACK.${stream}.${durable}.>`,
              account: "JS",
            },
          },
        ],
      },
    },
  };

  const { ns, nc } = await setup(jetstreamServerConf(template, true), {
    user: "service",
    pass: "service",
  });

  const srv = { jsm: await nc.jetstreamManager(), js: nc.jetstream() };

  await srv.jsm.streams.add({ name: stream, subjects: ["data.>"] });
  await srv.jsm.consumers.add(stream, {
    durable_name: durable,
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
  });

  const client = await connect({ port: ns.port, user: "a", pass: "a" });
  const js = client.jetstream();
  const ec = js.exportedConsumer("next");

  await assertRejects(
    async () => {
      await ec.next();
    },
    Error,
    "no messages",
  );

  const sc = StringCodec();
  for (let i = 0; i < 10; i++) {
    await srv.js.publish(`data.a`, sc.encode(`${i}`));
  }

  const m = await ec.next();
  assertExists(m);
  assertEquals(m.subject, "data.a");
  m.ack();

  const iter = await ec.read({
    inflight_limit: { batch: 2 },
  }) as QueuedIterator<JsMsg>;
  const done = (async () => {
    for await (const m of iter) {
      m.ack();
      if (m.seq === 10) {
        break;
      }
    }
  })();

  await done;

  await cleanup(ns, nc, client);
});

Deno.test("consumer - no messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
  });

  const consumer = await jsm.consumers.get(stream, "me");
  await consumer.read({
    inflight_limit: {
      idle_heartbeat: 500,
      expires: 1000,
    },
    callback: () => {},
  });
  await delay(3000);
  await cleanup(ns, nc);
});

Deno.test("consumer - push with callback", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
    deliver_subject: nuid.next(),
  });

  await assertRejects(async () => {
    const consumer = await jsm.consumers.get(stream, "me");
    await consumer.read({
      inflight_limit: {
        idle_heartbeat: 1000,
      },
      callback: () => {},
    });
  });

  await cleanup(ns, nc);
});

Deno.test("consumer - ephemeral", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const js = nc.jetstream();
  const consumer = await js.consumer(stream);
  const info = await consumer.info();
  assertEquals(millis(info.config.inactive_threshold!), 5000);

  await cleanup(ns, nc);
});

Deno.test("consumer - code sample", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  // consumers.add() creates a consumer and returns an info - this is the existing API since the epoch
  // IF major version bump, I would change this to return the actual consumer, as consumers have
  // a direct API to get info() on them, and since we are not doing anything yet, there are no
  // additional RPC calls.
  await jsm.consumers.add(stream, { durable_name: "A" });

  // retrieve the consumer
  const consumer = await jsm.consumers.get(stream, "A");

  // no options - returns an iterator with default buffering settings
  const iter = await consumer.read() as QueuedIterator<JsMsg>;
  for await (const m of iter) {
    // process messages
    console.log(m.subject);
    // if we are done, we can simply break
    break;
  }
  // some examples on options
  await consumer.read({
    inflight_limit: {
      batch: 1000,
      expires: 15000,
      idle_heartbeat: 5000,
    },
  });

  // a callback example looks almost exactly the same, except that instead
  // of an iterator, you get a JetStreamReader, which gives you stop(), and closed
  const reader = await consumer.read({
    callback: (m: JsMsg) => {
      console.log(m.subject);
    },
  }) as JetStreamReader;

  reader.closed.then((err: NatsError | null) => {
    if (err) {
      console.error(`Reader was closed with an error: ${err.message}`);
    } else {
      console.error(`Reader closed`);
    }
  });

  // stop the reader
  setTimeout(() => {
    reader.stop();
  }, 2000);

  await cleanup(ns, nc);
});

Deno.test("consumer - redundant API", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  // consumers.add() creates a consumer and returns an info - this is the existing API since the epoch
  // IF major version bump, I would change this to return the actual consumer, as consumers have
  // a direct API to get info() on them, and since we are not doing anything yet, there are no
  // additional RPC calls.
  await jsm.consumers.add(stream, { durable_name: "A" });

  // retrieve the consumer
  const consumer = await jsm.consumers.get(stream, "A");

  // next() WAS a polling API as originally defined - it was intended to return a single message
  while (true) {
    // wait for a message at most for one minute
    const m = await consumer.next({ expires: 60000 });
    console.log(m.subject);
    break;
  }

  // if we are asking for multiple messages, the above API is not any different in practice
  // to using read(), read() will buffer messages, and as necessary re-fetch additional messages
  // with the caveat that at most batch messages / or bytes will be left outstanding if the client
  // breaks from the loop - or the consumer could offer a `drain()` where it won't refresh
  // pulls, but when all requests close, terminates the iterator.
  const iter = await consumer.read({
    inflight_limit: { batch: 10 },
  }) as QueuedIterator<JsMsg>;
  for await (const m of iter) {
    console.log(m.subject);
    break;
  }

  await cleanup(ns, nc);
});

Deno.test("consumer - update", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  // consumers.add() creates a consumer and returns an info - this is the existing API since the epoch
  // IF major version bump, I would change this to return the actual consumer, as consumers have
  // a direct API to get info() on them, and since we are not doing anything yet, there are no
  // additional RPC calls.
  await jsm.consumers.add(stream, { durable_name: "A" });

  // retrieve the consumer
  const consumer = await jsm.consumers.get(stream, "A");
  let iter: QueuedIterator<JsMsg>;
  while (true) {
    iter = await consumer.fetch({ count: 10 });
    for await (const _m of iter) {
      // do something
    }
    // stop by not calling fetch after processing messages
    break;
  }

  // no options - returns an iterator with default buffering settings
  iter = await consumer.read() as QueuedIterator<JsMsg>;
  for await (const m of iter) {
    // process messages
    console.log(m.subject);
    // if we are done, we can simply break
    break;
  }
  // some examples on options
  await consumer.read({
    inflight_limit: {
      batch: 1000,
      expires: 15000,
      idle_heartbeat: 5000,
    },
  });

  // a callback example looks almost exactly the same, except that instead
  // of an iterator, you get a JetStreamReader, which gives you stop(), and closed
  const reader = await consumer.read({
    callback: (m: JsMsg) => {
      console.log(m.subject);
    },
  }) as JetStreamReader;

  reader.closed.then((err: NatsError | null) => {
    if (err) {
      console.error(`Reader was closed with an error: ${err.message}`);
    } else {
      console.error(`Reader closed`);
    }
  });

  // stop the reader
  setTimeout(() => {
    reader.stop();
  }, 2000);

  await cleanup(ns, nc);
});

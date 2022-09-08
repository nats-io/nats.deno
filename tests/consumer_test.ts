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
  nuid,
  StringCodec,
} from "../nats-base-client/mod.ts";
import { assert } from "../nats-base-client/denobuffer.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";
import { QueuedIterator } from "../nats-base-client/queued_iterator.ts";
import { connect } from "../src/mod.ts";
import { assertExists } from "https://deno.land/std@0.75.0/testing/asserts.ts";

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
      if (msgs.length >= 10) {
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
    inflight_limit: { messages: 2 },
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

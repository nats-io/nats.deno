/*
 * Copyright 2021-2023 The NATS Authors
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
  _setup,
  assertBetween,
  cleanup,
  jetstreamExportServerConf,
  jetstreamServerConf,
  notCompatible,
} from "../../test_helpers/mod.ts";
import { initStream } from "./jstest_util.ts";
import {
  connect,
  createInbox,
  DebugEvents,
  deferred,
  delay,
  Empty,
  ErrorCode,
  Events,
  JSONCodec,
  nanos,
  NatsError,
  nuid,
  StringCodec,
  syncIterator,
} from "jsr:@nats-io/nats-transport-deno@3.0.0-2";
import { consumerOpts, JsHeaders } from "../types.ts";

import type {
  ConsumerOpts,
  ConsumerOptsBuilderImpl,
  JetStreamSubscriptionInfoable,
  PubAck,
} from "../types.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
  assertExists,
  assertIsError,
  assertRejects,
} from "jsr:@std/assert";
import { callbackConsume } from "./jetstream_test.ts";
import {
  AckPolicy,
  DeliverPolicy,
  RetentionPolicy,
  StorageType,
} from "../jsapi_types.ts";
import type { JsMsg } from "../jsmsg.ts";
import { isFlowControlMsg, isHeartbeatMsg, Js409Errors } from "../jsutil.ts";
import type { JetStreamSubscriptionImpl } from "../jsclient.ts";
import { jetstream, jetstreamManager } from "../mod.ts";

Deno.test("jetstream - ephemeral push", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);
  const js = jetstream(nc);
  await js.publish(subj);

  const opts = {
    max: 1,
    config: { deliver_subject: createInbox() },
  } as ConsumerOpts;
  opts.callbackFn = callbackConsume();
  const sub = await js.subscribe(subj, opts);
  await sub.closed;
  assertEquals(sub.getProcessed(), 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - durable", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { stream, subj } = await initStream(nc);
  const js = jetstream(nc);
  await js.publish(subj);

  const opts = consumerOpts();
  opts.durable("me");
  opts.manualAck();
  opts.ackExplicit();
  opts.maxMessages(1);
  opts.deliverTo(createInbox());

  let sub = await js.subscribe(subj, opts);
  const done = await (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })();

  await done;
  assertEquals(sub.getProcessed(), 1);

  // consumer should exist
  const jsm = await jetstreamManager(nc);
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.name, "me");

  // delete the consumer
  sub = await js.subscribe(subj, opts);
  await sub.destroy();
  await assertRejects(
    async () => {
      await jsm.consumers.info(stream, "me");
    },
    Error,
    "consumer not found",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - queue error checks", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  if (await notCompatible(ns, nc, "2.3.5")) {
    return;
  }
  const { stream, subj } = await initStream(nc);
  const js = jetstream(nc);

  await assertRejects(
    async () => {
      const opts = consumerOpts();
      opts.durable("me");
      opts.deliverTo("x");
      opts.queue("x");
      opts.idleHeartbeat(1000);

      await js.subscribe(subj, opts);
    },
    Error,
    "jetstream idle heartbeat is not supported with queue groups",
    undefined,
  );

  await assertRejects(
    async () => {
      const opts = consumerOpts();
      opts.durable("me");
      opts.deliverTo("x");
      opts.queue("x");
      opts.flowControl();

      await js.subscribe(subj, opts);
    },
    Error,
    "jetstream flow control is not supported with queue groups",
    undefined,
  );

  const jsm = await jetstreamManager(nc);
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_group: "x",
    ack_policy: AckPolicy.Explicit,
    deliver_subject: "x",
  });

  await assertRejects(
    async () => {
      await js.subscribe(subj, {
        stream: stream,
        config: { durable_name: "me", deliver_group: "y" },
      });
    },
    Error,
    "durable requires queue group 'x'",
    undefined,
  );

  await jsm.consumers.add(stream, {
    durable_name: "memo",
    ack_policy: AckPolicy.Explicit,
    deliver_subject: "z",
  });

  await assertRejects(
    async () => {
      await js.subscribe(subj, {
        stream: stream,
        config: { durable_name: "memo", deliver_group: "y" },
      });
    },
    Error,
    "durable requires no queue group",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - max ack pending", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { stream, subj } = await initStream(nc);

  const jsm = await jetstreamManager(nc);
  const sc = StringCodec();
  const d = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
  const buf: Promise<PubAck>[] = [];
  const js = jetstream(nc);
  d.forEach((v) => {
    buf.push(js.publish(subj, sc.encode(v), { msgID: v }));
  });
  await Promise.all(buf);

  const consumers = await jsm.consumers.list(stream).next();
  assert(consumers.length === 0);

  const opts = consumerOpts();
  opts.maxAckPending(2);
  opts.maxMessages(10);
  opts.manualAck();
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  await (async () => {
    for await (const m of sub) {
      assert(
        sub.getPending() < 3,
        `didn't expect pending messages greater than 2`,
      );
      m.ack();
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("jetstream - subscribe - not attached callback", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { stream, subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 3 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 4 } });

  const opts = consumerOpts();
  opts.durable("me");
  opts.ackExplicit();
  opts.callback(callbackConsume(false));
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  const subin = sub as unknown as JetStreamSubscriptionInfoable;
  assert(subin.info);
  assertEquals(subin.info.attached, false);

  await delay(500);
  sub.unsubscribe();
  await nc.flush();

  const jsm = await jetstreamManager(nc);
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 5);
  assertEquals(ci.ack_floor.stream_seq, 5);

  await cleanup(ns, nc);
});

Deno.test("jetstream - subscribe - not attached non-durable", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 3 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 4 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.callback(callbackConsume());
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  const subin = sub as unknown as JetStreamSubscriptionInfoable;
  assert(subin.info);
  assertEquals(subin.info.attached, false);
  await delay(500);
  assertEquals(sub.getProcessed(), 5);
  sub.unsubscribe();

  await cleanup(ns, nc);
});

Deno.test("jetstream - autoack", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { stream, subj } = await initStream(nc);
  const jsm = await jetstreamManager(nc);

  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
    deliver_subject: createInbox(),
  });

  const js = jetstream(nc);
  await js.publish(subj);

  let ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 1);

  const sopts = consumerOpts();
  sopts.ackAll();
  sopts.durable("me");
  sopts.callback(() => {
    // nothing
  });
  sopts.maxMessages(1);
  const sub = await js.subscribe(subj, sopts);
  await sub.closed;

  await nc.flush();
  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.num_waiting, 0);
  assertEquals(ci.num_ack_pending, 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - subscribe - info", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 3 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 4 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.callback(callbackConsume());
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  await delay(250);
  const ci = await sub.consumerInfo();
  assertEquals(ci.delivered.stream_seq, 5);
  assertEquals(ci.ack_floor.stream_seq, 5);
  await sub.destroy();

  await assertRejects(
    async () => {
      await sub.consumerInfo();
    },
    Error,
    "consumer not found",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - deliver new", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 3 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 4 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.deliverNew();
  opts.maxMessages(1);
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  const done = (async () => {
    for await (const m of sub) {
      assertEquals(m.seq, 6);
    }
  })();
  await js.publish(subj, Empty, { expect: { lastSequence: 5 } });
  await done;
  await cleanup(ns, nc);
});

Deno.test("jetstream - deliver last", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 3 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 4 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.deliverLast();
  opts.maxMessages(1);
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  const done = (async () => {
    for await (const m of sub) {
      assertEquals(m.seq, 5);
    }
  })();
  await done;
  await cleanup(ns, nc);
});

Deno.test("jetstream - deliver seq", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 3 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 4 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.startSequence(2);
  opts.maxMessages(1);
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  const done = (async () => {
    for await (const m of sub) {
      assertEquals(m.seq, 2);
    }
  })();
  await done;
  await cleanup(ns, nc);
});

Deno.test("jetstream - deliver start time", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });

  await delay(1000);
  const now = new Date();
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.startTime(now);
  opts.maxMessages(1);
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  const done = (async () => {
    for await (const m of sub) {
      assertEquals(m.seq, 3);
    }
  })();
  await done;
  await cleanup(ns, nc);
});

Deno.test("jetstream - deliver last per subject", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  if (await notCompatible(ns, nc)) {
    return;
  }
  const jsm = await jetstreamManager(nc);
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add(
    { name: stream, subjects: [subj] },
  );

  const js = jetstream(nc);
  await js.publish(`${stream}.A`, Empty, { expect: { lastSequence: 0 } });
  await js.publish(`${stream}.B`, Empty, { expect: { lastSequence: 1 } });
  await js.publish(`${stream}.A`, Empty, { expect: { lastSequence: 2 } });
  await js.publish(`${stream}.B`, Empty, { expect: { lastSequence: 3 } });
  await js.publish(`${stream}.A`, Empty, { expect: { lastSequence: 4 } });
  await js.publish(`${stream}.B`, Empty, { expect: { lastSequence: 5 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.deliverLastPerSubject();
  opts.deliverTo(createInbox());

  const sub = await js.subscribe(subj, opts);
  const ci = await sub.consumerInfo();
  const buf: JsMsg[] = [];
  assertEquals(ci.num_ack_pending, 2);
  const done = (async () => {
    for await (const m of sub) {
      buf.push(m);
      if (buf.length === 2) {
        sub.unsubscribe();
      }
    }
  })();
  await done;
  assertEquals(buf[0].info.streamSequence, 5);
  assertEquals(buf[1].info.streamSequence, 6);
  await cleanup(ns, nc);
});

Deno.test("jetstream - cross account subscribe", async () => {
  const { ns, nc: admin } = await _setup(connect, jetstreamExportServerConf(), {
    user: "js",
    pass: "js",
  });

  // add a stream
  const { subj } = await initStream(admin);
  const adminjs = jetstream(admin);
  await adminjs.publish(subj);
  await adminjs.publish(subj);

  // create a durable config
  const bo = consumerOpts() as ConsumerOptsBuilderImpl;
  bo.durable("me");
  bo.manualAck();
  bo.maxMessages(2);
  bo.deliverTo(createInbox("A"));

  const nc = await connect({
    port: ns.port,
    user: "a",
    pass: "s3cret",
  });
  const js = jetstream(nc, { apiPrefix: "IPA" });

  const opts = bo.getOpts();
  const acks: Promise<boolean>[] = [];
  const d = deferred();
  const sub = await js.subscribe(subj, opts);
  await (async () => {
    for await (const m of sub) {
      acks.push(m.ackAck());
      if (m.seq === 2) {
        d.resolve();
      }
    }
  })();
  await d;
  await Promise.all(acks);
  const ci = await sub.consumerInfo();
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 2);
  assertEquals(ci.ack_floor.stream_seq, 2);
  await sub.destroy();
  await assertRejects(
    async () => {
      await sub.consumerInfo();
    },
    Error,
    "consumer not found",
    undefined,
  );

  await cleanup(ns, admin, nc);
});

Deno.test("jetstream - ack lease extends with working", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf());

  const sn = nuid.next();
  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({ name: sn, subjects: [`${sn}.>`] });

  const js = jetstream(nc);
  await js.publish(`${sn}.A`, Empty, { msgID: "1" });

  const inbox = createInbox();
  const cc = {
    "ack_wait": nanos(2000),
    "deliver_subject": inbox,
    "ack_policy": AckPolicy.Explicit,
    "durable_name": "me",
  };
  await jsm.consumers.add(sn, cc);

  const opts = consumerOpts();
  opts.durable("me");
  opts.manualAck();

  const sub = await js.subscribe(`${sn}.>`, opts);
  const done = (async () => {
    for await (const m of sub) {
      const timer = setInterval(() => {
        m.working();
      }, 750);
      // we got a message now we are going to delay for 31 sec
      await delay(15);
      const ci = await jsm.consumers.info(sn, "me");
      assertEquals(ci.num_ack_pending, 1);
      m.ack();
      clearInterval(timer);
      break;
    }
  })();

  await done;

  // make sure the message went out
  await nc.flush();
  const ci2 = await jsm.consumers.info(sn, "me");
  assertEquals(ci2.delivered.stream_seq, 1);
  assertEquals(ci2.num_redelivered, 0);
  assertEquals(ci2.num_ack_pending, 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - qsub", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  if (await notCompatible(ns, nc, "2.3.5")) {
    return;
  }
  const { subj } = await initStream(nc);
  const js = jetstream(nc);

  const opts = consumerOpts();
  opts.queue("q");
  opts.durable("n");
  opts.deliverTo("here");
  opts.callback((_err, m) => {
    if (m) {
      m.ack();
    }
  });

  const sub = await js.subscribe(subj, opts);
  const sub2 = await js.subscribe(subj, opts);

  for (let i = 0; i < 100; i++) {
    await js.publish(subj, Empty);
  }
  await nc.flush();
  await sub.drain();
  await sub2.drain();

  assert(sub.getProcessed() > 0);
  assert(sub2.getProcessed() > 0);
  assertEquals(sub.getProcessed() + sub2.getProcessed(), 100);

  await cleanup(ns, nc);
});

Deno.test("jetstream - qsub ackall", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  if (await notCompatible(ns, nc, "2.3.5")) {
    return;
  }
  const { stream, subj } = await initStream(nc);
  const js = jetstream(nc);

  const opts = consumerOpts();
  opts.queue("q");
  opts.durable("n");
  opts.deliverTo("here");
  opts.ackAll();
  opts.callback((_err, _m) => {});

  const sub = await js.subscribe(subj, opts);
  const sub2 = await js.subscribe(subj, opts);

  for (let i = 0; i < 100; i++) {
    await js.publish(subj, Empty);
  }
  await nc.flush();
  await sub.drain();
  await sub2.drain();

  assert(sub.getProcessed() > 0);
  assert(sub2.getProcessed() > 0);
  assertEquals(sub.getProcessed() + sub2.getProcessed(), 100);

  const jsm = await jetstreamManager(nc);
  const ci = await jsm.consumers.info(stream, "n");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.num_ack_pending, 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - idle heartbeats", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));

  const { stream, subj } = await initStream(nc);
  const js = jetstream(nc);
  await js.publish(subj);
  const jsm = await jetstreamManager(nc);
  const inbox = createInbox();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_subject: inbox,
    idle_heartbeat: nanos(2000),
  });

  const sub = nc.subscribe(inbox, {
    callback: (_err, msg) => {
      if (isHeartbeatMsg(msg)) {
        assertEquals(msg.headers?.get(JsHeaders.LastConsumerSeqHdr), "1");
        assertEquals(msg.headers?.get(JsHeaders.LastStreamSeqHdr), "1");
        sub.drain();
      }
    },
  });

  await sub.closed;
  await cleanup(ns, nc);
});

Deno.test("jetstream - flow control", async () => {
  const { ns, nc } = await _setup(
    connect,
    jetstreamServerConf({
      jetstream: {
        max_file: -1,
      },
    }),
  );
  const { stream, subj } = await initStream(nc);
  const data = new Uint8Array(1024 * 100);
  const js = jetstream(nc);
  const proms = [];
  for (let i = 0; i < 2000; i++) {
    proms.push(js.publish(subj, data));
    nc.publish(subj, data);
    if (proms.length % 100 === 0) {
      await Promise.all(proms);
      proms.length = 0;
    }
  }
  if (proms.length) {
    await Promise.all(proms);
  }
  await nc.flush();

  const jsm = await jetstreamManager(nc);
  const inbox = createInbox();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_subject: inbox,
    flow_control: true,
    idle_heartbeat: nanos(750),
  });

  const fc = deferred();
  const hb = deferred();
  const sub = nc.subscribe(inbox, {
    callback: (_err, msg) => {
      msg.respond();
      if (isFlowControlMsg(msg)) {
        fc.resolve();
      }
      if (isHeartbeatMsg(msg)) {
        hb.resolve();
      }
    },
  });

  await Promise.all([fc, hb]);
  sub.unsubscribe();
  await cleanup(ns, nc);
});

Deno.test("jetstream - durable resumes", async () => {
  let { ns, nc } = await _setup(connect, jetstreamServerConf({}), {
    maxReconnectAttempts: -1,
    reconnectTimeWait: 100,
  });

  const { stream, subj } = await initStream(nc);
  const jc = JSONCodec();
  const jsm = await jetstreamManager(nc);
  const js = jetstream(nc);
  let values = ["a", "b", "c"];
  for (const v of values) {
    await js.publish(subj, jc.encode(v));
  }

  const dsubj = createInbox();
  const opts = consumerOpts();
  opts.ackExplicit();
  opts.deliverAll();
  opts.deliverTo(dsubj);
  opts.durable("me");
  const sub = await js.subscribe(subj, opts);
  const done = (async () => {
    for await (const m of sub) {
      m.ack();
      if (m.seq === 6) {
        sub.unsubscribe();
      }
    }
  })();
  await nc.flush();
  await ns.stop();
  ns = await ns.restart();
  await delay(300);
  values = ["d", "e", "f"];
  for (const v of values) {
    await js.publish(subj, jc.encode(v));
  }
  await nc.flush();
  await done;

  const si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 6);
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.delivered.stream_seq, 6);
  assertEquals(ci.num_pending, 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - reuse consumer", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const id = nuid.next();
  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({
    subjects: [`${id}.*`],
    name: id,
    retention: RetentionPolicy.Workqueue,
  });

  await jsm.consumers.add(id, {
    "durable_name": "X",
    "deliver_subject": "out",
    "deliver_policy": DeliverPolicy.All,
    "ack_policy": AckPolicy.Explicit,
    "deliver_group": "queuea",
  });

  // second create should be OK, since it is idempotent
  await jsm.consumers.add(id, {
    "durable_name": "X",
    "deliver_subject": "out",
    "deliver_policy": DeliverPolicy.All,
    "ack_policy": AckPolicy.Explicit,
    "deliver_group": "queuea",
  });

  const js = jetstream(nc);
  const opts = consumerOpts();
  opts.ackExplicit();
  opts.durable("X");
  opts.deliverAll();
  opts.deliverTo("out2");
  opts.queue("queuea");

  const sub = await js.subscribe(`${id}.*`, opts);
  const ci = await sub.consumerInfo();
  // the deliver subject we specified should be ignored
  // the one specified by the consumer is used
  assertEquals(ci.config.deliver_subject, "out");

  await cleanup(ns, nc);
});

Deno.test("jetstream - nak delay", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  if (await notCompatible(ns, nc, "2.7.1")) {
    return;
  }

  const { subj } = await initStream(nc);
  const js = jetstream(nc);
  await js.publish(subj);

  let start = 0;

  const opts = consumerOpts();
  opts.ackAll();
  opts.deliverTo(createInbox());
  opts.maxMessages(2);
  opts.callback((_, m) => {
    assert(m);
    if (m.redelivered) {
      m.ack();
    } else {
      start = Date.now();
      m.nak(2000);
    }
  });

  const sub = await js.subscribe(subj, opts);
  await sub.closed;

  const delay = Date.now() - start;
  assertBetween(delay, 1800, 2200);
  await cleanup(ns, nc);
});

Deno.test("jetstream - redelivery property works", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  if (await notCompatible(ns, nc, "2.3.5")) {
    return;
  }
  const { stream, subj } = await initStream(nc);
  const js = jetstream(nc);

  let r = 0;

  const opts = consumerOpts();
  opts.ackAll();
  opts.queue("q");
  opts.durable("n");
  opts.deliverTo(createInbox());
  opts.callback((_err, m) => {
    if (m) {
      if (m.info.redelivered) {
        r++;
      }
      if (m.seq === 100) {
        m.ack();
      }
      if (m.seq % 3 === 0) {
        m.nak();
      }
    }
  });

  const sub = await js.subscribe(subj, opts);
  const sub2 = await js.subscribe(subj, opts);

  for (let i = 0; i < 100; i++) {
    await js.publish(subj, Empty);
  }
  await nc.flush();
  await sub.drain();
  await sub2.drain();

  assert(sub.getProcessed() > 0);
  assert(sub2.getProcessed() > 0);
  assert(r > 0);
  assert(sub.getProcessed() + sub2.getProcessed() > 100);

  await nc.flush();
  const jsm = await jetstreamManager(nc);
  const ci = await jsm.consumers.info(stream, "n");
  assert(ci.delivered.consumer_seq > 100);
  await cleanup(ns, nc);
});

Deno.test("jetstream - bind", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const js = jetstream(nc);
  const jsm = await jetstreamManager(nc);
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
  });

  const inbox = createInbox();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.None,
    deliver_subject: inbox,
  });

  const opts = consumerOpts();
  opts.bind(stream, "hello");
  opts.deliverTo(inbox);

  await assertRejects(
    async () => {
      await js.subscribe(subj, opts);
    },
    Error,
    `unable to bind - durable consumer hello doesn't exist in ${stream}`,
    undefined,
  );
  // the rejection happens and the unsub is scheduled, but it is possible that
  // the server didn't process it yet - flush to make sure the unsub was seen
  await nc.flush();

  opts.bind(stream, "me");
  const sub = await js.subscribe(subj, opts);
  assertEquals(sub.getProcessed(), 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - bind example", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const js = jetstream(nc);
  const jsm = await jetstreamManager(nc);
  const subj = `A.*`;
  await jsm.streams.add({
    name: "A",
    subjects: [subj],
  });

  const inbox = createInbox();
  await jsm.consumers.add("A", {
    durable_name: "me",
    ack_policy: AckPolicy.None,
    deliver_subject: inbox,
  });

  const opts = consumerOpts();
  opts.bind("A", "me");

  const sub = await js.subscribe(subj, opts);
  assertEquals(sub.getProcessed(), 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - push bound", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { stream, subj } = await initStream(nc);

  const jsm = await jetstreamManager(nc);
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_subject: "here",
  });

  const opts = consumerOpts();
  opts.durable("me");
  opts.manualAck();
  opts.ackExplicit();
  opts.deliverTo("here");
  opts.callback((_err, msg) => {
    if (msg) {
      msg.ack();
    }
  });
  const js = jetstream(nc);
  await js.subscribe(subj, opts);

  const nc2 = await connect({ port: ns.port });
  const js2 = jetstream(nc2);
  await assertRejects(
    async () => {
      await js2.subscribe(subj, opts);
    },
    Error,
    "duplicate subscription",
  );

  await cleanup(ns, nc, nc2);
});

Deno.test("jetstream - idleheartbeats errors repeat in callback push sub", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const js = jetstream(nc);
  await js.publish(subj, Empty);

  const buf: NatsError[] = [];

  const d = deferred<void>();
  const fn = (err: NatsError | null, _msg: JsMsg | null): void => {
    if (err) {
      buf.push(err);
      if (buf.length === 3) {
        d.resolve();
      }
    }
  };

  const opts = consumerOpts();
  opts.durable("me");
  opts.manualAck();
  opts.ackExplicit();
  opts.idleHeartbeat(800);
  opts.deliverTo(createInbox());
  opts.callback(fn);

  const sub = await js.subscribe(subj, opts) as JetStreamSubscriptionImpl;
  assert(sub.monitor);
  await delay(3000);
  sub.monitor._change(100, 0, 3);

  buf.forEach((err) => {
    assertIsError(err, NatsError, Js409Errors.IdleHeartbeatMissed);
  });

  assertEquals(sub.sub.isClosed(), false);

  await cleanup(ns, nc);
});

Deno.test("jetstream - idleheartbeats errors in iterator push sub", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { subj } = await initStream(nc);

  const opts = consumerOpts();
  opts.durable("me");
  opts.manualAck();
  opts.ackExplicit();
  opts.idleHeartbeat(800);
  opts.deliverTo(createInbox());

  const js = jetstream(nc);
  const sub = await js.subscribe(subj, opts) as JetStreamSubscriptionImpl;

  const d = deferred<NatsError>();
  (async () => {
    for await (const _m of sub) {
      // not going to get anything
    }
  })().catch((err) => {
    d.resolve(err);
  });
  assert(sub.monitor);
  await delay(1700);
  sub.monitor._change(100, 0, 1);
  const err = await d;
  assertIsError(err, Error, Js409Errors.IdleHeartbeatMissed);
  assertEquals(err.code, ErrorCode.JetStreamIdleHeartBeat);
  assertEquals(sub.sub.isClosed(), true);

  await cleanup(ns, nc);
});

Deno.test("jetstream - bind ephemeral can get consumer info", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { stream, subj } = await initStream(nc);

  const jsm = await jetstreamManager(nc);

  async function testEphemeral(deliverSubject = ""): Promise<void> {
    const ci = await jsm.consumers.add(stream, {
      ack_policy: AckPolicy.Explicit,
      inactive_threshold: nanos(5000),
      deliver_subject: deliverSubject,
    });

    const js = jetstream(nc);
    const opts = consumerOpts();
    opts.bind(stream, ci.name);
    const sub = deliverSubject
      ? await js.subscribe(subj, opts)
      : await js.pullSubscribe(subj, opts);
    const sci = await sub.consumerInfo();
    assertEquals(
      sci.name,
      ci.name,
      `failed getting ci for ${deliverSubject ? "push" : "pull"}`,
    );
  }

  await testEphemeral();
  await testEphemeral(createInbox());

  await cleanup(ns, nc);
});

Deno.test("jetstream - create ephemeral with config can get consumer info", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf({}));
  const { stream, subj } = await initStream(nc);
  const js = jetstream(nc);

  async function testEphemeral(deliverSubject = ""): Promise<void> {
    const opts = {
      stream,
      config: {
        ack_policy: AckPolicy.Explicit,
        deliver_subject: deliverSubject,
      },
    };
    const sub = deliverSubject
      ? await js.subscribe(subj, opts)
      : await js.pullSubscribe(subj, opts);
    const ci = await sub.consumerInfo();
    assert(
      ci.name,
      `failed getting name for ${deliverSubject ? "push" : "pull"}`,
    );
    assert(
      !ci.config.durable_name,
      `unexpected durable name for ${deliverSubject ? "push" : "pull"}`,
    );
  }

  await testEphemeral();
  await testEphemeral(createInbox());

  await cleanup(ns, nc);
});

Deno.test("jetstream - push on stopped server doesn't close client", async () => {
  let { ns, nc } = await _setup(connect, jetstreamServerConf(), {
    maxReconnectAttempts: -1,
  });
  const reconnected = deferred<void>();
  (async () => {
    let reconnects = 0;
    for await (const s of nc.status()) {
      switch (s.type) {
        case DebugEvents.Reconnecting:
          reconnects++;
          if (reconnects === 2) {
            ns.restart().then((s) => {
              ns = s;
            });
          }
          break;
        case Events.Reconnect:
          setTimeout(() => {
            reconnected.resolve();
          }, 1000);
          break;
        default:
          // nothing
      }
    }
  })().then();
  const jsm = await jetstreamManager(nc);
  const si = await jsm.streams.add({ name: nuid.next(), subjects: ["test"] });
  const { name: stream } = si.config;

  const js = jetstream(nc);

  await jsm.consumers.add(stream, {
    durable_name: "dur",
    ack_policy: AckPolicy.Explicit,
    deliver_subject: "bar",
  });

  const opts = consumerOpts().manualAck().deliverTo(nuid.next());
  const sub = await js.subscribe("test", opts);
  (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })().then();

  setTimeout(() => {
    ns.stop();
  }, 2000);

  await reconnected;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("jetstream - push heartbeat iter", async () => {
  let { ns, nc } = await _setup(connect, jetstreamServerConf(), {
    maxReconnectAttempts: -1,
  });

  const reconnected = deferred();
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === Events.Reconnect) {
        // if we reconnect, close the client
        reconnected.resolve();
      }
    }
  })().then();

  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({ name: "my-stream", subjects: ["test"] });

  const js = jetstream(nc);

  const opts = consumerOpts({ idle_heartbeat: nanos(500) }).ackExplicit()
    .deliverTo(nuid.next());
  const psub = await js.subscribe("test", opts);
  const done = assertRejects(
    async () => {
      for await (const m of psub) {
        m.ack();
      }
    },
    Error,
    "idle heartbeats missed",
  );

  await ns.stop();
  await done;

  ns = await ns.restart();
  // this here because otherwise get a resource leak error in the test
  await reconnected;
  await cleanup(ns, nc);
});

Deno.test("jetstream - push heartbeat callback", async () => {
  let { ns, nc } = await _setup(connect, jetstreamServerConf(), {
    maxReconnectAttempts: -1,
  });

  const reconnected = deferred();
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === Events.Reconnect) {
        // if we reconnect, close the client
        reconnected.resolve();
      }
    }
  })().then();

  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({ name: "my-stream", subjects: ["test"] });

  const js = jetstream(nc);
  const d = deferred();
  const opts = consumerOpts({ idle_heartbeat: nanos(500) }).ackExplicit()
    .deliverTo(nuid.next())
    .callback((err, m) => {
      if (err?.code === ErrorCode.JetStreamIdleHeartBeat) {
        d.resolve();
      }
      if (m) {
        m.ack();
      }
    });
  await js.subscribe("test", opts);
  await ns.stop();
  await d;

  ns = await ns.restart();
  // this here because otherwise get a resource leak error in the test
  await reconnected;
  await cleanup(ns, nc);
});

Deno.test("jetstream - push multi-subject filter", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf());
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }
  const name = nuid.next();
  const jsm = await jetstreamManager(nc);
  const js = jetstream(nc);
  await jsm.streams.add({ name, subjects: [`a.>`] });

  const opts = consumerOpts()
    .durable("me")
    .ackExplicit()
    .filterSubject("a.b")
    .filterSubject("a.c")
    .deliverTo(createInbox())
    .callback((_err, msg) => {
      msg?.ack();
    });

  const sub = await js.subscribe("a.>", opts);
  const ci = await sub.consumerInfo();
  assertExists(ci.config.filter_subjects);
  assertArrayIncludes(ci.config.filter_subjects, ["a.b", "a.c"]);

  await cleanup(ns, nc);
});

Deno.test("jetstream - push single filter", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf());
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }
  const name = nuid.next();
  const jsm = await jetstreamManager(nc);
  const js = jetstream(nc);
  await jsm.streams.add({ name, subjects: [`a.>`] });

  const opts = consumerOpts()
    .durable("me")
    .ackExplicit()
    .filterSubject("a.b")
    .deliverTo(createInbox())
    .callback((_err, msg) => {
      msg?.ack();
    });

  const sub = await js.subscribe("a.>", opts);
  const ci = await sub.consumerInfo();
  assertEquals(ci.config.filter_subject, "a.b");

  await cleanup(ns, nc);
});

Deno.test("jetstream - push sync", async () => {
  const { ns, nc } = await _setup(connect, jetstreamServerConf());
  const name = nuid.next();
  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({
    name,
    subjects: [name],
    storage: StorageType.Memory,
  });
  await jsm.consumers.add(name, {
    durable_name: name,
    ack_policy: AckPolicy.Explicit,
    deliver_subject: "here",
  });

  const js = jetstream(nc);

  await js.publish(name);
  await js.publish(name);

  const sub = await js.subscribe(name, consumerOpts().bind(name, name));
  const sync = syncIterator(sub);
  assertExists(await sync.next());
  assertExists(await sync.next());

  await cleanup(ns, nc);
});

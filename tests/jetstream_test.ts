/*
 * Copyright 2021 The NATS Authors
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
  cleanup,
  initStream,
  jetstreamExportServerConf,
  jetstreamServerConf,
  setup,
  time,
} from "./jstest_util.ts";
import {
  AckPolicy,
  ConsumerConfig,
  ConsumerOpts,
  consumerOpts,
  createInbox,
  delay,
  DeliverPolicy,
  Empty,
  ErrorCode,
  headers,
  JsHeaders,
  JsMsg,
  JsMsgCallback,
  JSONCodec,
  nanos,
  NatsConnectionImpl,
  NatsError,
  nuid,
  QueuedIterator,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assertEquals,
  assertThrows,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std@0.95.0/testing/asserts.ts";
import { assert } from "../nats-base-client/denobuffer.ts";
import { PubAck } from "../nats-base-client/types.ts";
import {
  JetStreamClientImpl,
  JetStreamSubscriptionInfoable,
} from "../nats-base-client/jsclient.ts";
import { defaultJsOptions } from "../nats-base-client/jsbaseclient_api.ts";
import { connect } from "../src/connect.ts";
import { ConsumerOptsBuilderImpl } from "../nats-base-client/jsconsumeropts.ts";
import { assertBetween, disabled, notCompatible } from "./helpers/mod.ts";
import {
  isFlowControlMsg,
  isHeartbeatMsg,
} from "../nats-base-client/jsutil.ts";

function callbackConsume(debug = false): JsMsgCallback {
  return (err: NatsError | null, jm: JsMsg | null) => {
    if (err) {
      switch (err.code) {
        case ErrorCode.JetStream408RequestTimeout:
        case ErrorCode.JetStream409MaxAckPendingExceeded:
        case ErrorCode.JetStream404NoMessages:
          return;
        default:
          fail(err.code);
      }
    }
    if (debug && jm) {
      console.dir(jm.info);
      if (jm.headers) {
        console.info(jm.headers.toString());
      }
    }
    if (jm) {
      jm.ack();
    }
  };
}

async function consume(iter: QueuedIterator<JsMsg>): Promise<JsMsg[]> {
  const buf: JsMsg[] = [];
  await (async () => {
    for await (const m of iter) {
      m.ack();
      buf.push(m);
    }
  })();
  return buf;
}

Deno.test("jetstream - default options", () => {
  const opts = defaultJsOptions();
  assertEquals(opts, { apiPrefix: "$JS.API", timeout: 5000 });
});

Deno.test("jetstream - default override timeout", () => {
  const opts = defaultJsOptions({ timeout: 1000 });
  assertEquals(opts, { apiPrefix: "$JS.API", timeout: 1000 });
});

Deno.test("jetstream - default override prefix", () => {
  const opts = defaultJsOptions({ apiPrefix: "$XX.API" });
  assertEquals(opts, { apiPrefix: "$XX.API", timeout: 5000 });
});

Deno.test("jetstream - options rejects empty prefix", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  assertThrows(() => {
    nc.jetstream({ apiPrefix: "" });
  });
  await cleanup(ns, nc);
});

Deno.test("jetstream - options removes trailing dot", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream({ apiPrefix: "hello." }) as JetStreamClientImpl;
  assertEquals(js.opts.apiPrefix, "hello");
  await cleanup(ns, nc);
});

Deno.test("jetstream - find stream throws when not found", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream() as JetStreamClientImpl;
  await assertThrowsAsync(
    async () => {
      await js.findStream("hello");
    },
    Error,
    "no stream matches subject",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - publish basic", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  let pa = await js.publish(subj);
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 1);

  pa = await js.publish(subj);
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 2);

  await cleanup(ns, nc);
});

Deno.test("jetstream - ackAck", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  const js = nc.jetstream();
  await js.publish(subj);

  const ms = await js.pull(stream, "me");
  assertEquals(await ms.ackAck(), true);
  assertEquals(await ms.ackAck(), false);
  await cleanup(ns, nc);
});

Deno.test("jetstream - publish id", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  const pa = await js.publish(subj, Empty, { msgID: "a" });
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 1);

  const jsm = await nc.jetstreamManager();
  const sm = await jsm.streams.getMessage(stream, { seq: 1 });
  assertEquals(sm.header.get("Nats-Msg-Id"), "a");

  await cleanup(ns, nc);
});

Deno.test("jetstream - publish require stream", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  await assertThrowsAsync(
    async () => {
      await js.publish(subj, Empty, { expect: { streamName: "xxx" } });
    },
    Error,
    "expected stream does not match",
  );

  const pa = await js.publish(subj, Empty, { expect: { streamName: stream } });
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - publish require last message id", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  let pa = await js.publish(subj, Empty, { msgID: "a" });
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 1);

  await assertThrowsAsync(
    async () => {
      await js.publish(subj, Empty, { msgID: "b", expect: { lastMsgID: "b" } });
    },
    Error,
    "wrong last msg id: a",
  );

  pa = await js.publish(subj, Empty, {
    msgID: "b",
    expect: { lastMsgID: "a" },
  });
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 2);

  await cleanup(ns, nc);
});

Deno.test("jetstream - get message last by subject", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  await jsm.streams.add({ name: stream, subjects: [`${stream}.*`] });

  const js = nc.jetstream();
  const sc = StringCodec();
  await js.publish(`${stream}.A`, sc.encode("a"));
  await js.publish(`${stream}.A`, sc.encode("aa"));
  await js.publish(`${stream}.B`, sc.encode("b"));
  await js.publish(`${stream}.B`, sc.encode("bb"));

  const sm = await jsm.streams.getMessage(stream, {
    last_by_subj: `${stream}.A`,
  });
  assertEquals(sc.decode(sm.data), "aa");

  await cleanup(ns, nc);
});

Deno.test("jetstream - publish require last sequence", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  await js.publish(subj, Empty);

  await assertThrowsAsync(
    async () => {
      await js.publish(subj, Empty, {
        msgID: "b",
        expect: { lastSequence: 2 },
      });
    },
    Error,
    "wrong last sequence: 1",
  );

  const pa = await js.publish(subj, Empty, {
    msgID: "b",
    expect: { lastSequence: 1 },
  });
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 2);

  await cleanup(ns, nc);
});

Deno.test("jetstream - publish require last sequence by subject", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  await jsm.streams.add({ name: stream, subjects: [`${stream}.*`] });

  const js = nc.jetstream();

  await js.publish(`${stream}.A`, Empty);
  await js.publish(`${stream}.B`, Empty);
  const pa = await js.publish(`${stream}.A`, Empty, {
    expect: { lastSubjectSequence: 1 },
  });
  for (let i = 0; i < 100; i++) {
    await js.publish(`${stream}.B`, Empty);
  }
  // this will only succeed if the last recording sequence for the subject matches
  await js.publish(`${stream}.A`, Empty, {
    expect: { lastSubjectSequence: pa.seq },
  });

  await cleanup(ns, nc);
});

Deno.test("jetstream - ephemeral push", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);
  const js = nc.jetstream();
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
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const js = nc.jetstream();
  await js.publish(subj);

  const opts = consumerOpts();
  opts.durable("me");
  opts.manualAck();
  opts.ackExplicit();
  opts.maxMessages(1);
  opts.deliverTo(createInbox());

  let sub = await js.subscribe(subj, opts);
  const done = (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })();

  await done;
  assertEquals(sub.getProcessed(), 1);

  // consumer should exist
  const jsm = await nc.jetstreamManager();
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.name, "me");

  // delete the consumer
  sub = await js.subscribe(subj, opts);
  await sub.destroy();
  await assertThrowsAsync(
    async () => {
      await jsm.consumers.info(stream, "me");
    },
    Error,
    "consumer not found",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - queue error checks", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.3.5")) {
    return;
  }
  const { stream, subj } = await initStream(nc);
  const js = nc.jetstream();

  await assertThrowsAsync(
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
  );

  await assertThrowsAsync(
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
  );

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_group: "x",
    ack_policy: AckPolicy.Explicit,
    deliver_subject: "x",
  });

  await assertThrowsAsync(
    async () => {
      await js.subscribe(subj, {
        stream: stream,
        config: { durable_name: "me", deliver_group: "y" },
      });
    },
    Error,
    "durable requires queue group 'x'",
  );

  await jsm.consumers.add(stream, {
    durable_name: "memo",
    ack_policy: AckPolicy.Explicit,
    deliver_subject: "z",
  });

  await assertThrowsAsync(
    async () => {
      await js.subscribe(subj, {
        stream: stream,
        config: { durable_name: "memo", deliver_group: "y" },
      });
    },
    Error,
    "durable requires no queue group",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull no messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  const js = nc.jetstream();
  await assertThrowsAsync(
    async () => {
      await js.pull(stream, "me");
    },
    Error,
    "no messages",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  const js = nc.jetstream();
  await js.publish(subj, Empty, { msgID: "a" });
  let ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 1);

  const jm = await js.pull(stream, "me");
  jm.ack();
  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 1);
  assertEquals(ci.ack_floor.stream_seq, 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch expires waits", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  const js = nc.jetstream();
  const start = Date.now();
  const iter = js.fetch(stream, "me", { expires: 1000 });
  await (async () => {
    for await (const m of iter) {
      // nothing
    }
  })();
  const elapsed = Date.now() - start;
  assertBetween(elapsed, 950, 1050);
  assertEquals(iter.getReceived(), 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch expires waits after initial", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  const js = nc.jetstream();
  await js.publish(subj, Empty);
  const start = Date.now();
  const iter = js.fetch(stream, "me", { expires: 1000, batch: 5 });
  await (async () => {
    for await (const m of iter) {
      // nothing
    }
  })();
  const elapsed = Date.now() - start;
  assertBetween(elapsed, 950, 1050);
  assertEquals(iter.getReceived(), 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch expires or no_wait is required", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  assertThrows(
    () => {
      js.fetch(stream, "me");
    },
    Error,
    "expires or no_wait is required",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch: no_wait with more left", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  await js.publish(subj);
  await js.publish(subj);

  const iter = js.fetch(stream, "me", { no_wait: true });
  await consume(iter);

  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch some messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  // try to get messages = none available
  let sub = await js.fetch(stream, "me", { batch: 2, no_wait: true });
  await (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })();
  assertEquals(sub.getProcessed(), 0);

  // seed some messages
  await js.publish(subj, Empty, { msgID: "a" });
  await js.publish(subj, Empty, { msgID: "b" });
  await js.publish(subj, Empty, { msgID: "c" });

  // try to get 2 messages - OK
  sub = await js.fetch(stream, "me", { batch: 2, no_wait: true });
  await (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })();
  assertEquals(sub.getProcessed(), 2);
  let ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 1);
  assertEquals(ci.delivered.stream_seq, 2);
  assertEquals(ci.ack_floor.stream_seq, 2);

  // try to get 2 messages - OK, but only gets 1
  sub = await js.fetch(stream, "me", { batch: 2, no_wait: true });
  await (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })();
  assertEquals(sub.getProcessed(), 1);
  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 3);
  assertEquals(ci.ack_floor.stream_seq, 3);

  // try to get 2 messages - OK, none available
  sub = await js.fetch(stream, "me", { batch: 2, no_wait: true });
  await (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })();
  assertEquals(sub.getProcessed(), 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - max ack pending", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  const sc = StringCodec();
  const d = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
  const buf: Promise<PubAck>[] = [];
  const js = nc.jetstream();
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

Deno.test("jetstream - pull sub - attached iterator", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const jc = JSONCodec<number>();

  let sum = 0;
  const opts = consumerOpts();
  opts.durable("me");

  const js = nc.jetstream();
  const sub = await js.pullSubscribe(subj, opts);
  (async () => {
    for await (const msg of sub) {
      assert(msg);
      const n = jc.decode(msg.data);
      sum += n;
      msg.ack();
    }
  })().then();
  sub.pull({ expires: 500, batch: 5 });

  const subin = sub as unknown as JetStreamSubscriptionInfoable;
  assert(subin.info);
  assertEquals(subin.info.attached, true);
  await delay(250);
  assertEquals(sum, 0);

  let ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 0);
  assertEquals(ci.ack_floor.stream_seq, 0);

  await js.publish(subj, jc.encode(1), { msgID: "1" });
  await js.publish(subj, jc.encode(2), { msgID: "2" });
  sub.pull({ expires: 500, batch: 5 });
  await delay(500);
  assertEquals(sum, 3);

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 2);
  assertEquals(ci.ack_floor.stream_seq, 2);

  await js.publish(subj, jc.encode(3), { msgID: "3" });
  await js.publish(subj, jc.encode(5), { msgID: "4" });
  sub.pull({ expires: 500, batch: 5 });
  await delay(1000);
  assertEquals(sum, 11);

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 4);
  assertEquals(ci.ack_floor.stream_seq, 4);

  await js.publish(subj, jc.encode(7), { msgID: "5" });

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull sub - attached callback", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const jc = JSONCodec<number>();

  let sum = 0;
  const opts = consumerOpts();
  opts.durable("me");

  opts.callback((err, msg) => {
    if (err) {
      switch (err.code) {
        case ErrorCode.JetStream408RequestTimeout:
        case ErrorCode.JetStream409MaxAckPendingExceeded:
        case ErrorCode.JetStream404NoMessages:
          return;
        default:
          fail(err.code);
      }
    }
    if (msg) {
      const n = jc.decode(msg.data);
      sum += n;
      msg.ack();
    }
  });

  const js = nc.jetstream();
  const sub = await js.pullSubscribe(subj, opts);
  sub.pull({ expires: 500, batch: 5 });
  const subin = sub as unknown as JetStreamSubscriptionInfoable;
  assert(subin.info);
  assertEquals(subin.info.attached, true);
  await delay(250);
  assertEquals(sum, 0);

  let ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 0);
  assertEquals(ci.ack_floor.stream_seq, 0);

  await js.publish(subj, jc.encode(1), { msgID: "1" });
  await js.publish(subj, jc.encode(2), { msgID: "2" });
  sub.pull({ expires: 500, batch: 5 });
  await delay(500);
  assertEquals(sum, 3);

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 2);
  assertEquals(ci.ack_floor.stream_seq, 2);

  await js.publish(subj, jc.encode(3), { msgID: "3" });
  await js.publish(subj, jc.encode(5), { msgID: "4" });
  sub.pull({ expires: 500, batch: 5 });
  await delay(1000);
  assertEquals(sum, 11);

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 4);
  assertEquals(ci.ack_floor.stream_seq, 4);

  await js.publish(subj, jc.encode(7), { msgID: "5" });

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull sub - not attached callback", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  await js.publish(subj);

  const opts = consumerOpts();
  opts.durable("me");
  opts.ackExplicit();
  opts.maxMessages(1);
  opts.callback(callbackConsume(false));

  const sub = await js.pullSubscribe(subj, opts);
  sub.pull();
  const subin = sub as unknown as JetStreamSubscriptionInfoable;
  assert(subin.info);
  assertEquals(subin.info.attached, false);
  await sub.closed;

  const jsm = await nc.jetstreamManager();
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 1);
  assertEquals(ci.ack_floor.stream_seq, 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull sub requires explicit", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);

  const js = nc.jetstream();

  await assertThrowsAsync(
    async () => {
      const opts = consumerOpts();
      opts.durable("me");
      opts.ackAll();
      await js.pullSubscribe(subj, opts);
    },
    Error,
    "ack policy for pull",
  );
  await cleanup(ns, nc);
});

Deno.test("jetstream - subscribe - not attached callback", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
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

  const jsm = await nc.jetstreamManager();
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 5);
  assertEquals(ci.ack_floor.stream_seq, 5);

  await cleanup(ns, nc);
});

Deno.test("jetstream - subscribe - not attached non-durable", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);

  const js = nc.jetstream();
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

Deno.test("jetstream - fetch none - breaks after expires", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const sw = time();
  const batch = js.fetch(stream, "me", {
    batch: 10,
    expires: 1000,
  });
  const done = (async () => {
    for await (const m of batch) {
      console.log(m.info);
      fail("expected no messages");
    }
  })();

  await done;
  sw.mark();
  sw.assertInRange(1000);
  assertEquals(batch.getReceived(), 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch none - no wait breaks fast", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const sw = time();
  const batch = js.fetch(stream, "me", {
    batch: 10,
    no_wait: true,
  });
  const done = (async () => {
    for await (const m of batch) {
      m.ack();
    }
  })();

  await done;
  sw.mark();
  assert(25 > sw.duration());
  assertEquals(batch.getReceived(), 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch one - no wait breaks fast", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  await js.publish(subj);

  const sw = time();
  const batch = js.fetch(stream, "me", {
    batch: 10,
    no_wait: true,
  });
  const done = (async () => {
    for await (const m of batch) {
      m.ack();
    }
  })();

  await done;
  sw.mark();
  assert(25 > sw.duration());
  assertEquals(batch.getReceived(), 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch none - cancel timers", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const sw = time();
  const batch = js.fetch(stream, "me", {
    batch: 10,
    expires: 1000,
  });
  const done = (async () => {
    for await (const m of batch) {
      m.ack();
    }
  })();

  const nci = nc as NatsConnectionImpl;
  const last = nci.protocol.subscriptions.sidCounter;
  const sub = nci.protocol.subscriptions.get(last);
  assert(sub);
  sub.unsubscribe();

  await done;
  sw.mark();
  assert(25 > sw.duration());
  assertEquals(batch.getReceived(), 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch one - breaks after expires", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  nc.publish(subj);

  const js = nc.jetstream();

  const sw = time();
  const batch = js.fetch(stream, "me", {
    batch: 10,
    expires: 1000,
  });
  const done = (async () => {
    for await (const m of batch) {
      m.ack();
    }
  })();

  await done;
  sw.mark();
  sw.assertInRange(1000);
  assertEquals(batch.getReceived(), 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull consumer info without pull", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();

  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  await js.publish(subj);

  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 1);

  const sopts = consumerOpts();
  sopts.durable("me");
  await assertThrowsAsync(
    async () => {
      await js.subscribe(subj, sopts);
    },
    Error,
    "consumer info specifies a pull consumer",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - autoack", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();

  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
    deliver_subject: createInbox(),
  });

  const js = nc.jetstream();
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

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.num_waiting, 0);
  assertEquals(ci.num_ack_pending, 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - subscribe - info", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);

  const js = nc.jetstream();
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

  await assertThrowsAsync(
    async () => {
      await sub.consumerInfo();
    },
    Error,
    "consumer not found",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - deliver new", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);

  const js = nc.jetstream();
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
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);

  const js = nc.jetstream();
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

Deno.test("jetstream - last of", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const jsm = await nc.jetstreamManager();
  const n = nuid.next();
  await jsm.streams.add({
    name: n,
    subjects: [`${n}.>`],
  });

  const subja = `${n}.A`;
  const subjb = `${n}.B`;

  const js = nc.jetstream();

  await js.publish(subja, Empty);
  await js.publish(subjb, Empty);
  await js.publish(subjb, Empty);
  await js.publish(subja, Empty);

  const opts = {
    durable_name: "B",
    filter_subject: subjb,
    deliver_policy: DeliverPolicy.Last,
    ack_policy: AckPolicy.Explicit,
  } as Partial<ConsumerConfig>;

  await jsm.consumers.add(n, opts);
  const m = await js.pull(n, "B");
  assertEquals(m.seq, 3);

  await cleanup(ns, nc);
});

Deno.test("jetstream - deliver seq", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);

  const js = nc.jetstream();
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
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);

  const js = nc.jetstream();
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
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc)) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add(
    { name: stream, subjects: [subj] },
  );

  const js = nc.jetstream();
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
  const { ns, nc: admin } = await setup(
    jetstreamExportServerConf(),
    {
      user: "js",
      pass: "js",
    },
  );

  // add a stream
  const { subj } = await initStream(admin);
  const adminjs = admin.jetstream();
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
  const js = nc.jetstream({ apiPrefix: "IPA" });

  const opts = bo.getOpts();
  const sub = await js.subscribe(subj, opts);
  await (async () => {
    for await (const m of sub) {
      m.ack();
    }
  })();
  const ci = await sub.consumerInfo();
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 2);
  await sub.destroy();
  await assertThrowsAsync(
    async () => {
      await sub.consumerInfo();
    },
    Error,
    "consumer not found",
  );

  await cleanup(ns, admin, nc);
});

Deno.test("jetstream - cross account pull subscribe", () => {
  disabled("cross account pull subscribe test needs updating");
  // const { ns, nc: admin } = await setup(
  //   jetstreamExportServerConf(),
  //   {
  //     user: "js",
  //     pass: "js",
  //   },
  // );
  //
  // // add a stream
  // const { stream, subj } = await initStream(admin);
  // const adminjs = admin.jetstream();
  // await adminjs.publish(subj);
  // await adminjs.publish(subj);
  //
  // // FIXME: create a durable config
  // const bo = consumerOpts() as ConsumerOptsBuilderImpl;
  // bo.manualAck();
  // bo.ackExplicit();
  // bo.maxMessages(2);
  // bo.durable("me");
  //
  // // pull subscriber stalls
  // const nc = await connect({
  //   port: ns.port,
  //   user: "a",
  //   pass: "s3cret",
  //   inboxPrefix: "A",
  // });
  // const js = nc.jetstream({ apiPrefix: "IPA" });
  //
  // const opts = bo.getOpts();
  // const sub = await js.pullSubscribe(subj, opts);
  // const done = (async () => {
  //   for await (const m of sub) {
  //     m.ack();
  //   }
  // })();
  // sub.pull({ batch: 2 });
  // await done;
  // assertEquals(sub.getProcessed(), 2);
  //
  // const ci = await sub.consumerInfo();
  // assertEquals(ci.num_pending, 0);
  // assertEquals(ci.delivered.stream_seq, 2);
  //
  // await sub.destroy();
  // await assertThrowsAsync(
  //   async () => {
  //     await sub.consumerInfo();
  //   },
  //   Error,
  //   "consumer not found",
  // );
  //
  // await cleanup(ns, admin, nc);
});

Deno.test("jetstream - cross account pull", async () => {
  const { ns, nc: admin } = await setup(
    jetstreamExportServerConf(),
    {
      user: "js",
      pass: "js",
    },
  );

  // add a stream
  const { stream, subj } = await initStream(admin);
  const admjs = admin.jetstream();
  await admjs.publish(subj);
  await admjs.publish(subj);

  const admjsm = await admin.jetstreamManager();

  // create a durable config
  const bo = consumerOpts() as ConsumerOptsBuilderImpl;
  bo.manualAck();
  bo.ackExplicit();
  bo.durable("me");
  const opts = bo.getOpts();
  await admjsm.consumers.add(stream, opts.config);

  const nc = await connect({
    port: ns.port,
    user: "a",
    pass: "s3cret",
    inboxPrefix: "A",
  });

  // the api prefix is not used for pull/fetch()
  const js = nc.jetstream({ apiPrefix: "IPA" });
  let msg = await js.pull(stream, "me");
  assertEquals(msg.seq, 1);
  msg = await js.pull(stream, "me");
  assertEquals(msg.seq, 2);
  await assertThrowsAsync(
    async () => {
      await js.pull(stream, "me");
    },
    Error,
    "no messages",
  );

  await cleanup(ns, admin, nc);
});

Deno.test("jetstream - publish headers", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  const js = nc.jetstream();
  const h = headers();
  h.set("a", "b");

  await js.publish(subj, Empty, { headers: h });
  const ms = await js.pull(stream, "me");
  ms.ack();
  assertEquals(ms.headers!.get("a"), "b");
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull stream doesn't exist", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const js = nc.jetstream({ timeout: 1000 });
  await assertThrowsAsync(
    async () => {
      await js.pull("helloworld", "me");
    },
    Error,
    ErrorCode.Timeout,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull consumer doesn't exist", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream } = await initStream(nc);
  const js = nc.jetstream({ timeout: 1000 });
  await assertThrowsAsync(
    async () => {
      await js.pull(stream, "me");
    },
    Error,
    ErrorCode.Timeout,
  );

  await cleanup(ns, nc);
});

// Deno.test("jetstream - cross account fetch", async () => {
//   const { ns, nc: admin } = await setup(
//     jetstreamExportServerConf(),
//     {
//       user: "js",
//       pass: "js",
//     },
//   );
//
//   // add a stream
//   const { stream, subj } = await initStream(admin);
//   const admjs = admin.jetstream();
//   await admjs.publish(subj, Empty, {msgID: "1"});
//   await admjs.publish(subj, Empty, {msgID: "2"});
//
//   const admjsm = await admin.jetstreamManager();
//
//   // create a durable config
//   const bo = consumerOpts() as ConsumerOptsBuilderImpl;
//   bo.manualAck();
//   bo.ackExplicit();
//   bo.durable("me");
//   bo.maxAckPending(10);
//   const opts = bo.getOpts();
//   await admjsm.consumers.add(stream, opts.config);
//
//   const nc = await connect({
//     port: ns.port,
//     user: "a",
//     pass: "s3cret",
//     inboxPrefix: "A",
//     debug: true,
//   });
//
//   // the api prefix is not used for pull/fetch()
//   const js = nc.jetstream({ apiPrefix: "IPA" });
//   let iter = js.fetch(stream, "me", { batch: 20, expires: 1000 });
//   const msgs = await consume(iter);
//
//   assertEquals(msgs.length, 2);
//
//   // msg = await js.pull(stream, "me");
//   // assertEquals(msg.seq, 2);
//   // await assertThrowsAsync(async () => {
//   //   await js.pull(stream, "me");
//   // }, Error, "No Messages");
//
//   await cleanup(ns, admin, nc);
// });

Deno.test("jetstream - pull consumer doesn't exist", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { stream } = await initStream(nc);
  const js = nc.jetstream({ timeout: 1000 });
  await assertThrowsAsync(
    async () => {
      await js.pull(stream, "me");
    },
    Error,
    ErrorCode.Timeout,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - ack lease extends with working", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());

  const sn = nuid.next();
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: sn, subjects: [`${sn}.>`] });

  const js = nc.jetstream();
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

Deno.test("jetstream - JSON", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jc = JSONCodec();
  const js = nc.jetstream();
  const values = [undefined, null, true, "", ["hello"], { hello: "world" }];
  for (const v of values) {
    await js.publish(subj, jc.encode(v));
  }

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  for (let v of values) {
    const m = await js.pull(stream, "me");
    m.ack();
    // JSON doesn't serialize undefines, but if passed to the encoder
    // it becomes a null
    if (v === undefined) {
      v = null;
    }
    assertEquals(jc.decode(m.data), v);
  }
  await cleanup(ns, nc);
});

Deno.test("jetstream - qsub", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.3.5")) {
    return;
  }
  const { subj } = await initStream(nc);
  const js = nc.jetstream();

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

Deno.test("jetstream - idle heartbeats", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const { stream, subj } = await initStream(nc);
  nc.publish(subj);
  const jsm = await nc.jetstreamManager();
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
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  for (let i = 0; i < 10000; i++) {
    nc.publish(subj, Empty);
  }
  await nc.flush();

  const jsm = await nc.jetstreamManager();
  const inbox = createInbox();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_subject: inbox,
    flow_control: true,
    idle_heartbeat: nanos(750),
  });

  let fc = 0;
  const sub = nc.subscribe(inbox, {
    callback: (_err, msg) => {
      if (isHeartbeatMsg(msg)) {
        sub.drain();
        return;
      }
      if (isFlowControlMsg(msg)) {
        fc++;
      }
      msg.respond();
    },
  });

  await sub.closed;
  assert(fc > 0);
  assertEquals(sub.getProcessed(), 10000 + fc + 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - domain", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({
      jetstream: {
        domain: "afton",
      },
    }, true),
  );

  const jsm = await nc.jetstreamManager({ domain: "afton" });
  const ai = await jsm.getAccountInfo();
  assert(ai.domain, "afton");
  //@ts-ignore: internal use
  assertEquals(jsm.prefix, `$JS.afton.API`);
  await cleanup(ns, nc);
});

Deno.test("jetstream - account domain", async () => {
  const conf = jetstreamServerConf({
    jetstream: {
      domain: "A",
    },
    accounts: {
      A: {
        users: [
          { user: "a", password: "a" },
        ],
        jetstream: { max_memory: 10000, max_file: 10000 },
      },
    },
  }, true);

  const { ns, nc } = await setup(conf, { user: "a", pass: "a" });

  const jsm = await nc.jetstreamManager({ domain: "A" });
  const ai = await jsm.getAccountInfo();
  assert(ai.domain, "A");
  //@ts-ignore: internal use
  assertEquals(jsm.prefix, `$JS.A.API`);
  await cleanup(ns, nc);
});

Deno.test("jetstream - durable resumes", async () => {
  let { ns, nc } = await setup(jetstreamServerConf({}, true), {
    maxReconnectAttempts: -1,
    reconnectTimeWait: 100,
  });

  const { stream, subj } = await initStream(nc);
  const jc = JSONCodec();
  const jsm = await nc.jetstreamManager();
  const js = nc.jetstream();
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

Deno.test("jetstream - puback domain", async () => {
  const { ns, nc } = await setup(
    jetstreamServerConf({
      jetstream: {
        domain: "A",
      },
    }, true),
  );

  if (await notCompatible(ns, nc, "2.3.5")) {
    return;
  }

  const { subj } = await initStream(nc);
  const js = nc.jetstream();
  const pa = await js.publish(subj);
  assertEquals(pa.domain, "A");
  await cleanup(ns, nc);
});

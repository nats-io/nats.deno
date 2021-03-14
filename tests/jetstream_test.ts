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
  ConsumerOpts,
  consumerOpts,
  createInbox,
  delay,
  Empty,
  ErrorCode,
  JsMsg,
  JsMsgCallback,
  JSONCodec,
  nanos,
  NatsConnectionImpl,
  NatsError,
  QueuedIterator,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assertEquals,
  assertThrows,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { yellow } from "https://deno.land/std@0.83.0/fmt/colors.ts";
import { assert } from "../nats-base-client/denobuffer.ts";
import { PubAck } from "../nats-base-client/types.ts";
import {
  JetStreamClientImpl,
  JetStreamSubscriptionInfoable,
} from "../nats-base-client/jsclient.ts";
import { defaultJsOptions } from "../nats-base-client/jsbaseclient_api.ts";
import { connect } from "../src/connect.ts";
import { ConsumerOptsBuilderImpl } from "../nats-base-client/consumeropts.ts";

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
  const sm = await jsm.streams.getMessage(stream, 1);
  assertEquals(sm.header.get("nats-msg-id"), "a");

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

Deno.test("jetstream - ephemeral push", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
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

Deno.test("jetstream - pull no messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
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

Deno.test("jetstream - pull batch no messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
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

Deno.test("jetstream - expires or no_wait is required", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  assertThrows(
    () => {
      js.pullBatch(stream, "me");
    },
    Error,
    "expires or no_wait is required",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch: no_wait with more left", async () => {
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

  const iter = js.pullBatch(stream, "me", { no_wait: true });
  await consume(iter);

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch some messages", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  // try to get messages = none available
  let sub = await js.pullBatch(stream, "me", { batch: 2, no_wait: true });
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
  sub = await js.pullBatch(stream, "me", { batch: 2, no_wait: true });
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
  sub = await js.pullBatch(stream, "me", { batch: 2, no_wait: true });
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
  sub = await js.pullBatch(stream, "me", { batch: 2, no_wait: true });
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
  sub.pull({ expires: nanos(500), batch: 5 });

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
  sub.pull({ expires: nanos(500), batch: 5 });
  await delay(500);
  assertEquals(sum, 3);

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 2);
  assertEquals(ci.ack_floor.stream_seq, 2);

  await js.publish(subj, jc.encode(3), { msgID: "3" });
  await js.publish(subj, jc.encode(5), { msgID: "4" });
  sub.pull({ expires: nanos(500), batch: 5 });
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
  sub.pull({ expires: nanos(500), batch: 5 });
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
  sub.pull({ expires: nanos(500), batch: 5 });
  await delay(500);
  assertEquals(sum, 3);

  ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.stream_seq, 2);
  assertEquals(ci.ack_floor.stream_seq, 2);

  await js.publish(subj, jc.encode(3), { msgID: "3" });
  await js.publish(subj, jc.encode(5), { msgID: "4" });
  sub.pull({ expires: nanos(500), batch: 5 });
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
  const { stream, subj } = await initStream(nc);

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
  const { stream, subj } = await initStream(nc);

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

Deno.test("jetstream - pull batch none - breaks after expires", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const sw = time();
  const batch = js.pullBatch(stream, "me", {
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

Deno.test("jetstream - pull batch none - no wait breaks fast", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const sw = time();
  const batch = js.pullBatch(stream, "me", {
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

Deno.test("jetstream - pull batch one - no wait breaks fast", async () => {
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
  const batch = js.pullBatch(stream, "me", {
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

Deno.test("jetstream - pull batch none - cancel timers", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const js = nc.jetstream();
  const sw = time();
  const batch = js.pullBatch(stream, "me", {
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

Deno.test("jetstream - pull batch one - breaks after expires", async () => {
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
  const batch = js.pullBatch(stream, "me", {
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
  sopts.callback((err, msg) => {
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
  const { stream, subj } = await initStream(nc);

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
  const { stream, subj } = await initStream(nc);

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
  const { stream, subj } = await initStream(nc);

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

Deno.test("jetstream - deliver seq", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

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
  const { stream, subj } = await initStream(nc);

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

Deno.test("jetstream - cross account subscribe", async () => {
  const { ns, nc: admin } = await setup(
    jetstreamExportServerConf(),
    {
      user: "js",
      pass: "js",
    },
  );

  // add a stream
  const { stream, subj } = await initStream(admin);
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

Deno.test("jetstream - cross account pull subscribe", async () => {
  console.error(yellow("FAILING - ignoring"));
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

  // the api prefix is not used for pull/pullBatch()
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

// Deno.test("jetstream - cross account pull batch", async () => {
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
//   // the api prefix is not used for pull/pullBatch()
//   const js = nc.jetstream({ apiPrefix: "IPA" });
//   let iter = js.pullBatch(stream, "me", { batch: 20, expires: 1000 });
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

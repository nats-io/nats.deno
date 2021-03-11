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
  JetStreamConfig,
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
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assertEquals,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { assert } from "../nats-base-client/denobuffer.ts";
import { PubAck } from "../nats-base-client/types.ts";
import { JetStreamInfoable } from "../nats-base-client/jsclient.ts";

function callbackConsumer(debug = false): JsMsgCallback {
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

Deno.test("jetstream - publish basic", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
    "wrong last msg ID: a",
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const js = nc.jetstream();
  await js.publish(subj);

  const opts = { max: 1 } as ConsumerOpts;
  opts.callbackFn = callbackConsumer();
  const sub = await js.subscribe(subj, opts);
  await sub.closed;
  assertEquals(sub.getProcessed(), 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - ephemeral pull", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const js = nc.jetstream();
  await js.publish(subj);

  const opts = { max: 1, pullCount: 10 } as ConsumerOpts;
  opts.callbackFn = callbackConsumer();
  const sub = await js.subscribe(subj, opts);
  await sub.closed;
  assertEquals(sub.getProcessed(), 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - durable", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const js = nc.jetstream();
  await js.publish(subj);

  const opts = consumerOpts();
  opts.durable("me");
  opts.manualAck();

  const sub = await js.subscribe(subj, opts);
  const done = callbackConsumer();
  sub.unsubscribe();
  await done;
  assertEquals(sub.getProcessed(), 1);

  // consumer should exist
  const jsm = await nc.jetstreamManager();
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.name, "me");
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull no messages", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });
  const js = nc.jetstream();
  assertThrowsAsync(
    async () => {
      await js.pull(stream, "me");
    },
    Error,
    "404 No Messages",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
    "404 No Messages",
  );
  await cleanup(ns, nc);
});

Deno.test("jetstream - pullBatch requires no_wait or expires", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const js = nc.jetstream();

  await assertThrowsAsync(
    async () => {
      await js.pullBatch(stream, "me", { batch: 10 });
    },
    Error,
    "expires or no_wait is required",
  );
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch some messages", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  assertEquals(sub.processed, 0);

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
  assertEquals(sub.processed, 2);
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
  assertEquals(sub.processed, 1);
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
  assertEquals(sub.processed, 0);

  await cleanup(ns, nc);
});

Deno.test("jetstream - max ack pending", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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

Deno.test("jetstream - pullsub - attached iterator", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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

  const subin = sub as unknown as JetStreamInfoable;
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

Deno.test("jetstream - pullsub - attached callback", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
    assert(msg);
    const n = jc.decode(msg.data);
    sum += n;
    msg.ack();
  });

  const js = nc.jetstream();
  const sub = await js.pullSubscribe(subj, opts);
  sub.pull({ expires: nanos(500), batch: 5 });
  const subin = sub as unknown as JetStreamInfoable;
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

Deno.test("jetstream - pullsubscribe - not attached callback", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  await js.publish(subj);

  const opts = consumerOpts();
  opts.durable("me");
  opts.ackExplicit();
  opts.pull(5);
  opts.maxMessages(1);
  opts.callback(callbackConsumer(false));

  const sub = await js.pullSubscribe(subj, opts);
  const subin = sub as unknown as JetStreamInfoable;
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

Deno.test("jetstream - pullsub requires explicit", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  opts.callback(callbackConsumer(false));

  const sub = await js.subscribe(subj, opts);
  const subin = sub as unknown as JetStreamInfoable;
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 1 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 2 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 3 } });
  await js.publish(subj, Empty, { expect: { lastSequence: 4 } });

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.callback(callbackConsumer());

  const sub = await js.subscribe(subj, opts);
  const subin = sub as unknown as JetStreamInfoable;
  assert(subin.info);
  assertEquals(subin.info.attached, false);
  await delay(500);
  assertEquals(sub.getProcessed(), 5);
  sub.unsubscribe();

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch none - breaks after expires", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  assertEquals(batch.received, 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch none - no wait breaks fast", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  assertEquals(batch.received, 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch one - no wait breaks fast", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  assertEquals(batch.received, 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch none - cancel timers", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  assertEquals(batch.received, 0);
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull batch one - breaks after expires", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  assertEquals(batch.received, 1);
  await cleanup(ns, nc);
});

Deno.test("jetstream - pull consumer info without pull", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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
  const { ns, nc } = await setup(JetStreamConfig({}, true));
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

//
// Deno.test("jetstream - pull batch full", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream, subj } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//   await jsm.consumers.add(stream, {
//     durable_name: "me",
//     ack_policy: AckPolicy.Explicit,
//   });
//
//   const sc = StringCodec();
//   const js = nc.jetstream();
//   const data = "0123456789a";
//
//   for (const c of data) {
//     await js.publish(subj, sc.encode(c));
//   }
//   const sw = time();
//   const batch = js.pullBatch(stream, "me", {
//     batch: 5,
//     expires: 1000,
//   });
//   const done = (async () => {
//     for await (const m of batch) {
//       m.ack();
//     }
//   })();
//   await done;
//   sw.mark();
//   sw.assertLess(1000);
//   assertEquals(batch.received, 5);
//   await cleanup(ns, nc);
// });
//
// Deno.test("jetstream - sub", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream, subj } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//   await jsm.consumers.add(stream, {
//     durable_name: "me",
//     ack_policy: AckPolicy.Explicit,
//     deliver_subject: "xxxx",
//   });
//
//   const sc = StringCodec();
//   const js = nc.jetstream();
//   await js.publish(subj, sc.encode("one"));
//   await js.publish(subj, sc.encode("two"));
//   await js.publish(subj, sc.encode("three"));
//
//   let yielded = 0;
//   let cleaned = 0;
//   const opts = {} as TypedSubscriptionOptions<JsMsg>;
//   opts.max = 3;
//   opts.dispatchedFn = () => {
//     yielded++;
//   };
//   opts.cleanupFn = () => {
//     cleaned++;
//   };
//   opts.adapter = (err, msg): [NatsError | null, JsMsg | null] => {
//     try {
//       const jm = toJsMsg(msg);
//       jm.info;
//       return [err, jm];
//     } catch (err) {
//       return [null, null];
//     }
//   };
//
//   const nci = nc as NatsConnectionImpl;
//   const messages = nci.consumer<JsMsg>("xxxx", opts);
//   await (async () => {
//     for await (const m of messages) {
//       m.ack();
//     }
//   })();
//
//   assertEquals(messages.getProcessed(), 3);
//   assertEquals(messages.getPending(), 0);
//   assertEquals(yielded, 3);
//   assertEquals(cleaned, 1);
//   await cleanup(ns, nc);
// });

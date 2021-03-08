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

import { cleanup, initStream, JetStreamConfig, setup } from "./jstest_util.ts";
import {
  AckPolicy,
  ConsumerOpts,
  consumerOpts,
  Empty,
  JsMsg,
  JsMsgCallback,
  NatsError,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assertEquals,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";

function callbackConsumer(debug = false): JsMsgCallback {
  return (err: NatsError | null, jm: JsMsg | null) => {
    if (err) {
      fail(err.message);
    }
    if (debug && jm) {
      console.dir(jm.info);
      console.info(jm.headers!.toString());
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

Deno.test("jetstream - publish id", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  let pa = await js.publish(subj, Empty, { msgID: "a" });
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
  let err = await assertThrowsAsync(async () => {
    await js.publish(subj, Empty, { expect: { streamName: "xxx" } });
  });
  assertEquals(err.message, `expected stream does not match`);

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

  let err = await assertThrowsAsync(async () => {
    await js.publish(subj, Empty, { msgID: "b", expect: { lastMsgID: "b" } });
  });
  assertEquals(err.message, `wrong last msg ID: a`);

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
  let pa = await js.publish(subj, Empty);

  let err = await assertThrowsAsync(async () => {
    await js.publish(subj, Empty, { msgID: "b", expect: { lastSequence: 2 } });
  });
  assertEquals(err.message, `wrong last sequence: 1`);

  pa = await js.publish(subj, Empty, {
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
  const err = await assertThrowsAsync(async () => {
    await js.pull(stream, "me");
  });
  assertEquals(err.message, "404 No Messages");

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

  const err = await assertThrowsAsync(async () => {
    await js.pull(stream, "me");
  });
  assertEquals(err.message, "404 No Messages");

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
  await js.publish(subj, Empty, { msgID: "a" });
  await js.publish(subj, Empty, { msgID: "b" });
  await js.publish(subj, Empty, { msgID: "c" });

  let sub = await js.pullBatch(stream, "me", { batch: 2, no_wait: true });
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

  console.log('second set');
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

  console.log("will toss")
  const err = await assertThrowsAsync(async () => {
    await js.pullBatch(stream, "me", { batch: 2, no_wait: true })
  })
  assertEquals(err.message, "no messages");

  await cleanup(ns, nc);
});

// Deno.test("jetstream - ephemeral", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream, subj } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//
//   let consumers = await jsm.consumers.list(stream).next();
//   assert(consumers.length === 0);
//
//   const sub = await jsm.consumers.ephemeral(stream, {}, { manualAcks: true });
//   sub.unsubscribe(1);
//   consumers = await jsm.consumers.list(stream).next();
//   assert(consumers.length === 1);
//
//   const done = (async () => {
//     for await (const m of sub) {
//       const jm = toJsMsg(m);
//       const h = jm.headers;
//       console.log(h);
//       const info = jm.info;
//       console.log(info);
//       jm.ack();
//     }
//   })();
//
//   const js = nc.jetstream();
//   const pa = await js.publish(subj, Empty, { msgID: "a" });
//   console.log(pa);
//   assertEquals(pa.stream, stream);
//   assertEquals(pa.duplicate, false);
//   assertEquals(pa.seq, 1);
//   await done;
//   assertEquals(sub.getProcessed(), 1);
//
//   await cleanup(ns, nc);
// });
//
// Deno.test("jetstream - newEphemeralConsumer", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream, subj } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//   const js = nc.jetstream();
//   const pa = await js.publish(subj, Empty, { msgID: "a" });
//
//   let consumers = await jsm.consumers.list(stream).next();
//   assert(consumers.length === 0);
//
//   const opts = consumerOpts();
//   opts.manualAck();
//   const sub = await jsm.consumers.newEphemeralConsumer(subj, opts);
//   consumers = await jsm.consumers.list(stream).next();
//   assertEquals(consumers.length, 1);
//
//   const ok = deferred<void>();
//   const done = (async () => {
//     for await (const m of sub) {
//       if (m.seq === 1) {
//         ok.resolve();
//       }
//       m.ack();
//     }
//   })();
//
//   await ok;
//   sub.unsubscribe(1);
//   await done;
//   await cleanup(ns, nc);
// });
//
// Deno.test("jetstream - max ack pending", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream, subj } = await initStream(nc);
//
//   const jsm = await nc.jetstreamManager();
//   const sc = StringCodec();
//   const d = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
//   const buf: Promise<PubAck>[] = [];
//   const js = nc.jetstream();
//   d.forEach((v) => {
//     buf.push(js.publish(subj, sc.encode(v), { msgID: v }));
//   });
//   await Promise.all(buf);
//
//   const consumers = await jsm.consumers.list(stream).next();
//   assert(consumers.length === 0);
//
//   const sub = await js.subscribe(stream, { max_ack_pending: 10 }, {
//     manualAcks: true,
//     max: 10,
//   });
//   await (async () => {
//     for await (const m of sub) {
//       console.log(
//         `${sub.getProcessed()} - pending: ${sub.getPending()}: ${
//           sc.decode(m.data)
//         }`,
//       );
//       m.respond();
//     }
//   })();
//
//   await cleanup(ns, nc);
// });
//

// Deno.test("jetstream - fetch", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream, subj } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//
//   await jsm.consumers.add(stream, {
//     durable_name: "me",
//     ack_policy: AckPolicy.Explicit,
//   });
//
//   const noMessages = deferred();
//   const inbox = createInbox();
//   const sub = nc.subscribe(inbox);
//   const done = (async () => {
//     for await (const m of sub) {
//       if (m.headers && m.headers.code === 404) {
//         console.log("NO MESSAGES");
//         noMessages.resolve();
//       } else {
//         m.respond();
//         sub.unsubscribe();
//       }
//     }
//   })();
//
//   jsm.consumers.fetch(stream, "me", inbox, { no_wait: true });
//   await noMessages;
//
//   const js = nc.jetstream();
//   const sc = StringCodec();
//   const data = sc.encode("hello");
//   await js.publish(subj, data, { msgID: "a" });
//
//   jsm.consumers.fetch(stream, "me", inbox, { no_wait: true });
//
//   await done;
//   const ci = await jsm.consumers.info(stream, "me");
//   assertEquals(ci.num_pending, 0);
//   assertEquals(ci.num_ack_pending, 0);
//   assertEquals(ci.delivered.stream_seq, 1);
//
//   await cleanup(ns, nc);
// });
//
// Deno.test("jetstream - date format", () => {
//   const d = new Date();
//   console.log(d.toISOString());
// });
//
// Deno.test("jetstream - pull batch requires no_wait or expires", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream } = await initStream(nc);
//   const js = JetStream(nc);
//
//   const err = await assertThrowsAsync(async () => {
//     await js.pullBatch(stream, "me", { batch: 10 });
//   });
//   assertEquals(err.message, "expires or no_wait is required");
//   await cleanup(ns, nc);
// });
//
// Deno.test("jetstream - pull batch none - no_wait", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//   await jsm.consumers.add(stream, {
//     durable_name: "me",
//     ack_policy: AckPolicy.Explicit,
//   });
//
//   const js = JetStream(nc);
//
//   const batch = js.pullBatch(stream, "me", {
//     batch: 10,
//     no_wait: true,
//   });
//
//   const err = await assertThrowsAsync(async () => {
//     for await (const m of batch) {
//       console.log(m.info);
//       fail("expected no messages");
//     }
//   });
//   assertEquals(err.message, "no messages");
//   assertEquals(batch.received, 0);
//   await cleanup(ns, nc);
// });
//
// Deno.test("jetstream - pull batch none - breaks after expires", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//   await jsm.consumers.add(stream, {
//     durable_name: "me",
//     ack_policy: AckPolicy.Explicit,
//   });
//
//   const js = JetStream(nc);
//
//   const sw = time();
//   const batch = js.pullBatch(stream, "me", {
//     batch: 10,
//     expires: 1000,
//   });
//   const done = (async () => {
//     for await (const m of batch) {
//       console.log(m.info);
//       fail("expected no messages");
//     }
//   })();
//
//   await done;
//   sw.mark();
//   sw.assertInRange(1000);
//   assertEquals(batch.received, 0);
//   await cleanup(ns, nc);
// });

// Deno.test("jetstream - pull batch one - breaks after expires", async () => {
//   const { ns, nc } = await setup(JetStreamConfig({}, true));
//   const { stream, subj } = await initStream(nc);
//   const jsm = await nc.jetstreamManager();
//   await jsm.consumers.add(stream, {
//     durable_name: "me",
//     ack_policy: AckPolicy.Explicit,
//   });
//   nc.publish(subj);
//
//   const js = JetStream(nc);
//
//   const sw = time();
//   const batch = js.pullBatch(stream, "me", {
//     batch: 10,
//     expires: 1000,
//   });
//   const done = (async () => {
//     for await (const m of batch) {
//       console.log(m.info);
//     }
//   })();
//
//   await done;
//   sw.mark();
//   sw.assertInRange(1000);
//   assertEquals(batch.received, 1);
//   await cleanup(ns, nc);
// });
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
//   const js = JetStream(nc);
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

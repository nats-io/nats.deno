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
import { NatsServer } from "../../tests/helpers/launcher.ts";

import { initStream } from "./jstest_util.ts";
import {
  AckPolicy,
  Advisory,
  consumerOpts,
  DeliverPolicy,
  JsHeaders,
  JsMsg,
  JsMsgCallback,
  nanos,
  PubAck,
  RepublishHeaders,
  RetentionPolicy,
  StorageType,
} from "../mod.ts";
import {
  collect,
  delay,
  NatsConnectionImpl,
} from "../../nats-base-client/internal_mod.ts";
import {
  createInbox,
  deferred,
  Empty,
  ErrorCode,
  headers,
  JSONCodec,
  NatsError,
  nuid,
  StringCodec,
} from "../../nats-base-client/mod.ts";
import {
  assertArrayIncludes,
  assertEquals,
  assertExists,
  assertRejects,
  assertThrows,
  fail,
} from "https://deno.land/std@0.200.0/assert/mod.ts";

import { assert } from "../../nats-base-client/denobuffer.ts";
import {
  JetStreamClientImpl,
  JetStreamSubscriptionImpl,
  PubHeaders,
} from "../jsclient.ts";
import { defaultJsOptions } from "../jsbaseclient_api.ts";
import { connect } from "../../src/connect.ts";
import {
  cleanup,
  jetstreamServerConf,
  Lock,
  notCompatible,
  setup,
} from "../../tests/helpers/mod.ts";
import { ConsumerOptsBuilderImpl } from "../types.ts";

export function callbackConsume(debug = false): JsMsgCallback {
  return (err: NatsError | null, jm: JsMsg | null) => {
    if (err) {
      switch (err.code) {
        case ErrorCode.JetStream408RequestTimeout:
        case ErrorCode.JetStream409:
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
  await assertRejects(
    async () => {
      await js.findStream("hello");
    },
    Error,
    "no stream matches subject",
    undefined,
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
  assertEquals(sm.header.get(PubHeaders.MsgIdHdr), "a");

  await cleanup(ns, nc);
});

Deno.test("jetstream - publish require stream", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  await assertRejects(
    async () => {
      await js.publish(subj, Empty, { expect: { streamName: "xxx" } });
    },
    Error,
    "expected stream does not match",
    undefined,
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

  await assertRejects(
    async () => {
      await js.publish(subj, Empty, { msgID: "b", expect: { lastMsgID: "b" } });
    },
    Error,
    "wrong last msg id: a",
    undefined,
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

Deno.test("jetstream - publish first sequence", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const { subj } = await initStream(nc);

  const js = nc.jetstream();
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
  await assertRejects(
    async () => {
      await js.publish(subj, Empty, { expect: { lastSequence: 0 } });
    },
    Error,
    "wrong last sequence",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - publish require last sequence", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const js = nc.jetstream();
  await js.publish(subj, Empty, { expect: { lastSequence: 0 } });

  await assertRejects(
    async () => {
      await js.publish(subj, Empty, {
        msgID: "b",
        expect: { lastSequence: 2 },
      });
    },
    Error,
    "wrong last sequence: 1",
    undefined,
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

Deno.test("jetstream - ephemeral options", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  const v = await jsm.consumers.add(stream, {
    inactive_threshold: nanos(1000),
    ack_policy: AckPolicy.Explicit,
  });
  assertEquals(v.config.inactive_threshold, nanos(1000));
  await cleanup(ns, nc);
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

Deno.test("jetstream - cleanup", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);
  const js = nc.jetstream();

  for (let i = 0; i < 100; i++) {
    await js.publish(subj, Empty);
  }

  const opts = consumerOpts();
  opts.deliverTo(createInbox());
  const sub = await js.subscribe(subj, opts);
  let counter = 0;
  const done = (async () => {
    for await (const m of sub) {
      counter++;
      m.ack();
      break;
    }
  })();

  await done;
  assertEquals(counter, 1);
  assertEquals(sub.isClosed(), true);
  const nci = nc as NatsConnectionImpl;
  const min = nci.protocol.subscriptions.getMux() ? 1 : 0;
  assertEquals(nci.protocol.subscriptions.subs.size, min);

  await cleanup(ns, nc);
});

Deno.test("jetstream - source", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));

  const stream = nuid.next();
  const subj = `${stream}.*`;
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add(
    { name: stream, subjects: [subj] },
  );

  const js = nc.jetstream();

  for (let i = 0; i < 10; i++) {
    await js.publish(`${stream}.A`);
    await js.publish(`${stream}.B`);
  }

  await jsm.streams.add({
    name: "work",
    storage: StorageType.File,
    retention: RetentionPolicy.Workqueue,
    sources: [
      { name: stream, filter_subject: ">" },
    ],
  });

  // source will not process right away?
  await delay(1000);

  const ci = await jsm.consumers.add("work", {
    ack_policy: AckPolicy.Explicit,
    durable_name: "worker",
    filter_subject: `${stream}.B`,
    deliver_subject: createInbox(),
  });

  const sub = await js.subscribe(`${stream}.B`, { config: ci.config });
  for await (const m of sub) {
    m.ack();
    if (m.info.pending === 0) {
      break;
    }
  }

  // give the server a chance to process the ack's and cleanup
  await nc.flush();

  const si = await jsm.streams.info("work");
  // stream still has all the 'A' messages
  assertEquals(si.state.messages, 10);

  await cleanup(ns, nc);
});

async function ocTest(
  N: number,
  S: number,
  callback: boolean,
): Promise<void> {
  if (N % 10 !== 0) {
    throw new Error("N must be divisible by 10");
  }

  const storage = N * S + (1024 * 1024);
  const { ns, nc } = await setup(jetstreamServerConf({
    jetstream: {
      max_file_store: storage,
    },
  }, true));
  const { subj } = await initStream(nc);
  const js = nc.jetstream();

  const buf = new Uint8Array(S);
  for (let i = 0; i < S; i++) {
    buf[i] = "a".charCodeAt(0) + (i % 26);
  }

  // speed up the loading by sending 10 at time
  const n = N / 10;
  for (let i = 0; i < n; i++) {
    await Promise.all([
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
      js.publish(subj, buf),
    ]);
  }

  const lock = Lock(N, 1000 * 60);
  const opts = consumerOpts({ idle_heartbeat: nanos(1000) });
  opts.orderedConsumer();
  if (callback) {
    opts.callback((err: NatsError | null, msg: JsMsg | null): void => {
      if (err) {
        fail(err.message);
        return;
      }
      if (!msg) {
        fail(`didn't expect to get null msg`);
        return;
      }
      lock.unlock();
    });
  }

  const sub = await js.subscribe(subj, opts);
  if (!callback) {
    (async () => {
      for await (const _jm of sub) {
        lock.unlock();
      }
    })().then();
  }
  await lock;
  //@ts-ignore: test
  assertEquals(sub.sub.info.ordered_consumer_sequence.stream_seq, N);
  //@ts-ignore: test
  assertEquals(sub.sub.info.ordered_consumer_sequence.delivery_seq, N);

  await delay(3 * 1000);
  // @ts-ignore: test
  const hbc = sub.sub.info.flow_control.heartbeat_count;
  assert(hbc >= 2);
  // @ts-ignore: test
  const fcc = sub.sub.info.flow_control.fc_count;
  assert(fcc >= 0);

  // @ts-ignore: test
  assert(sub.sub.info.flow_control.consumer_restarts >= 0);

  // @ts-ignore: test
  assert(sub.sub.info.flow_control.heartbeat_count > 0);

  const ci = await sub.consumerInfo();

  assertEquals(ci.config.deliver_policy, DeliverPolicy.All);
  assertEquals(ci.config.ack_policy, AckPolicy.None);
  assertEquals(ci.config.max_deliver, 1);
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.delivered.consumer_seq, N);
  assertEquals(ci.delivered.stream_seq, N);

  await cleanup(ns, nc);
}

Deno.test("jetstream - ordered consumer callback", async () => {
  await ocTest(500, 1024, true);
});

Deno.test("jetstream - ordered consumer iterator", async () => {
  await ocTest(500, 1024, false);
});

Deno.test("jetstream - seal", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.2")) {
    return;
  }
  const { stream, subj } = await initStream(nc);
  const js = nc.jetstream();
  const sc = StringCodec();
  await js.publish(subj, sc.encode("hello"));
  await js.publish(subj, sc.encode("second"));

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info(stream);
  assertEquals(si.config.sealed, false);
  assertEquals(si.config.deny_purge, false);
  assertEquals(si.config.deny_delete, false);

  await jsm.streams.deleteMessage(stream, 1);

  si.config.sealed = true;
  const usi = await jsm.streams.update(stream, si.config);
  assertEquals(usi.config.sealed, true);

  await assertRejects(
    async () => {
      await jsm.streams.deleteMessage(stream, 2);
    },
    Error,
    "invalid operation on sealed stream",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - deny delete", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.2")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
    deny_delete: true,
  });

  const js = nc.jetstream();
  const sc = StringCodec();
  await js.publish(subj, sc.encode("hello"));
  await js.publish(subj, sc.encode("second"));

  const si = await jsm.streams.info(stream);
  assertEquals(si.config.deny_delete, true);

  await assertRejects(
    async () => {
      await jsm.streams.deleteMessage(stream, 1);
    },
    Error,
    "message delete not permitted",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - deny purge", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.2")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
    deny_purge: true,
  });

  const js = nc.jetstream();
  const sc = StringCodec();
  await js.publish(subj, sc.encode("hello"));
  await js.publish(subj, sc.encode("second"));

  const si = await jsm.streams.info(stream);
  assertEquals(si.config.deny_purge, true);

  await assertRejects(
    async () => {
      await jsm.streams.purge(stream);
    },
    Error,
    "stream purge not permitted",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - rollup all", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
    allow_rollup_hdrs: true,
  });

  const js = nc.jetstream();
  const jc = JSONCodec();
  const buf = [];
  for (let i = 1; i < 11; i++) {
    buf.push(js.publish(`${stream}.A`, jc.encode({ value: i })));
  }
  await Promise.all(buf);

  const h = headers();
  h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueAll);
  await js.publish(`${stream}.summary`, jc.encode({ value: 42 }), {
    headers: h,
  });

  const si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 1);

  const opts = consumerOpts();
  opts.manualAck();
  opts.deliverTo(createInbox());
  opts.callback((_err, jm) => {
    assert(jm);
    assertEquals(jm.subject, `${stream}.summary`);
    const obj = jc.decode(jm.data) as Record<string, number>;
    assertEquals(obj.value, 42);
  });
  opts.maxMessages(1);

  const sub = await js.subscribe(subj, opts);
  await sub.closed;
  assertEquals(sub.getProcessed(), 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - rollup subject", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const stream = "S";
  const subj = `${stream}.*`;
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
    allow_rollup_hdrs: true,
  });

  const js = nc.jetstream();
  const jc = JSONCodec<Record<string, number>>();
  const buf = [];
  for (let i = 1; i < 11; i++) {
    buf.push(js.publish(`${stream}.A`, jc.encode({ value: i })));
    buf.push(js.publish(`${stream}.B`, jc.encode({ value: i })));
  }
  await Promise.all(buf);

  let si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 20);

  let cia = await jsm.consumers.add(stream, {
    durable_name: "dura",
    filter_subject: `${stream}.A`,
    ack_policy: AckPolicy.Explicit,
  });
  assertEquals(cia.num_pending, 10);

  const h = headers();
  h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueSubject);
  await js.publish(`${stream}.A`, jc.encode({ value: 0 }), {
    headers: h,
  });

  await delay(5000);

  cia = await jsm.consumers.info(stream, "dura");
  assertEquals(cia.num_pending, 1);

  si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 11);

  const cib = await jsm.consumers.add(stream, {
    durable_name: "durb",
    filter_subject: `${stream}.B`,
    ack_policy: AckPolicy.Explicit,
  });
  assertEquals(cib.num_pending, 10);
  await cleanup(ns, nc);
});

Deno.test("jetstream - no rollup", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.3")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const stream = "S";
  const subj = `${stream}.*`;
  const si = await jsm.streams.add({
    name: stream,
    subjects: [subj],
    allow_rollup_hdrs: false,
  });
  assertEquals(si.config.allow_rollup_hdrs, false);

  const js = nc.jetstream();
  const jc = JSONCodec<Record<string, number>>();
  const buf = [];
  for (let i = 1; i < 11; i++) {
    buf.push(js.publish(`${stream}.A`, jc.encode({ value: i })));
  }
  await Promise.all(buf);

  const h = headers();
  h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueSubject);
  await assertRejects(
    async () => {
      await js.publish(`${stream}.A`, jc.encode({ value: 42 }), {
        headers: h,
      });
    },
    Error,
    "rollup not permitted",
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - headers only", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.2")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
  });

  const js = nc.jetstream();
  const sc = StringCodec();
  await js.publish(`${stream}.A`, sc.encode("a"));
  await js.publish(`${stream}.B`, sc.encode("b"));

  const opts = consumerOpts();
  opts.deliverTo(createInbox());
  opts.headersOnly();
  opts.manualAck();
  opts.callback((_err, jm) => {
    assert(jm);
    assert(jm.headers);
    const size = parseInt(jm.headers.get(JsHeaders.MessageSizeHdr), 10);
    assertEquals(size, 1);
    assertEquals(jm.data, Empty);
    jm.ack();
  });
  opts.maxMessages(2);

  const sub = await js.subscribe(subj, opts);
  await sub.closed;
  assertEquals(sub.getProcessed(), 2);

  await cleanup(ns, nc);
});

Deno.test("jetstream - can access kv", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.6.2")) {
    return;
  }
  const sc = StringCodec();

  const js = nc.jetstream();
  // create the named KV or bind to it if it exists:
  const kv = await js.views.kv("testing", { history: 5 });

  // create an entry - this is similar to a put, but will fail if the
  // key exists
  await kv.create("hello.world", sc.encode("hi"));

  // Values in KV are stored as KvEntries:
  // {
  //   bucket: string,
  //   key: string,
  //   value: Uint8Array,
  //   created: Date,
  //   revision: number,
  //   delta?: number,
  //   operation: "PUT"|"DEL"|"PURGE"
  // }
  // The operation property specifies whether the value was
  // updated (PUT), deleted (DEL) or purged (PURGE).

  // you can monitor values modification in a KV by watching.
  // You can watch specific key subset or everything.
  // Watches start with the latest value for each key in the
  // set of keys being watched - in this case all keys
  const watch = await kv.watch();
  (async () => {
    for await (const _e of watch) {
      // do something with the change
    }
  })().then();

  // update the entry
  await kv.put("hello.world", sc.encode("world"));
  // retrieve the KvEntry storing the value
  // returns null if the value is not found
  const e = await kv.get("hello.world");
  assert(e);
  // initial value of "hi" was overwritten above
  assertEquals(sc.decode(e.value), "world");

  const keys = await collect<string>(await kv.keys());
  assertEquals(keys.length, 1);
  assertEquals(keys[0], "hello.world");

  const h = await kv.history({ key: "hello.world" });
  (async () => {
    for await (const _e of h) {
      // do something with the historical value
      // you can test e.operation for "PUT", "DEL", or "PURGE"
      // to know if the entry is a marker for a value set
      // or for a deletion or purge.
    }
  })().then();

  // deletes the key - the delete is recorded
  await kv.delete("hello.world");

  // purge is like delete, but all history values
  // are dropped and only the purge remains.
  await kv.purge("hello.world");

  // stop the watch operation above
  watch.stop();

  // danger: destroys all values in the KV!
  await kv.destroy();

  await cleanup(ns, nc);
});

Deno.test("jetstream - bind with diff subject fails", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const jsm = await nc.jetstreamManager();
  const stream = nuid.next();
  const subj = `${stream}.*`;
  await jsm.streams.add({
    name: stream,
    subjects: [subj],
  });

  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.None,
    deliver_subject: createInbox(),
    filter_subject: `${stream}.foo`,
  });

  const opts = consumerOpts();
  opts.bind(stream, "me");
  opts.filterSubject(`${stream}.bar`);
  await assertRejects(
    async () => {
      const js = nc.jetstream();
      await js.subscribe(subj, opts);
    },
    Error,
    "subject does not match consumer",
    undefined,
  );
  await cleanup(ns, nc);
});

Deno.test("jetstream - test events stream", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({
    name: "events",
    subjects: ["events.>"],
  });

  await js.subscribe("events.>", {
    stream: "events",
    config: {
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      deliver_subject: "foo",
      durable_name: "me",
      filter_subject: "events.>",
    },
    callbackFn: (_err: NatsError | null, msg: JsMsg | null) => {
      msg?.ack();
    },
  });

  await js.publish("events.a");
  await js.publish("events.b");
  await delay(2000);
  await cleanup(ns, nc);
});

Deno.test("jetstream - bind without consumer should fail", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({
    name: "events",
    subjects: ["events.>"],
  });

  const opts = consumerOpts();
  opts.manualAck();
  opts.ackExplicit();
  opts.bind("events", "hello");

  await assertRejects(
    async () => {
      await js.subscribe("events.>", opts);
    },
    Error,
    "unable to bind - durable consumer hello doesn't exist in events",
  );

  await cleanup(ns, nc);
});

Deno.test("jetstream - mirror alternates", async () => {
  const servers = await NatsServer.jetstreamCluster(3);
  const nc = await connect({ port: servers[0].port });
  if (await notCompatible(servers[0], nc, "2.8.2")) {
    await NatsServer.stopAll([servers[1], servers[2]]);
    return;
  }

  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({ name: "src", subjects: ["A", "B"] });

  const nc1 = await connect({ port: servers[1].port });
  const jsm1 = await nc1.jetstreamManager();

  await jsm1.streams.add({
    name: "mirror",
    mirror: {
      name: "src",
    },
  });

  const n = await jsm1.streams.find("A");
  const si = await jsm1.streams.info(n);
  assertEquals(si.alternates?.length, 2);

  await nc.close();
  await nc1.close();
  await NatsServer.stopAll(servers);
});

Deno.test("jetstream - backoff", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.7.2")) {
    return;
  }

  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();

  const backoff = [nanos(250), nanos(1000), nanos(3000)];
  const ci = await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
    max_deliver: 4,
    deliver_subject: "here",
    backoff,
  });

  assert(ci.config.backoff);
  assertEquals(ci.config.backoff[0], backoff[0]);
  assertEquals(ci.config.backoff[1], backoff[1]);
  assertEquals(ci.config.backoff[2], backoff[2]);

  const js = nc.jetstream();
  await js.publish(subj);

  const opts = consumerOpts();
  opts.bind(stream, "me");
  opts.manualAck();

  const arrive: number[] = [];
  let start = 0;
  const sub = await js.subscribe(subj, opts);
  await (async () => {
    for await (const m of sub) {
      if (start === 0) {
        start = Date.now();
      }
      arrive.push(Date.now());
      if (m.info.redeliveryCount === 4) {
        break;
      }
    }
  })();

  const delta = arrive.map((v) => {
    return v - start;
  });

  assert(delta[1] > 250 && delta[1] < 1000);
  assert(delta[2] > 1250 && delta[2] < 1500);
  assert(delta[3] > 4250 && delta[2] < 4500);

  await cleanup(ns, nc);
});

Deno.test("jetstream - detailed errors", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const jsm = await nc.jetstreamManager();

  const ne = await assertRejects(() => {
    return jsm.streams.add({
      name: "test",
      num_replicas: 3,
      subjects: ["foo"],
    });
  }) as NatsError;

  assert(ne.api_error);
  assertEquals(
    ne.message,
    "replicas > 1 not supported in non-clustered mode",
  );
  assertEquals(
    ne.api_error.description,
    "replicas > 1 not supported in non-clustered mode",
  );
  assertEquals(ne.api_error.code, 500);
  assertEquals(ne.api_error.err_code, 10074);

  await cleanup(ns, nc);
});

Deno.test("jetstream - repub on 503", async () => {
  let servers = await NatsServer.jetstreamCluster(4, {});
  servers[0].config.jetstream = "disabled";
  await NatsServer.stopAll(servers);
  const proms = servers.map((s) => {
    return s.restart();
  });

  const connection = await proms[0];
  const data = await NatsServer.dataClusterFormed([
    proms[1],
    proms[2],
    proms[3],
  ]);
  servers = [connection, data[0], data[1], data[2]];

  const nc = await connect({ port: connection.port });

  const { stream, subj } = await initStream(nc, nuid.next(), {
    num_replicas: 3,
  });

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info(stream);
  const host = si.cluster!.leader || "";
  const leader = servers.find((s) => {
    return s.config.server_name === host;
  });

  // publish a message
  const js = nc.jetstream();
  const pa = await js.publish(subj);
  assertEquals(pa.stream, stream);

  // now stop and wait a bit for the servers
  await leader?.stop();
  await delay(1000);

  await js.publish(subj, Empty, {
    //@ts-ignore: testing
    retries: 15,
    retry_delay: 1000,
    timeout: 15000,
  });

  await nc.close();
  await NatsServer.stopAll(servers);
});

Deno.test("jetstream - duplicate message pub", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { subj } = await initStream(nc);
  const js = nc.jetstream();

  let ack = await js.publish(subj, Empty, { msgID: "x" });
  assertEquals(ack.duplicate, false);

  ack = await js.publish(subj, Empty, { msgID: "x" });
  assertEquals(ack.duplicate, true);

  await cleanup(ns, nc);
});

Deno.test("jetstream - republish", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.9.0")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.add({
    name: nuid.next(),
    subjects: ["foo"],
    republish: {
      src: "foo",
      dest: "bar",
    },
  });

  assertEquals(si.config.republish?.src, "foo");
  assertEquals(si.config.republish?.dest, "bar");

  const sub = nc.subscribe("bar", { max: 1 });
  const done = (async () => {
    for await (const m of sub) {
      assertEquals(m.subject, "bar");
      assert(m.headers?.get(RepublishHeaders.Subject), "foo");
      assert(m.headers?.get(RepublishHeaders.Sequence), "1");
      assert(m.headers?.get(RepublishHeaders.Stream), si.config.name);
      assert(m.headers?.get(RepublishHeaders.LastSequence), "0");
    }
  })();

  nc.publish("foo");
  await done;

  await cleanup(ns, nc);
});

Deno.test("jetstream - mem_storage consumer option", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.9.0")) {
    return;
  }

  const { stream, subj } = await initStream(nc);
  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info(stream);
  assertEquals(si.config.storage, StorageType.File);

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.durable("opts");
  opts.memory();

  const js = nc.jetstream();
  const sub = await js.pullSubscribe(subj, opts);
  let ci = await sub.consumerInfo();
  assertEquals(ci.config.mem_storage, true);

  ci.config.mem_storage = false;
  ci = await jsm.consumers.update(stream, "opts", ci.config);
  assertEquals(ci.config.mem_storage, undefined);

  ci = await jsm.consumers.add(stream, {
    durable_name: "dopts",
    mem_storage: true,
    ack_policy: AckPolicy.Explicit,
  });
  assertEquals(ci.config.mem_storage, true);

  await cleanup(ns, nc);
});

Deno.test("jetstream - num_replicas consumer option", async () => {
  const servers = await NatsServer.jetstreamCluster(3);
  const nc = await connect({ port: servers[0].port });
  if (await notCompatible(servers[0], nc, "2.9.0")) {
    await NatsServer.stopAll(servers);
    return;
  }

  const { stream, subj } = await initStream(nc, nuid.next(), {
    num_replicas: 3,
  });
  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.info(stream);
  assertEquals(si.config.num_replicas, 3);

  const opts = consumerOpts();
  opts.ackExplicit();
  opts.durable("opts");

  const js = nc.jetstream();
  const sub = await js.pullSubscribe(subj, opts);
  let ci = await sub.consumerInfo();
  assertEquals(ci.config.num_replicas, 0);

  ci.config.num_replicas = 2;
  ci = await jsm.consumers.update(stream, "opts", ci.config);
  assertEquals(ci.config.num_replicas, 2);

  await nc.close();
  await NatsServer.stopAll(servers);
  // in ci this hangs
  await delay(500);
});

Deno.test("jetstream - filter_subject consumer update", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.9.0")) {
    return;
  }

  const jsm = await nc.jetstreamManager();
  const si = await jsm.streams.add({ name: nuid.next(), subjects: ["foo.>"] });
  let ci = await jsm.consumers.add(si.config.name, {
    ack_policy: AckPolicy.Explicit,
    filter_subject: "foo.bar",
    durable_name: "a",
  });
  assertEquals(ci.config.filter_subject, "foo.bar");

  ci.config.filter_subject = "foo.baz";
  ci = await jsm.consumers.update(si.config.name, "a", ci.config);
  assertEquals(ci.config.filter_subject, "foo.baz");
  await cleanup(ns, nc);
});

Deno.test("jetstream - kv and object store views reject in older servers", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({
    max_payload: 1024 * 1024,
  }, true));

  const nci = nc as NatsConnectionImpl;
  const js = nc.jetstream();
  async function t(version: string, kv: boolean, os: boolean): Promise<void> {
    nci.features.update(version);

    if (!kv) {
      await assertRejects(
        async () => {
          await js.views.kv(nuid.next());
        },
        Error,
        `kv is only supported on servers 2.6.2 or better`,
      );
    } else {
      await js.views.kv(nuid.next());
    }

    if (!os) {
      await assertRejects(
        async () => {
          await js.views.os(nuid.next());
        },
        Error,
        `objectstore is only supported on servers 2.6.3 or better`,
      );
    } else {
      await js.views.os(nuid.next());
    }
  }

  await t("2.6.1", false, false);
  await t("2.6.2", true, false);
  await t("2.6.3", true, true);
  await t("2.6.4", true, true);

  await cleanup(ns, nc);
});

Deno.test("jetstream - ordered consumer reset", async () => {
  let { ns, nc } = await setup(jetstreamServerConf({}));
  const { subj } = await initStream(nc, "A");
  const d = deferred<JsMsg>();
  const js = nc.jetstream();
  const opts = consumerOpts();
  opts.orderedConsumer();
  opts.callback((err, m) => {
    if (err) {
      fail(err.message);
    }
    c.unsubscribe();
    d.resolve(m!);
  });
  const c = await js.subscribe(subj, opts);

  // stop the server and wait until hbs are missed
  await ns.stop();
  while (true) {
    const missed = (c as JetStreamSubscriptionImpl).monitor?.missed || 0;
    const connected = (nc as NatsConnectionImpl).protocol.connected;
    // we want to wait until after 2 because we want to have a cycle
    // where we try to recreate the consumer, but skip it because we are
    // not connected
    if (!connected && missed >= 3) {
      break;
    }
    await delay(300);
  }
  ns = await ns.restart();
  let ack: PubAck;
  while (true) {
    try {
      ack = await js.publish(subj);
      break;
    } catch (err) {
      if (err.code !== ErrorCode.Timeout) {
        fail(err.message);
      }
      await delay(1000);
    }
  }
  await c.closed;

  assertEquals((await d).seq, ack.seq);
  await cleanup(ns, nc);
});

Deno.test("jetstream - consumer opt multi subject filter", () => {
  const opts = new ConsumerOptsBuilderImpl();
  opts.filterSubject("foo");
  let co = opts.getOpts();
  assertEquals(co.config.filter_subject, "foo");

  opts.filterSubject("bar");
  co = opts.getOpts();
  assertEquals(co.config.filter_subject, undefined);
  assertExists(co.config.filter_subjects);
  assertArrayIncludes(co.config.filter_subjects, ["foo", "bar"]);
});

Deno.test("jetstream - jsmsg decode", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  const name = nuid.next();
  const jsm = await nc.jetstreamManager();
  const js = nc.jetstream();
  await jsm.streams.add({ name, subjects: [`a.>`] });

  await jsm.consumers.add(name, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  await js.publish("a.a", StringCodec().encode("hello"));
  await js.publish("a.a", JSONCodec().encode({ one: "two", a: [1, 2, 3] }));

  assertEquals((await js.pull(name, "me")).string(), "hello");
  assertEquals((await js.pull(name, "me")).json(), {
    one: "two",
    a: [1, 2, 3],
  });

  await cleanup(ns, nc);
});

Deno.test("jetstream - input transform", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }
  const name = nuid.next();
  const jsm = await nc.jetstreamManager();

  const si = await jsm.streams.add({
    name,
    subjects: ["foo"],
    subject_transform: {
      src: ">",
      dest: "transformed.>",
    },
    storage: StorageType.Memory,
  });

  assertEquals(si.config.subject_transform, {
    src: ">",
    dest: "transformed.>",
  });

  const js = nc.jetstream();
  const pa = await js.publish("foo", Empty);
  assertEquals(pa.seq, 1);

  const m = await jsm.streams.getMessage(si.config.name, { seq: 1 });
  assertEquals(m.subject, "transformed.foo");

  await cleanup(ns, nc);
});

Deno.test("jetstream - source transforms", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  if (await notCompatible(ns, nc, "2.10.0")) {
    return;
  }
  const jsm = await nc.jetstreamManager();

  const proms = ["foo", "bar", "baz"].map((subj) => {
    return jsm.streams.add({
      name: subj,
      subjects: [subj],
      storage: StorageType.Memory,
    });
  });
  await Promise.all(proms);

  const js = nc.jetstream();
  await Promise.all([
    js.publish("foo", Empty),
    js.publish("bar", Empty),
    js.publish("baz", Empty),
  ]);

  await jsm.streams.add({
    name: "sourced",
    storage: StorageType.Memory,
    sources: [
      { name: "foo", subject_transforms: [{ src: ">", dest: "foo2.>" }] },
      { name: "bar" },
      { name: "baz" },
    ],
  });

  while (true) {
    const si = await jsm.streams.info("sourced");
    if (si.state.messages === 3) {
      break;
    }
    await delay(100);
  }

  const map = new Map<string, string>();
  const oc = await js.consumers.get("sourced");
  const iter = await oc.fetch({ max_messages: 3 });
  for await (const m of iter) {
    map.set(m.subject, m.subject);
  }

  assert(map.has("foo2.foo"));
  assert(map.has("bar"));
  assert(map.has("baz"));

  await cleanup(ns, nc);
});

Deno.test("jetstream - term reason", async () => {
  const { ns, nc } = await setup(jetstreamServerConf());
  if (await notCompatible(ns, nc, "2.11.0")) {
    return;
  }
  const jsm = await nc.jetstreamManager();
  await jsm.streams.add({
    name: "foos",
    subjects: ["foo.*"],
  });

  const js = nc.jetstream();

  await Promise.all(
    [
      js.publish("foo.1"),
      js.publish("foo.2"),
      js.publish("foo.term"),
    ],
  );

  await jsm.consumers.add("foos", {
    name: "bar",
    ack_policy: AckPolicy.Explicit,
  });

  const termed = deferred<Advisory>();
  const advisories = jsm.advisories();
  (async () => {
    for await (const a of advisories) {
      if (a.kind === "terminated") {
        termed.resolve(a);
        break;
      }
    }
  })().catch((err) => {
    console.log(err);
  });

  const c = await js.consumers.get("foos", "bar");
  const iter = await c.consume();
  await (async () => {
    for await (const m of iter) {
      if (m.subject.endsWith(".term")) {
        m.term("requested termination");
        break;
      } else {
        m.ack();
      }
    }
  })().catch();

  const s = await termed;
  const d = s.data as Record<string, unknown>;
  assertEquals(d.type, "io.nats.jetstream.advisory.v1.terminated");
  assertEquals(d.reason, "requested termination");

  await cleanup(ns, nc);
});

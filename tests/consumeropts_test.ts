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
  assertEquals,
  assertThrows,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";

import {
  consumerOpts,
  ConsumerOptsBuilderImpl,
  isConsumerOptsBuilder,
} from "../nats-base-client/consumeropts.ts";
import {
  AckPolicy,
  ConsumerOpts,
  DeliverPolicy,
  JsMsgCallback,
} from "../nats-base-client/types.ts";
import { nanos } from "../nats-base-client/jsutil.ts";

Deno.test("consumeropts - isConsumerOptsBuilder", () => {
  assertEquals(isConsumerOptsBuilder(consumerOpts()), true);
  assertEquals(isConsumerOptsBuilder({} as ConsumerOpts), false);
});

Deno.test("consumeropts - pull", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  assertThrows(
    () => {
      opts.pull(0);
    },
    Error,
    "batch must be greater than 0",
  );

  opts.pull(5);
  assertEquals(opts.pullCount, 5);

  const args = opts.getOpts();
  assertEquals(args.pullCount, 5);
});

Deno.test("consumeropts - pulldirect", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  assertThrows(
    () => {
      opts.pullDirect("bad.name", "consumer", 5);
    },
    Error,
    "invalid stream name",
  );

  assertThrows(
    () => {
      opts.pullDirect("stream", "bad.name", 5);
    },
    Error,
    "invalid durable name",
  );

  opts.pullDirect("stream", "consumer", 5);
  assertEquals(opts.stream, "stream");
  assertEquals(opts.config.durable_name, "consumer");
  assertEquals(opts.pullCount, 5);

  const args = opts.getOpts();
  assertEquals(args.stream, "stream");
  assertEquals(args.config.durable_name, "consumer");
  assertEquals(args.pullCount, 5);
});

Deno.test("consumeropts - deliverTo", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  opts.deliverTo("a.b");
  assertEquals(opts.config.deliver_subject, "a.b");

  const args = opts.getOpts();
  assertEquals(args.config.deliver_subject, "a.b");
});

Deno.test("consumeropts - queue", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  opts.queue("queue");
  assertEquals(opts.subQueue, "queue");

  const args = opts.getOpts();
  assertEquals(args.subQueue, "queue");
});

Deno.test("consumeropts - manualAck", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  opts.manualAck();
  assertEquals(opts.mack, true);

  const args = opts.getOpts();
  assertEquals(args.mack, true);
});

Deno.test("consumeropts - durable", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  assertThrows(
    () => {
      opts.durable("bad.durable");
    },
    Error,
    "invalid durable name",
  );

  opts.durable("durable");
  assertEquals(opts.config.durable_name, "durable");

  const args = opts.getOpts();
  assertEquals(args.config.durable_name, "durable");
});

Deno.test("consumeropts - deliverAll", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  opts.deliverAll();
  assertEquals(opts.config.deliver_policy, DeliverPolicy.All);

  const args = opts.getOpts();
  assertEquals(args.config.deliver_policy, DeliverPolicy.All);
});

Deno.test("consumeropts - deliverLast", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  opts.deliverLast();
  assertEquals(opts.config.deliver_policy, DeliverPolicy.Last);

  const args = opts.getOpts();
  assertEquals(args.config.deliver_policy, DeliverPolicy.Last);
});

Deno.test("consumeropts - deliverNew", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  opts.deliverNew();
  assertEquals(opts.config.deliver_policy, DeliverPolicy.New);

  const args = opts.getOpts();
  assertEquals(args.config.deliver_policy, DeliverPolicy.New);
});

Deno.test("consumeropts - startSequence", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  assertThrows(
    () => {
      opts.startSequence(0);
    },
    Error,
    "sequence must be greater than 0",
  );
  opts.startSequence(100);
  assertEquals(opts.config.opt_start_seq, 100);
  assertEquals(opts.config.deliver_policy, DeliverPolicy.StartSequence);

  const args = opts.getOpts();
  assertEquals(args.config.opt_start_seq, 100);
  assertEquals(args.config.deliver_policy, DeliverPolicy.StartSequence);
});

Deno.test("consumeropts - startTime Nanos", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  const ns = nanos(100);
  opts.startTime(ns);
  assertEquals(opts.config.opt_start_time, ns);
  assertEquals(opts.config.deliver_policy, DeliverPolicy.StartTime);

  const args = opts.getOpts();
  assertEquals(args.config.opt_start_time, ns);
  assertEquals(args.config.deliver_policy, DeliverPolicy.StartTime);
});

Deno.test("consumeropts - startTime Date", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  const epoch = new Date(0);
  const ns = nanos(epoch.getTime());
  opts.startTime(epoch);
  assertEquals(opts.config.opt_start_time, ns);
  assertEquals(opts.config.deliver_policy, DeliverPolicy.StartTime);

  const args = opts.getOpts();
  assertEquals(args.config.opt_start_time, ns);
  assertEquals(args.config.deliver_policy, DeliverPolicy.StartTime);
});

Deno.test("consumeropts - ackNone", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  opts.ackNone();
  assertEquals(opts.config.ack_policy, AckPolicy.None);

  const args = opts.getOpts();
  assertEquals(args.config.ack_policy, AckPolicy.None);
});

Deno.test("consumeropts - ackAll", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  opts.ackAll();
  assertEquals(opts.config.ack_policy, AckPolicy.All);

  const args = opts.getOpts();
  assertEquals(args.config.ack_policy, AckPolicy.All);
});

Deno.test("consumeropts - ackExplicit", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  opts.ackExplicit();
  assertEquals(opts.config.ack_policy, AckPolicy.Explicit);

  const args = opts.getOpts();
  assertEquals(args.config.ack_policy, AckPolicy.Explicit);
});

Deno.test("consumeropts - maxDeliver", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  opts.maxDeliver(100);
  assertEquals(opts.config.max_deliver, 100);

  const args = opts.getOpts();
  assertEquals(args.config.max_deliver, 100);
});

Deno.test("consumeropts - maxAcPending", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  opts.maxAckPending(100);
  assertEquals(opts.config.max_ack_pending, 100);

  const args = opts.getOpts();
  assertEquals(args.config.max_ack_pending, 100);
});

Deno.test("consumeropts - maxWaiting", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  opts.maxWaiting(100);
  assertEquals(opts.config.max_waiting, 100);

  const args = opts.getOpts();
  assertEquals(args.config.max_waiting, 100);
});

Deno.test("consumeropts - maxMessages", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;
  opts.maxMessages(100);
  assertEquals(opts.max, 100);

  const args = opts.getOpts();
  assertEquals(args.max, 100);
});

Deno.test("consumeropts - callback", () => {
  const opts = consumerOpts() as ConsumerOptsBuilderImpl;

  const cb: JsMsgCallback = (err, msg) => {};
  opts.callback(cb);
  assertEquals(opts.callbackFn, cb);

  const args = opts.getOpts();
  assertEquals(args.callbackFn, cb);
});

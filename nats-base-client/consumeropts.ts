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
  AckPolicy,
  ConsumerConfig,
  ConsumerOpts,
  DeliverPolicy,
  JsMsgCallback,
  Nanos,
} from "./types.ts";
import {
  defaultConsumer,
  nanos,
  validateDurableName,
  validateStreamName,
} from "./jsutil.ts";

export interface ConsumerOptsBuilder {
  // FIXME: kill this
  pull(batch: number): void;
  // FIXME: kill this
  pullDirect(
    stream: string,
    consumer: string,
    batchSize: number,
  ): void;

  deliverTo(subject: string): void;
  queue(name: string): void;
  manualAck(): void;
  durable(name: string): void;
  deliverAll(): void;
  deliverLast(): void;
  deliverNew(): void;
  startSequence(seq: number): void;
  startTime(time: Date | Nanos): void;
  ackNone(): void;
  ackAll(): void;
  ackExplicit(): void;
  maxDeliver(max: number): void;
  maxAckPending(max: number): void;
  maxWaiting(max: number): void;
  maxMessages(max: number): void;
  callback(fn: JsMsgCallback): void;

  // FIXME: 503:
  // maxRetries()
  // retryBackoff()

  // ackWait(time)
  // replayOriginal()
  // rateLimit(bytesPerSec)
}

export function consumerOpts(): ConsumerOptsBuilder {
  return new ConsumerOptsBuilderImpl();
}

export class ConsumerOptsBuilderImpl implements ConsumerOptsBuilder {
  config: Partial<ConsumerConfig>;
  mack: boolean;
  pullCount: number;
  subQueue: string;
  stream: string;
  callbackFn?: JsMsgCallback;
  max?: number;

  constructor() {
    this.stream = "";
    this.pullCount = 0;
    this.subQueue = "";
    this.mack = false;
    this.config = defaultConsumer("");
    // not set
    this.config.ack_policy = AckPolicy.All;
  }

  getOpts(): ConsumerOpts {
    const o = {} as ConsumerOpts;
    o.config = this.config;
    o.mack = this.mack;
    o.pullCount = this.pullCount;
    o.subQueue = this.subQueue;
    o.stream = this.stream;
    o.callbackFn = this.callbackFn;
    o.max = this.max;
    return o;
  }

  pull(batch: number) {
    if (batch <= 0) {
      throw new Error("batch must be greater than 0");
    }
    this.pullCount = batch;
  }

  pullDirect(
    stream: string,
    consumer: string,
    batchSize: number,
  ): void {
    validateStreamName(stream);
    this.stream = stream;
    this.durable(consumer);
    this.pull(batchSize);
  }

  deliverTo(subject: string) {
    this.config.deliver_subject = subject;
  }

  queue(name: string) {
    this.subQueue = name;
  }

  manualAck() {
    this.mack = true;
  }

  durable(name: string) {
    validateDurableName(name);
    this.config.durable_name = name;
  }

  deliverAll() {
    this.config.deliver_policy = DeliverPolicy.All;
  }

  deliverLast() {
    this.config.deliver_policy = DeliverPolicy.Last;
  }

  deliverNew() {
    this.config.deliver_policy = DeliverPolicy.New;
  }

  startSequence(seq: number) {
    if (seq <= 0) {
      throw new Error("sequence must be greater than 0");
    }
    this.config.deliver_policy = DeliverPolicy.StartSequence;
    this.config.opt_start_seq = seq;
  }

  startTime(time: Date | Nanos) {
    let n: Nanos;
    if (typeof time === "number") {
      n = time as Nanos;
    } else {
      const d = time as Date;
      n = nanos(d.getTime());
    }
    this.config.deliver_policy = DeliverPolicy.StartTime;
    this.config.opt_start_time = n;
  }

  ackNone() {
    this.config.ack_policy = AckPolicy.None;
  }

  ackAll() {
    this.config.ack_policy = AckPolicy.All;
  }

  ackExplicit() {
    this.config.ack_policy = AckPolicy.Explicit;
  }

  maxDeliver(max: number) {
    this.config.max_deliver = max;
  }

  maxAckPending(max: number) {
    this.config.max_ack_pending = max;
  }

  maxWaiting(max: number) {
    this.config.max_waiting = max;
  }

  maxMessages(max: number) {
    this.max = max;
  }

  callback(fn: JsMsgCallback) {
    this.callbackFn = fn;
  }
}

export function isConsumerOptsBuilder(
  o: ConsumerOptsBuilder | ConsumerOpts,
): o is ConsumerOptsBuilderImpl {
  return typeof (o as ConsumerOptsBuilderImpl).getOpts === "function";
}

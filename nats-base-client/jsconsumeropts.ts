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
  ConsumerOptsBuilder,
  DeliverPolicy,
  JsMsgCallback,
  ReplayPolicy,
} from "./types.ts";
import { defaultConsumer, nanos, validateDurableName } from "./jsutil.ts";

export function consumerOpts(
  opts?: Partial<ConsumerConfig>,
): ConsumerOptsBuilder {
  return new ConsumerOptsBuilderImpl(opts);
}

// FIXME: some items here that may need to be addressed
// 503s?
// maxRetries()
// retryBackoff()
// ackWait(time)
// replayOriginal()
// rateLimit(bytesPerSec)
export class ConsumerOptsBuilderImpl implements ConsumerOptsBuilder {
  config: Partial<ConsumerConfig>;
  ordered: boolean;
  mack: boolean;
  stream: string;
  callbackFn?: JsMsgCallback;
  max?: number;
  qname?: string;

  constructor(opts?: Partial<ConsumerConfig>) {
    this.stream = "";
    this.mack = false;
    this.ordered = false;
    this.config = defaultConsumer("", opts || {});
  }

  getOpts(): ConsumerOpts {
    const o = {} as ConsumerOpts;
    o.config = this.config;
    o.mack = this.mack;
    o.stream = this.stream;
    o.callbackFn = this.callbackFn;
    o.max = this.max;
    o.queue = this.qname;
    o.ordered = this.ordered;
    o.config.ack_policy = o.ordered ? AckPolicy.None : o.config.ack_policy;
    return o;
  }

  description(description: string) {
    this.config.description = description;
  }

  deliverTo(subject: string) {
    this.config.deliver_subject = subject;
  }

  durable(name: string) {
    validateDurableName(name);
    this.config.durable_name = name;
  }

  startSequence(seq: number) {
    if (seq <= 0) {
      throw new Error("sequence must be greater than 0");
    }
    this.config.deliver_policy = DeliverPolicy.StartSequence;
    this.config.opt_start_seq = seq;
  }

  startTime(time: Date) {
    this.config.deliver_policy = DeliverPolicy.StartTime;
    this.config.opt_start_time = time.toISOString();
  }

  deliverAll() {
    this.config.deliver_policy = DeliverPolicy.All;
  }

  deliverLastPerSubject() {
    this.config.deliver_policy = DeliverPolicy.LastPerSubject;
  }

  deliverLast() {
    this.config.deliver_policy = DeliverPolicy.Last;
  }

  deliverNew() {
    this.config.deliver_policy = DeliverPolicy.New;
  }

  startAtTimeDelta(millis: number) {
    this.startTime(new Date(Date.now() - millis));
  }

  headersOnly() {
    this.config.headers_only = true;
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

  ackWait(millis: number) {
    this.config.ack_wait = nanos(millis);
  }

  maxDeliver(max: number) {
    this.config.max_deliver = max;
  }

  filterSubject(s: string) {
    this.config.filter_subject = s;
  }

  replayInstantly() {
    this.config.replay_policy = ReplayPolicy.Instant;
  }

  replayOriginal() {
    this.config.replay_policy = ReplayPolicy.Original;
  }

  sample(n: number) {
    n = Math.trunc(n);
    if (n < 0 || n > 100) {
      throw new Error(`value must be between 0-100`);
    }
    this.config.sample_freq = `${n}%`;
  }

  limit(n: number) {
    this.config.rate_limit_bps = n;
  }

  maxWaiting(max: number) {
    this.config.max_waiting = max;
  }

  maxAckPending(max: number) {
    this.config.max_ack_pending = max;
  }

  idleHeartbeat(millis: number) {
    this.config.idle_heartbeat = nanos(millis);
  }

  flowControl() {
    this.config.flow_control = true;
  }

  deliverGroup(name: string) {
    this.queue(name);
  }

  manualAck() {
    this.mack = true;
  }

  maxMessages(max: number) {
    this.max = max;
  }

  callback(fn: JsMsgCallback) {
    this.callbackFn = fn;
  }

  queue(n: string) {
    this.qname = n;
    this.config.deliver_group = n;
  }

  orderedConsumer() {
    this.ordered = true;
  }
}

export function isConsumerOptsBuilder(
  o: ConsumerOptsBuilder | Partial<ConsumerOpts>,
): o is ConsumerOptsBuilderImpl {
  return typeof (o as ConsumerOptsBuilderImpl).getOpts === "function";
}

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
  ConsumerAPI,
  ConsumerConfig,
  ConsumerOpts,
  DeliverPolicy,
  Empty,
  JetStreamClient,
  JetStreamOptions,
  JetStreamPublishOptions,
  JetStreamPullSubscription,
  JetStreamSubscription,
  JetStreamSubscriptionOptions,
  JsMsg,
  JsMsgCallback,
  Msg,
  Nanos,
  NatsConnection,
  PubAck,
  Pullable,
  PullOptions,
  RequestOptions,
  Subscription,
} from "./types.ts";
import { BaseApiClient } from "./jsbase_api.ts";
import {
  defaultConsumer,
  ns,
  validateDurableName,
  validateStreamName,
} from "./jsutil.ts";
import { ConsumerAPIImpl } from "./jsconsumer_api.ts";
import { ACK, toJsMsg } from "./jsmsg.ts";
import { TypedSubscription, TypedSubscriptionOptions } from "./typedsub.ts";
import { ErrorCode, NatsError } from "./error.ts";
import { SubscriptionImpl } from "./subscription.ts";
import { QueuedIterator } from "./queued_iterator.ts";
import { Timeout, timeout } from "./util.ts";
import { createInbox } from "./protocol.ts";
import { headers } from "./headers.ts";

interface JetStreamInfoable {
  info: JetStreamSubscriptionInfo | null;
}

enum PubHeaders {
  MsgIdHdr = "Nats-Msg-Id",
  ExpectedStreamHdr = "Nats-Expected-Stream",
  ExpectedLastSeqHdr = "Nats-Expected-Last-Sequence",
  ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id",
}

export class JetStreamClientImpl extends BaseApiClient
  implements JetStreamClient {
  api: ConsumerAPI;
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
    this.api = new ConsumerAPIImpl(nc, opts);
  }

  async publish(
    subj: string,
    data: Uint8Array = Empty,
    opts?: Partial<JetStreamPublishOptions>,
  ): Promise<PubAck> {
    opts = opts ?? {};
    opts.expect = opts.expect ?? {};
    const mh = headers();
    if (opts) {
      if (opts.msgID) {
        mh.set(PubHeaders.MsgIdHdr, opts.msgID);
      }
      if (opts.expect.lastMsgID) {
        mh.set(PubHeaders.ExpectedLastMsgIdHdr, opts.expect.lastMsgID);
      }
      if (opts.expect.streamName) {
        mh.set(PubHeaders.ExpectedStreamHdr, opts.expect.streamName);
      }
      if (opts.expect.lastSequence) {
        mh.set(PubHeaders.ExpectedLastSeqHdr, `${opts.expect.lastSequence}`);
      }
    }

    const to = opts.timeout ?? this.timeout;
    const ro = {} as RequestOptions;
    if (to) {
      ro.timeout = to;
    }
    if (opts) {
      ro.headers = mh;
    }

    const r = await this.nc.request(subj, data, ro);
    const pa = this.parseJsResponse(r) as PubAck;
    if (pa.stream === "") {
      throw NatsError.errorForCode(ErrorCode.JetStreamInvalidAck);
    }
    pa.duplicate = pa.duplicate ? pa.duplicate : false;
    return pa;
  }

  async pull(stream: string, durable: string): Promise<JsMsg> {
    validateStreamName(stream);
    validateDurableName(durable);
    const msg = await this.nc.request(
      `${this.prefix}.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      this.jc.encode({ no_wait: true, batch: 1 }),
      { noMux: true, timeout: this.timeout },
    );
    const err = checkJsError(msg);
    if (err) {
      throw (err);
    }
    return toJsMsg(msg);
  }

  /*
  * Returns available messages upto specified batch count.
  * If expires is set the iterator will wait for the specified
  * ammount of millis before closing the subscription.
  * If no_wait is specified, the iterator will return no messages.
  * @param stream
  * @param durable
  * @param opts
  */
  pullBatch(
    stream: string,
    durable: string,
    opts: Partial<PullOptions> = { batch: 1 },
  ): QueuedIterator<JsMsg> {
    validateStreamName(stream);
    validateDurableName(durable);

    let timer: Timeout<void> | null = null;

    const args: Partial<PullOptions> = {};
    args.batch = opts.batch ?? 1;
    args.no_wait = opts.no_wait ?? false;
    const expires = opts.expires ?? 0;
    if (expires) {
      args.expires = ns(expires);
    }
    if (expires === 0 && args.no_wait === false) {
      throw new Error("expires or no_wait is required");
    }

    const qi = new QueuedIterator<JsMsg>();
    const wants = opts.batch;
    let received = 0;
    qi.dispatchedFn = (m: JsMsg | null) => {
      if (m) {
        received++;
        if (timer && m.info.pending === 0) {
          // the expiration will close it
          return;
        }
        // if we have one pending and we got the expected
        // or there are no more stop the iterator
        if (
          qi.getPending() === 1 && m.info.pending === 0 || wants === received
        ) {
          qi.stop();
        }
      }
    };

    const inbox = createInbox(this.nc.options.inboxPrefix);
    const sub = this.nc.subscribe(inbox, {
      max: opts.batch,
      callback: (err: Error | null, msg) => {
        if (err === null) {
          err = checkJsError(msg);
        }
        if (err) {
          if (timer) {
            timer.cancel();
            timer = null;
          }
          qi.stop(err);
        } else {
          qi.received++;
          qi.push(toJsMsg(msg));
        }
      },
    });

    // timer on the client  the issue is that the request
    // is started on the client, which means that it will expire
    // on the client first
    if (expires) {
      timer = timeout<void>(expires);
      timer.catch(() => {
        if (!sub.isClosed()) {
          sub.drain();
          timer = null;
        }
      });
    }

    (async () => {
      // close the iterator if the connection or subscription closes unexpectedly
      await (sub as SubscriptionImpl).closed;
      if (timer !== null) {
        timer.cancel();
      }
      qi.stop();
    })().catch();

    this.nc.publish(
      `${this.prefix}.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      this.jc.encode(args),
      { reply: inbox },
    );
    return qi;
  }

  pullSubscribe(
    subject: string,
    opts: ConsumerOptsBuilder | ConsumerOpts = consumerOpts(),
  ): Promise<JetStreamPullSubscription> {
    return this.subscribe(
      subject,
      opts,
    ) as unknown as Promise<JetStreamPullSubscription>;
  }

  async subscribe(
    subject: string,
    opts: ConsumerOptsBuilder | ConsumerOpts = consumerOpts(),
  ): Promise<JetStreamSubscription> {
    const cso =
      (isConsumerOptsBuilder(opts)
        ? opts.getOpts()
        : opts) as JetStreamSubscriptionInfo;
    cso.api = this;
    cso.config = cso.config ?? {} as ConsumerConfig;
    if (cso.pullCount) {
      const ackPolicy = cso.config.ack_policy;
      if (ackPolicy === AckPolicy.None || ackPolicy === AckPolicy.All) {
        throw new Error("ack policy for pull consumers must be explicit");
      }
    }
    cso.stream = cso.stream === ""
      ? await this.findStream(subject)
      : cso.stream;

    cso.attached = false;
    if (cso.consumer) {
      const info = await this.api.info(cso.stream, cso.consumer);
      if (info) {
        if (
          info.config.filter_subject && info.config.filter_subject !== subject
        ) {
          throw new Error("subject does not match consumer");
        }
        cso.config = info.config;
        cso.attached = true;
      }
    }

    if (cso.attached) {
      cso.config.filter_subject = subject;
      if (cso.pullCount === 0) {
        cso.config.deliver_subject = createInbox(this.nc.options.inboxPrefix);
      }
    }

    cso.config.deliver_subject = cso.config.deliver_subject ??
      createInbox(this.nc.options.inboxPrefix);

    // configure the typed subscription
    const so = {} as TypedSubscriptionOptions<JsMsg>;
    so.adapter = jsMsgAdapter;
    so.cleanupFn = jsCleanupFn;
    if (cso.callbackFn) {
      so.callback = cso.callbackFn;
    }
    if (!cso.mack) {
      so.dispatchedFn = autoAckJsMsg;
    }
    so.max = cso.max ?? 0;

    let sub: JetStreamSubscription & JetStreamInfoable;
    if (cso.pullCount) {
      sub = new JetStreamPullSubscriptionImpl(
        this,
        cso.config.deliver_subject,
        so,
      );
    } else {
      sub = new JetStreamSubscriptionImpl(
        this,
        cso.config.deliver_subject,
        so,
      );
    }
    if (!cso.attached) {
      // create the consumer
      try {
        const ci = await this.api.add(cso.stream, cso.config);
        cso.config = ci.config;
      } catch (err) {
        sub.unsubscribe();
        throw err;
      }
    }
    sub.info = cso;

    if (cso.pullCount) {
      (sub as unknown as JetStreamPullSubscription).pull(cso.pullCount);
    }

    return sub;
  }
}

class JetStreamSubscriptionImpl extends TypedSubscription<JsMsg>
  implements JetStreamInfoable {
  constructor(
    js: BaseApiClient,
    subject: string,
    opts: JetStreamSubscriptionOptions,
  ) {
    super(js.nc, subject, opts);
  }

  set info(info: JetStreamSubscriptionInfo | null) {
    (this.sub as SubscriptionImpl).info = info;
  }
  get info(): JetStreamSubscriptionInfo | null {
    if (this.sub.info) {
      return this.sub.info as JetStreamSubscriptionInfo;
    }
    return null;
  }
}

class JetStreamPullSubscriptionImpl extends JetStreamSubscriptionImpl
  implements Pullable {
  constructor(
    js: BaseApiClient,
    subject: string,
    opts: TypedSubscriptionOptions<JsMsg>,
  ) {
    super(js, subject, opts);
  }
  pull(batch: number, opts: Partial<PullOptions> = { batch: 1 }): void {
    const { stream, consumer } = this.sub.info as JetStreamSubscriptionInfo;
    const args: Partial<PullOptions> = {};
    args.batch = opts.batch ?? 1;
    args.no_wait = opts.no_wait ?? false;

    if (this.info) {
      const api = (this.info.api as BaseApiClient);
      api.nc.publish(
        `${api.prefix}.CONSUMER.MSG.NEXT.${stream}.${consumer}`,
        api.jc.encode(args),
        { reply: this.sub.subject },
      );
    }
  }
}

interface JetStreamSubscriptionInfo extends ConsumerOpts {
  api: BaseApiClient;
  attached: boolean;
}

export function consumerOpts(): ConsumerOptsBuilder {
  return new ConsumerOptsBuilderImpl();
}

function isConsumerOptsBuilder(
  o: ConsumerOptsBuilder | ConsumerOpts,
): o is ConsumerOptsBuilderImpl {
  return typeof (o as ConsumerOptsBuilderImpl).getOpts === "function";
}

export interface ConsumerOptsBuilder {
  pull(batch: number): void;
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
}

class ConsumerOptsBuilderImpl implements ConsumerOptsBuilder {
  config: Partial<ConsumerConfig>;
  consumer: string;
  mack: boolean;
  pullCount: number;
  subQueue: string;
  stream: string;
  callbackFn?: JsMsgCallback;
  max?: number;
  debug?: boolean;

  constructor() {
    this.stream = "";
    this.consumer = "";
    this.pullCount = 0;
    this.subQueue = "";
    this.mack = false;
    this.config = defaultConsumer("");
    // not set
    this.config.ack_policy = AckPolicy.None;
  }

  getOpts(): ConsumerOpts {
    const o = {} as ConsumerOpts;
    o.config = this.config;
    o.consumer = this.consumer;
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
    this.consumer = consumer;
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
      n = ns(d.getTime());
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

  _debug(tf: boolean) {
    this.debug = tf;
  }
}

function jsMsgAdapter(
  err: NatsError | null,
  msg: Msg,
): [NatsError | null, JsMsg | null] {
  if (err) {
    return [err, null];
  }
  const jm = toJsMsg(msg);
  try {
    // this will throw if not a JsMsg
    jm.info;
    return [null, jm];
  } catch (err) {
    return [err, null];
  }
}

function autoAckJsMsg(data: JsMsg | null) {
  if (data) {
    data.ack();
  }
}

function jsCleanupFn(sub: Subscription, info?: unknown) {
  const jinfo = info as JetStreamSubscriptionInfo;
  // FIXME: need a property on the subscription that tells if drained
  // drained subs should be skipped as well
  if (jinfo.attached || jinfo.config.durable_name) {
    return;
  }
  jinfo.api._request(
    `${jinfo.api.prefix}.CONSUMER.DELETE.${jinfo.stream}.${jinfo.config.durable_name}`,
  )
    .catch((err) => {
      // FIXME: dispatch an error
    });
}

export function autoAck(sub: Subscription) {
  const s = sub as SubscriptionImpl;
  s.setDispatchedFn((msg) => {
    if (msg) {
      msg.respond(ACK);
    }
  });
}

function checkJsError(msg: Msg): Error | null {
  const h = msg.headers;
  if (!h) {
    return null;
  }
  if (h.code === 0 || (h.code >= 200 && h.code < 300)) {
    return null;
  }
  switch (h.code) {
    case 404:
      return new Error("no messages");
    case 408:
      return new Error("too many pulls");
    case 409:
      return new Error("max ack pending exceeded");
    default:
      return new Error(h.status);
  }
}

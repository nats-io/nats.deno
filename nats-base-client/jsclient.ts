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
  Events,
  JetStreamClient,
  JetStreamOptions,
  JetStreamPublishOptions,
  JetStreamPullSubscription,
  JetStreamSubscription,
  JetStreamSubscriptionOptions,
  JsMsg,
  Msg,
  NatsConnection,
  PubAck,
  Pullable,
  PullOptions,
  ReplayPolicy,
  RequestOptions,
  Status,
  Subscription,
} from "./types.ts";
import { BaseApiClient } from "./jsbase_api.ts";
import { nanos, validateDurableName, validateStreamName } from "./jsutil.ts";
import { ConsumerAPIImpl } from "./jsconsumer_api.ts";
import { ACK, toJsMsg } from "./jsmsg.ts";
import {
  MsgAdapter,
  TypedSubscription,
  TypedSubscriptionOptions,
} from "./typedsub.ts";
import { ErrorCode, isNatsError, NatsError } from "./error.ts";
import { SubscriptionImpl } from "./subscription.ts";
import { QueuedIterator } from "./queued_iterator.ts";
import { Timeout, timeout } from "./util.ts";
import { createInbox } from "./protocol.ts";
import { headers } from "./headers.ts";
import { consumerOpts, isConsumerOptsBuilder } from "./consumeropts.ts";
import type { ConsumerOptsBuilder } from "./consumeropts.ts";

export interface JetStreamInfoable {
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
  * amount of millis before closing the subscription.
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
      args.expires = nanos(expires);
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
        if (err !== null) {
          if (timer) {
            timer.cancel();
            timer = null;
          }
          if (
            isNatsError(err) && err.code === ErrorCode.JetStream404NoMessages
          ) {
            qi.stop();
          } else {
            qi.stop(err);
          }
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
        timer = null;
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
    const co =
      (isConsumerOptsBuilder(opts) ? opts.getOpts() : opts) as ConsumerOpts;
    co.pullCount = co.pullCount > 0 ? co.pullCount : 1;
    return this.subscribe(
      subject,
      co,
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
    // deduce the stream
    cso.stream = cso.stream ? cso.stream : await this.findStream(subject);
    // deduce if durable
    if (cso.config.durable_name) {
      cso.consumer = cso.config.durable_name;
    }

    cso.attached = false;
    if (cso.consumer) {
      try {
        const info = await this.api.info(cso.stream, cso.consumer);
        if (info) {
          if (cso.pullCount === 0 && !info.config.deliver_subject) {
            throw new Error(
              "consumer info specifies a pull consumer - pullCount must be specified",
            );
          }
          if (
            info.config.filter_subject && info.config.filter_subject !== subject
          ) {
            throw new Error("subject does not match consumer");
          }
          cso.config = info.config;
          cso.attached = true;
        }
      } catch (err) {
        //consumer doesn't exist
        if (err.code !== "404") {
          throw err;
        }
      }
    }

    if (cso.attached) {
      cso.config.filter_subject = subject;
      if (cso.pullCount === 0) {
        cso.config.deliver_subject = createInbox(this.nc.options.inboxPrefix);
      }
    } else if (cso.pullCount) {
      const ackPolicy = cso.config.ack_policy;
      if (ackPolicy === AckPolicy.None || ackPolicy === AckPolicy.All) {
        throw new Error("ack policy for pull consumers must be explicit");
      }
    }

    cso.config.deliver_subject = cso.config.deliver_subject ??
      createInbox(this.nc.options.inboxPrefix);

    // configure the typed subscription
    const so = {} as TypedSubscriptionOptions<JsMsg>;
    so.adapter = msgAdapter(cso.callbackFn === undefined);
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
        // make sure that we have some defaults
        cso.config = Object.assign({
          deliver_policy: DeliverPolicy.All,
          ack_policy: AckPolicy.Explicit,
          ack_wait: nanos(30 * 1000),
          replay_policy: ReplayPolicy.Instant,
        }, cso.config);

        const ci = await this.api.add(cso.stream, cso.config);
        cso.config = ci.config;
      } catch (err) {
        sub.unsubscribe();
        throw err;
      }
    }
    sub.info = cso;

    if (cso.pullCount) {
      (sub as unknown as JetStreamPullSubscription).pull({
        batch: cso.pullCount,
      });
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
  pull(opts: Partial<PullOptions> = { batch: 1 }): void {
    const { stream, consumer } = this.sub.info as JetStreamSubscriptionInfo;
    const args: Partial<PullOptions> = {};
    args.batch = opts.batch ?? 1;
    args.no_wait = opts.no_wait ?? false;
    if (opts.expires && opts.expires > 0) {
      args.expires = opts.expires;
    }

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

function msgAdapter(iterator: boolean): MsgAdapter<JsMsg> {
  if (iterator) {
    return iterMsgAdapter;
  } else {
    return cbMsgAdapter;
  }
}

function cbMsgAdapter(
  err: NatsError | null,
  msg: Msg,
): [NatsError | null, JsMsg | null] {
  if (err) {
    return [err, null];
  }
  err = checkJsError(msg);
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

function iterMsgAdapter(
  err: NatsError | null,
  msg: Msg,
): [NatsError | null, JsMsg | null] {
  if (err) {
    return [err, null];
  }

  // iterator will close if we have an error
  // check for errors that shouldn't close it
  const ne = checkJsError(msg);
  if (ne !== null) {
    switch (ne.code) {
      case ErrorCode.JetStream404NoMessages:
      case ErrorCode.JetStream408RequestTimeout:
      case ErrorCode.JetStream409MaxAckPendingExceeded:
        return [null, null];
      default:
        return [ne, null];
    }
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
  const si = sub as SubscriptionImpl;
  if (si.drained || jinfo.attached || jinfo.config.durable_name) {
    return;
  }
  const subj =
    `${jinfo.api.prefix}.CONSUMER.DELETE.${jinfo.stream}.${jinfo.config.durable_name}`;
  jinfo.api._request(subj)
    .catch((err) => {
      const desc = `${subj}: ${err.message}`;
      const s = { type: Events.Error, data: desc } as Status;
      jinfo.api.nc.protocol.dispatchStatus(s);
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

function checkJsError(msg: Msg): NatsError | null {
  const h = msg.headers;
  if (!h) {
    return null;
  }
  if (h.code < 300) {
    return null;
  }
  switch (h.code) {
    case 404:
      return NatsError.errorForCode(ErrorCode.JetStream404NoMessages);
    case 408:
      return NatsError.errorForCode(ErrorCode.JetStream408RequestTimeout);
    case 409:
      return NatsError.errorForCode(
        ErrorCode.JetStream409MaxAckPendingExceeded,
      );
    default:
      return NatsError.errorForCode(ErrorCode.Unknown, new Error(h.status));
  }
}

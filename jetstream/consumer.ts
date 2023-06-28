/*
 * Copyright 2022-2023 The NATS Authors
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
import { deferred, Timeout, timeout } from "../nats-base-client/util.ts";
import { ConsumerAPI, ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import { nuid } from "../nats-base-client/nuid.ts";
import { isHeartbeatMsg, nanos } from "./jsutil.ts";
import { QueuedIteratorImpl } from "../nats-base-client/queued_iterator.ts";
import {
  createInbox,
  Events,
  MsgHdrs,
  NatsError,
  QueuedIterator,
  Status,
  Subscription,
} from "../nats-base-client/core.ts";
import * as core from "../nats-base-client/idleheartbeat.ts";
import { JsMsg, toJsMsg } from "./jsmsg.ts";
import { MsgImpl } from "../nats-base-client/msg.ts";
import {
  AckPolicy,
  ConsumerConfig,
  ConsumerInfo,
  DeliverPolicy,
  PullOptions,
  ReplayPolicy,
} from "./jsapi_types.ts";
import { JsHeaders } from "./types.ts";
import { SubscriptionImpl } from "../nats-base-client/protocol.ts";

enum PullConsumerType {
  Unset = -1,
  Consume,
  Fetch,
}

export type Ordered = {
  ordered: true;
};
export type NextOptions = Expires;
export type ConsumeBytes =
  & MaxBytes
  & Partial<MaxMessages>
  & ThresholdBytes
  & Expires
  & IdleHeartbeat
  & ConsumeCallback;
export type ConsumeMessages =
  & Partial<MaxMessages>
  & ThresholdMessages
  & Expires
  & IdleHeartbeat
  & ConsumeCallback;
export type ConsumeOptions = ConsumeBytes | ConsumeMessages;
/**
 * Options for fetching
 */
export type FetchBytes =
  & MaxBytes
  & Partial<MaxMessages>
  & Expires
  & IdleHeartbeat;
/**
 * Options for a c
 */
export type FetchMessages =
  & Partial<MaxMessages>
  & Expires
  & IdleHeartbeat;
export type FetchOptions = FetchBytes | FetchMessages;
export type PullConsumerOptions = FetchOptions | ConsumeOptions;
export type MaxMessages = {
  /**
   * Maximum number of messages to retrieve.
   * @default 100 messages
   */
  max_messages: number;
};
export type MaxBytes = {
  /**
   * Maximum number of bytes to retrieve - note request must fit the entire message
   * to be honored (this includes, subject, headers, etc). Partial messages are not
   * supported.
   */
  max_bytes: number;
};
export type ThresholdMessages = {
  /**
   * Threshold message count on which the client will auto-trigger additional requests
   * from the server. This is only applicable to `consume`.
   * @default  75% of {@link MaxMessages}.
   */
  threshold_messages?: number;
};
export type ThresholdBytes = {
  /**
   * Threshold bytes on which the client wil auto-trigger additional message requests
   * from the server. This is only applicable to `consume`.
   * @default 75% of {@link MaxBytes}.
   */
  threshold_bytes?: number;
};
export type Expires = {
  /**
   * Amount of milliseconds to wait for messages before issuing another request.
   * Note this value shouldn't be set by the user, as the default provides proper behavior.
   * A low value will stress the server.
   *
   * Minimum value is 1000 (1s).
   * @default 30_000 (30s)
   */
  expires?: number;
};
export type IdleHeartbeat = {
  /**
   * Number of milliseconds to wait for a server heartbeat when not actively receiving
   * messages. When two or more heartbeats are missed in a row, the consumer will emit
   * a notification. Note this value shouldn't be set by the user, as the default provides
   * the proper behavior. A low value will stress the server.
   */
  idle_heartbeat?: number;
};
export type ConsumerCallbackFn = (r: JsMsg) => void;
export type ConsumeCallback = {
  /**
   * Process messages using a callback instead of an iterator. Note that when using callbacks,
   * the callback cannot be async. If you must use async functionality, process messages
   * using an iterator.
   */
  callback?: ConsumerCallbackFn;
};

/**
 * ConsumerEvents are informational notifications emitted by ConsumerMessages
 * that may be of interest to a client.
 */
export enum ConsumerEvents {
  /**
   * Notification that heartbeats were missed. This notification is informational.
   * The `data` portion of the status, is a number indicating the number of missed heartbeats.
   * Note that when a client disconnects, heartbeat tracking is paused while
   * the client is disconnected.
   */
  HeartbeatsMissed = "heartbeats_missed",
}

/**
 * These events represent informational notifications emitted by ConsumerMessages
 * that can be safely ignored by clients.
 */
export enum ConsumerDebugEvents {
  /**
   * DebugEvents are effectively statuses returned by the server that were ignored
   * by the client. The `data` portion of the
   * status is just a string indicating the code/message of the status.
   */
  DebugEvent = "debug",
  /**
   * Requests for messages can be terminated by the server, these notifications
   * provide information on the number of messages and/or bytes that couldn't
   * be satisfied by the consumer request. The `data` portion of the status will
   * have the format of `{msgsLeft: number, bytesLeft: number}`.
   */
  Discard = "discard",
  /**
   * Notifies whenever there's a request for additional messages from the server.
   * This notification telegraphs the request options, which should be treated as
   * read-only. This notification is only useful for debugging. Data is PullOptions.
   */
  Next = "next",
}

export interface ConsumerStatus {
  type: ConsumerEvents | ConsumerDebugEvents;
  data: unknown;
}

export interface ExportedConsumer {
  next(
    opts?: NextOptions,
  ): Promise<JsMsg | null>;

  fetch(
    opts?: FetchOptions,
  ): Promise<ConsumerMessages>;

  consume(
    opts?: ConsumeOptions,
  ): Promise<ConsumerMessages>;
}

export interface Consumer extends ExportedConsumer {
  info(cached?: boolean): Promise<ConsumerInfo>;

  delete(): Promise<boolean>;
}

export interface Close {
  close(): Promise<void>;
}

export interface ConsumerMessages extends QueuedIterator<JsMsg>, Close {
  status(): Promise<AsyncIterable<ConsumerStatus>>;
}

export class PullConsumerMessagesImpl extends QueuedIteratorImpl<JsMsg>
  implements ConsumerMessages {
  consumer: PullConsumerImpl;
  opts: Record<string, number>;
  sub: Subscription;
  monitor: core.IdleHeartbeat | null;
  pending: { msgs: number; bytes: number; requests: number };
  inbox: string;
  refilling: boolean;
  stack: string;
  pong!: Promise<void> | null;
  callback: ConsumerCallbackFn | null;
  timeout: Timeout<unknown> | null;
  cleanupHandler?: () => void;
  listeners: QueuedIterator<ConsumerStatus>[];
  statusIterator?: QueuedIteratorImpl<Status>;

  // callback: ConsumerCallbackFn;
  constructor(
    c: PullConsumerImpl,
    opts: ConsumeOptions | FetchOptions,
    refilling = false,
  ) {
    super();
    this.consumer = c;

    this.opts = this.parseOptions(opts, refilling);
    this.callback = (opts as ConsumeCallback).callback || null;
    this.noIterator = typeof this.callback === "function";
    this.monitor = null;
    this.pong = null;
    this.pending = { msgs: 0, bytes: 0, requests: 0 };
    this.refilling = refilling;
    this.stack = new Error().stack!.split("\n").slice(1).join("\n");
    this.timeout = null;
    this.inbox = createInbox();
    this.listeners = [];

    const {
      max_messages,
      max_bytes,
      idle_heartbeat,
      threshold_bytes,
      threshold_messages,
    } = this.opts;

    // ordered consumer requires the ability to reset the
    // source pull consumer, if promise is registered and
    // close is called, the pull consumer will emit a close
    // which will close the ordered consumer, by registering
    // the close with a handler, we can replace it.
    this.closed().then(() => {
      if (this.cleanupHandler) {
        try {
          this.cleanupHandler();
        } catch (_err) {
          // nothing
        }
      }
    });

    const { sub } = this;
    if (sub) {
      sub.unsubscribe();
    }

    this.sub = c.api.nc.subscribe(this.inbox, {
      callback: (err, msg) => {
        if (err) {
          // this is possibly only a permissions error which means
          // that the server rejected (eliminating the sub)
          // or the client never had permissions to begin with
          // so this is terminal
          this.stop();
          return;
        }
        this.monitor?.work();
        const isProtocol = msg.subject === this.inbox;

        if (isProtocol) {
          if (isHeartbeatMsg(msg)) {
            return;
          }
          const code = msg.headers?.code;
          const description = msg.headers?.description?.toLowerCase() ||
            "unknown";
          const { msgsLeft, bytesLeft } = this.parseDiscard(msg.headers);
          if (msgsLeft > 0 || bytesLeft > 0) {
            this.pending.msgs -= msgsLeft;
            this.pending.bytes -= bytesLeft;
            this.pending.requests--;
            this.notify(ConsumerDebugEvents.Discard, { msgsLeft, bytesLeft });
          } else {
            // FIXME: 408 can be a Timeout or bad request,
            //  or it can be sent if a nowait request was
            //  sent when other waiting requests are pending
            //  "Requests Pending"

            // FIXME: 400 bad request Invalid Heartbeat or Unmarshalling Fails
            //  these are real bad values - so this is bad request
            //  fail on this
            const toErr = (): Error => {
              const err = new NatsError(description, `${code}`);
              err.stack += `\n\n${this.stack}`;
              return err;
            };

            // we got a bad request - no progress here
            if (code === 400) {
              const error = toErr();
              //@ts-ignore: fn
              this._push(() => {
                this.stop(error);
              });
            } else if (code === 409 && description === "consumer deleted") {
              const error = toErr();
              this.stop(error);
            } else {
              this.notify(
                ConsumerDebugEvents.DebugEvent,
                `${code} ${description}`,
              );
            }
          }
        } else {
          // push the user message
          this._push(toJsMsg(msg));
          this.received++;
          if (this.pending.msgs) {
            this.pending.msgs--;
          }
          if (this.pending.bytes) {
            this.pending.bytes -= (msg as MsgImpl).size();
          }
        }

        // if we don't have pending bytes/messages we are done or starving
        if (this.pending.msgs === 0 && this.pending.bytes === 0) {
          this.pending.requests = 0;
        }
        if (this.refilling) {
          // FIXME: this could result in  1/4 = 0
          if (
            (max_messages &&
              this.pending.msgs <= threshold_messages) ||
            (max_bytes && this.pending.bytes <= threshold_bytes)
          ) {
            const batch = this.pullOptions();
            // @ts-ignore: we are pushing the pull fn
            this.pull(batch);
          }
        } else if (this.pending.requests === 0) {
          // @ts-ignore: we are pushing the pull fn
          this._push(() => {
            this.stop();
          });
        }
      },
    });

    this.sub.closed.then(() => {
      // for ordered consumer we cannot break the iterator
      if ((this.sub as SubscriptionImpl).draining) {
        // @ts-ignore: we are pushing the pull fn
        this._push(() => {
          this.stop();
        });
      }
    });

    if (idle_heartbeat) {
      this.monitor = new core.IdleHeartbeat(idle_heartbeat, (data): boolean => {
        // for the pull consumer - missing heartbeats may be corrected
        // on the next pull etc - the only assumption here is we should
        // reset and check if the consumer was deleted from under us
        this.notify(ConsumerEvents.HeartbeatsMissed, data);
        this.resetPending()
          .then(() => {
          })
          .catch(() => {
          });
        return false;
      }, { maxOut: 2 });
    }

    // now if we disconnect, the consumer could be gone
    // or we were slow consumer'ed by the server
    (async () => {
      const status = c.api.nc.status();
      this.statusIterator = status as QueuedIteratorImpl<Status>;
      for await (const s of status) {
        switch (s.type) {
          case Events.Disconnect:
            // don't spam hb errors if we are disconnected
            // @ts-ignore: optional chaining
            this.monitor?.cancel();
            break;
          case Events.Reconnect:
            // do some sanity checks and reset
            // if that works resume the monitor
            this.resetPending()
              .then((ok) => {
                if (ok) {
                  // @ts-ignore: optional chaining
                  this.monitor?.restart();
                }
              })
              .catch(() => {
                // ignored - this should have fired elsewhere
              });
            break;
          default:
            // ignored
        }
      }
    })();

    // this is the initial pull
    this.pull(this.pullOptions());
  }

  _push(r: JsMsg) {
    if (!this.callback) {
      super.push(r);
    } else {
      const fn = typeof r === "function" ? r as () => void : null;
      try {
        if (!fn) {
          this.callback(r);
        } else {
          fn();
        }
      } catch (err) {
        this.stop(err);
      }
    }
  }

  notify(type: ConsumerEvents | ConsumerDebugEvents, data: unknown) {
    if (this.listeners.length > 0) {
      (() => {
        this.listeners.forEach((l) => {
          if (!(l as QueuedIteratorImpl<ConsumerStatus>).done) {
            l.push({ type, data });
          }
        });
      })();
    }
  }

  resetPending(): Promise<boolean> {
    // check we exist
    return this.consumer.info()
      .then((_ci) => {
        // we exist, so effectively any pending state is gone
        // so reset and re-pull
        this.pending.msgs = 0;
        this.pending.bytes = 0;
        this.pending.requests = 0;
        this.pull(this.pullOptions());
        return true;
      })
      .catch((err) => {
        // game over
        if (err.message === "consumer not found") {
          this.stop(err);
        }
        return false;
      });
  }

  pull(opts: Partial<PullOptions>) {
    this.pending.bytes += opts.max_bytes ?? 0;
    this.pending.msgs += opts.batch ?? 0;
    this.pending.requests++;

    const nc = this.consumer.api.nc;
    //@ts-ignore: iterator will pull
    this._push(() => {
      nc.publish(
        `${this.consumer.api.prefix}.CONSUMER.MSG.NEXT.${this.consumer.stream}.${this.consumer.name}`,
        this.consumer.api.jc.encode(opts),
        { reply: this.inbox },
      );
      this.notify(ConsumerDebugEvents.Next, opts);
    });
  }

  pullOptions(): Partial<PullOptions> {
    const batch = this.opts.max_messages - this.pending.msgs;
    const max_bytes = this.opts.max_bytes - this.pending.bytes;
    const idle_heartbeat = nanos(this.opts.idle_heartbeat);
    const expires = nanos(this.opts.expires);
    return { batch, max_bytes, idle_heartbeat, expires };
  }

  parseDiscard(
    headers?: MsgHdrs,
  ): { msgsLeft: number; bytesLeft: number } {
    const discard = {
      msgsLeft: 0,
      bytesLeft: 0,
    };
    const msgsLeft = headers?.get(JsHeaders.PendingMessagesHdr);
    if (msgsLeft) {
      discard.msgsLeft = parseInt(msgsLeft);
    }
    const bytesLeft = headers?.get(JsHeaders.PendingBytesHdr);
    if (bytesLeft) {
      discard.bytesLeft = parseInt(bytesLeft);
    }

    return discard;
  }

  trackTimeout(t: Timeout<unknown>) {
    this.timeout = t;
  }

  close(): Promise<void> {
    this.stop();
    return this.iterClosed;
  }

  closed(): Promise<void> {
    return this.iterClosed;
  }

  clearTimers() {
    this.monitor?.cancel();
    this.monitor = null;
    this.timeout?.cancel();
    this.timeout = null;
  }

  setCleanupHandler(fn?: () => void) {
    this.cleanupHandler = fn;
  }

  stop(err?: Error) {
    this.sub?.unsubscribe();
    this.clearTimers();
    this.statusIterator?.stop();
    //@ts-ignore: fn
    this._push(() => {
      super.stop(err);
      this.listeners.forEach((n) => {
        n.stop();
      });
    });
  }

  parseOptions(
    opts: PullConsumerOptions,
    refilling = false,
  ): Record<string, number> {
    const args = (opts || {}) as Record<string, number>;
    args.max_messages = args.max_messages || 0;
    args.max_bytes = args.max_bytes || 0;

    if (args.max_messages !== 0 && args.max_bytes !== 0) {
      throw new Error(
        `only specify one of max_messages or max_bytes`,
      );
    }

    // we must have at least one limit - default to 100 msgs
    // if they gave bytes but no messages, we will clamp
    // if they gave byte limits, we still need a message limit
    // or the server will send a single message and close the
    // request
    if (args.max_messages === 0) {
      // FIXME: if the server gives end pull completion, then this is not
      //   needed - the client will get 1 message but, we'll know that it
      //   worked - but we'll add a lot of latency, since all requests
      //   will end after one message
      args.max_messages = 100;
    }

    args.expires = args.expires || 30_000;
    if (args.expires < 1000) {
      throw new Error("expires should be at least 1000ms");
    }

    // require idle_heartbeat
    args.idle_heartbeat = args.idle_heartbeat || args.expires / 2;
    args.idle_heartbeat = args.idle_heartbeat > 30_000
      ? 30_000
      : args.idle_heartbeat;

    if (refilling) {
      const minMsgs = Math.round(args.max_messages * .75) || 1;
      args.threshold_messages = args.threshold_messages || minMsgs;

      const minBytes = Math.round(args.max_bytes * .75) || 1;
      args.threshold_bytes = args.threshold_bytes || minBytes;
    }

    return args;
  }

  status(): Promise<AsyncIterable<ConsumerStatus>> {
    const iter = new QueuedIteratorImpl<ConsumerStatus>();
    this.listeners.push(iter);
    return Promise.resolve(iter);
  }
}

export class OrderedConsumerMessages extends QueuedIteratorImpl<JsMsg>
  implements ConsumerMessages {
  src!: PullConsumerMessagesImpl;

  constructor() {
    super();
  }

  setSource(src: PullConsumerMessagesImpl) {
    if (this.src) {
      this.src.setCleanupHandler();
      this.src.stop();
    }
    this.src = src;
    this.src.setCleanupHandler(() => {
      this.close().catch();
    });
  }

  stop(err?: Error): void {
    this.src?.stop(err);
    super.stop(err);
  }

  close(): Promise<void> {
    this.stop();
    return this.iterClosed;
  }

  status(): Promise<AsyncIterable<ConsumerStatus>> {
    return Promise.reject(
      new Error("ordered consumers don't report consumer status"),
    );
  }
}

export class PullConsumerImpl implements Consumer {
  api: ConsumerAPIImpl;
  _info: ConsumerInfo;
  stream: string;
  name: string;

  constructor(api: ConsumerAPI, info: ConsumerInfo) {
    this.api = api as ConsumerAPIImpl;
    this._info = info;
    this.stream = info.stream_name;
    this.name = info.name;
  }

  consume(
    opts: ConsumeOptions = {
      max_messages: 100,
      expires: 30_000,
    } as ConsumeMessages,
  ): Promise<ConsumerMessages> {
    return Promise.resolve(new PullConsumerMessagesImpl(this, opts, true));
  }

  fetch(
    opts: FetchOptions = {
      max_messages: 100,
      expires: 30_000,
    } as FetchMessages,
  ): Promise<ConsumerMessages> {
    const m = new PullConsumerMessagesImpl(this, opts, false);
    // FIXME: need some way to pad this correctly
    const to = Math.round(m.opts.expires * 1.05);
    const timer = timeout(to);
    m.closed().then(() => {
      timer.cancel();
    });
    timer.catch(() => {
      m.close().catch();
    });
    m.trackTimeout(timer);

    return Promise.resolve(m);
  }

  next(
    opts: NextOptions = { expires: 30_000 },
  ): Promise<JsMsg | null> {
    const d = deferred<JsMsg | null>();
    const fopts = opts as FetchMessages;
    fopts.max_messages = 1;

    const iter = new PullConsumerMessagesImpl(this, fopts, false);

    // FIXME: need some way to pad this correctly
    const to = Math.round(iter.opts.expires * 1.05);

    // watch the messages for heartbeats missed
    if (to >= 60_000) {
      (async () => {
        for await (const s of await iter.status()) {
          if (
            s.type === ConsumerEvents.HeartbeatsMissed &&
            (s.data as number) >= 2
          ) {
            d.reject(new Error("consumer missed heartbeats"));
            break;
          }
        }
      })().catch();
    }

    (async () => {
      for await (const m of iter) {
        d.resolve(m);
        break;
      }
    })().catch();
    const timer = timeout(to);
    iter.closed().then(() => {
      d.resolve(null);
      timer.cancel();
    }).catch((err) => {
      d.reject(err);
    });
    timer.catch((_err) => {
      d.resolve(null);
      iter.close().catch();
    });
    iter.trackTimeout(timer);

    return d;
  }

  delete(): Promise<boolean> {
    const { stream_name, name } = this._info;
    return this.api.delete(stream_name, name);
  }

  info(cached = false): Promise<ConsumerInfo> {
    if (cached) {
      return Promise.resolve(this._info);
    }
    const { stream_name, name } = this._info;
    return this.api.info(stream_name, name)
      .then((ci) => {
        this._info = ci;
        return this._info;
      });
  }
}

export type OrderedConsumerOptions = {
  filterSubjects: string[] | string;
  deliver_policy: DeliverPolicy;
  opt_start_seq: number;
  opt_start_time: string;
  replay_policy: ReplayPolicy;
  inactive_threshold: number;
  headers_only: boolean;
};

export class OrderedPullConsumerImpl implements Consumer {
  api: ConsumerAPIImpl;
  consumerOpts: Partial<OrderedConsumerOptions>;
  consumer!: PullConsumerImpl;
  opts!: ConsumeOptions | FetchOptions;
  cursor: { stream_seq: number; deliver_seq: number };
  stream: string;
  namePrefix: string;
  serial: number;
  currentConsumer: ConsumerInfo | null;
  userCallback: ConsumerCallbackFn | null;
  iter: OrderedConsumerMessages | null;
  type: PullConsumerType;
  startSeq: number;

  constructor(
    api: ConsumerAPI,
    stream: string,
    opts: Partial<OrderedConsumerOptions> = {},
  ) {
    this.api = api as ConsumerAPIImpl;
    this.stream = stream;
    this.cursor = { stream_seq: 1, deliver_seq: 0 };
    this.namePrefix = nuid.next();
    this.serial = 0;
    this.currentConsumer = null;
    this.userCallback = null;
    this.iter = null;
    this.type = PullConsumerType.Unset;
    this.consumerOpts = opts;

    // to support a random start sequence we need to update the cursor
    this.startSeq = this.consumerOpts.opt_start_seq || 0;
    this.cursor.stream_seq = this.startSeq > 0 ? this.startSeq - 1 : 0;
  }

  getConsumerOpts(seq: number): ConsumerConfig {
    // change the serial - invalidating any callback not
    // matching the serial
    this.serial++;
    const name = `${this.namePrefix}_${this.serial}`;
    seq = seq === 0 ? 1 : seq;
    const config = {
      name,
      deliver_policy: DeliverPolicy.StartSequence,
      opt_start_seq: seq,
      ack_policy: AckPolicy.None,
      inactive_threshold: nanos(5 * 60 * 1000),
      num_replicas: 1,
    } as ConsumerConfig;

    if (this.consumerOpts.headers_only === true) {
      config.headers_only = true;
    }
    if (Array.isArray(this.consumerOpts.filterSubjects)) {
      config.filter_subjects = this.consumerOpts.filterSubjects;
    }
    if (typeof this.consumerOpts.filterSubjects === "string") {
      config.filter_subject = this.consumerOpts.filterSubjects;
    }
    // this is the initial request - tweak some options
    if (seq === this.startSeq + 1) {
      config.deliver_policy = this.consumerOpts.deliver_policy ||
        DeliverPolicy.StartSequence;
      if (
        this.consumerOpts.deliver_policy === DeliverPolicy.LastPerSubject ||
        this.consumerOpts.deliver_policy === DeliverPolicy.New ||
        this.consumerOpts.deliver_policy === DeliverPolicy.Last
      ) {
        delete config.opt_start_seq;
        config.deliver_policy = this.consumerOpts.deliver_policy;
      }
      // this requires a filter subject - we only set if they didn't
      // set anything, and to be pre-2.10 we set it as filter_subject
      if (config.deliver_policy === DeliverPolicy.LastPerSubject) {
        if (
          typeof config.filter_subjects === "undefined" &&
          typeof config.filter_subject === "undefined"
        ) {
          config.filter_subject = ">";
        }
      }
      if (this.consumerOpts.opt_start_time) {
        delete config.opt_start_seq;
        config.deliver_policy = DeliverPolicy.StartTime;
        config.opt_start_time = this.consumerOpts.opt_start_time;
      }
      if (this.consumerOpts.inactive_threshold) {
        config.inactive_threshold = nanos(this.consumerOpts.inactive_threshold);
      }
    }

    return config;
  }

  async resetConsumer(seq = 0): Promise<ConsumerInfo> {
    // try to delete the consumer
    if (this.consumer) {
      // FIXME: this needs to be a standard request option to retry
      while (true) {
        try {
          await this.delete();
          break;
        } catch (err) {
          if (err.message !== "TIMEOUT") {
            throw err;
          }
        }
      }
    }
    seq = seq === 0 ? 1 : seq;
    // reset the consumer sequence as JetStream will renumber from 1
    this.cursor.deliver_seq = 0;
    const config = this.getConsumerOpts(seq);
    let ci;
    // FIXME: replace with general jetstream retry logic
    while (true) {
      try {
        ci = await this.api.add(this.stream, config);
        break;
      } catch (err) {
        if (err.message !== "TIMEOUT") {
          throw err;
        }
      }
    }
    return ci;
  }

  internalHandler(serial: number) {
    // this handler will be noop if the consumer's serial changes
    return (m: JsMsg): void => {
      if (this.serial !== serial) {
        return;
      }
      const dseq = m.info.deliverySequence;
      if (dseq !== this.cursor.deliver_seq + 1) {
        this.reset(this.opts);
        return;
      }
      this.cursor.deliver_seq = dseq;
      this.cursor.stream_seq = m.info.streamSequence;

      if (this.userCallback) {
        this.userCallback(m);
      } else {
        this.iter?.push(m);
      }
    };
  }

  async reset(opts: ConsumeOptions | FetchOptions = {
    max_messages: 100,
    expires: 30_000,
  } as ConsumeMessages, fromFetch = false): Promise<ConsumerMessages> {
    this.currentConsumer = await this.resetConsumer(
      this.cursor.stream_seq + 1,
    );
    if (this.iter === null) {
      this.iter = new OrderedConsumerMessages();
    }
    this.consumer = new PullConsumerImpl(this.api, this.currentConsumer);
    const copts = opts as ConsumeOptions;
    copts.callback = this.internalHandler(this.serial);
    let msgs: ConsumerMessages | null = null;
    if (this.type === PullConsumerType.Fetch && fromFetch) {
      // we only repull if client initiates
      msgs = await this.consumer.fetch(opts);
    } else if (this.type === PullConsumerType.Consume) {
      msgs = await this.consumer.consume(opts);
    } else {
      return Promise.reject("reset called with unset consumer type");
    }
    this.iter.setSource(msgs as PullConsumerMessagesImpl);
    return this.iter;
  }

  consume(opts: ConsumeOptions = {
    max_messages: 100,
    expires: 30_000,
  } as ConsumeMessages): Promise<ConsumerMessages> {
    if (this.type === PullConsumerType.Fetch) {
      return Promise.reject(new Error("ordered consumer initialized as fetch"));
    }
    if (this.type === PullConsumerType.Consume) {
      return Promise.reject(
        new Error("ordered consumer doesn't support concurrent consume"),
      );
    }
    const { callback } = opts;
    if (callback) {
      this.userCallback = callback;
    }
    this.type = PullConsumerType.Consume;
    this.opts = opts;
    return this.reset(opts);
  }

  fetch(
    opts: FetchOptions = { max_messages: 100, expires: 30_000 },
  ): Promise<ConsumerMessages> {
    if (this.type === PullConsumerType.Consume) {
      return Promise.reject(
        new Error("ordered consumer already initialized as consume"),
      );
    }
    if (this.iter?.done === false) {
      return Promise.reject(
        new Error("ordered consumer doesn't support concurrent fetch"),
      );
    }

    //@ts-ignore: allow this for tests - api doesn't use it because
    // iterator close is the user signal that the pull is done.
    const { callback } = opts;
    if (callback) {
      this.userCallback = callback;
    }
    this.type = PullConsumerType.Fetch;
    this.opts = opts;
    this.iter = new OrderedConsumerMessages();
    return this.reset(opts, true);
  }

  async next(
    opts: NextOptions = { expires: 30_000 },
  ): Promise<JsMsg | null> {
    const d = deferred<JsMsg | null>();
    const copts = opts as ConsumeOptions;
    copts.max_messages = 1;
    copts.callback = (m) => {
      // we can clobber the callback, because they are not supported
      // except on consume, which will fail when we try to fetch
      this.userCallback = null;
      d.resolve(m);
    };
    const iter = await this.fetch(
      copts as FetchMessages,
    ) as OrderedConsumerMessages;
    iter.iterClosed
      .then(() => {
        d.resolve(null);
      })
      .catch((err) => {
        d.reject(err);
      });

    return d;
  }

  delete(): Promise<boolean> {
    if (!this.currentConsumer) {
      return Promise.resolve(false);
    }
    return this.api.delete(this.stream, this.currentConsumer.name)
      .then((tf) => {
        return Promise.resolve(tf);
      })
      .catch((err) => {
        return Promise.reject(err);
      })
      .finally(() => {
        this.currentConsumer = null;
      });
  }

  async info(cached?: boolean): Promise<ConsumerInfo> {
    if (this.currentConsumer == null) {
      this.currentConsumer = await this.resetConsumer(this.serial);
      return Promise.resolve(this.currentConsumer);
    }
    if (cached && this.currentConsumer) {
      return Promise.resolve(this.currentConsumer);
    }
    return this.api.info(this.stream, this.currentConsumer.name);
  }
}

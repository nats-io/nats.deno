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
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import type {
  ConsumeCallback,
  ConsumeOptions,
  ConsumerMessages,
  ConsumerStatus,
  FetchOptions,
  PullOptions,
  Subscription,
} from "./types.ts";
import {
  ConsumerCallbackFn,
  ConsumerDebugEvents,
  ConsumerEvents,
  Events,
  JsMsg,
  MsgHdrs,
  NatsError,
  PullConsumerOptions,
} from "./types.ts";
import { IdleHeartbeat } from "./idleheartbeat.ts";
import { createInbox } from "./protocol.ts";
import { isHeartbeatMsg, nanos } from "./jsutil.ts";
import { PullConsumerImpl } from "./consumer.ts";
import { MsgImpl } from "./msg.ts";
import { Timeout } from "./util.ts";
import { toJsMsg } from "./jsmsg.ts";

export class PullConsumerMessagesImpl extends QueuedIteratorImpl<JsMsg>
  implements ConsumerMessages {
  consumer: PullConsumerImpl;
  opts: Record<string, number>;
  sub: Subscription;
  monitor: IdleHeartbeat | null;
  pending: { msgs: number; bytes: number; requests: number };
  inbox: string;
  refilling: boolean;
  stack: string;
  pong!: Promise<void> | null;
  callback: ConsumerCallbackFn | null;
  timeout: Timeout<unknown> | null;
  cleanupHandler?: () => void;
  listeners: QueuedIterator<ConsumerStatus>[];

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

    if (idle_heartbeat) {
      this.monitor = new IdleHeartbeat(idle_heartbeat, (data): boolean => {
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
      for await (const s of c.api.nc.status()) {
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
    const msgsLeft = headers?.get("Nats-Pending-Messages");
    if (msgsLeft) {
      discard.msgsLeft = parseInt(msgsLeft);
    }
    const bytesLeft = headers?.get("Nats-Pending-Bytes");
    if (bytesLeft) {
      discard.bytesLeft = parseInt(bytesLeft);
    }
    // FIXME: batch complete header goes here...

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
    this.clearTimers();
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

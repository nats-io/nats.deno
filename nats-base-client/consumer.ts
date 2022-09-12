import {
  Consumer,
  ConsumerInfo,
  ConsumerReadOptions,
  ConsumerReadPullOptions,
  ExportedConsumer,
  JetStreamReader,
  JsMsg,
  NatsConnection,
  PullOptions,
} from "./types.ts";
import { checkJsError, isHeartbeatMsg, Js409Errors, nanos } from "./jsutil.ts";
import { toJsMsg } from "./jsmsg.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import {
  consumerOpts,
  createInbox,
  deferred,
  ErrorCode,
  JSONCodec,
  NatsError,
} from "./mod.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { isNatsError } from "./error.ts";
import { NatsConnectionImpl } from "./nats.ts";
import { hideNonTerminalJsErrors } from "./jsclient.ts";
import { IdleHeartbeat } from "./idleheartbeat.ts";

const jc = JSONCodec();

type BufferedPullOptions = PullOptions & { low?: number };

export class ExportedConsumerImpl implements ExportedConsumer {
  nc: NatsConnection;
  subject: string;

  constructor(nc: NatsConnection, subject: string) {
    this.nc = nc;
    this.subject = subject;
  }
  async next(opts: Partial<{ expires: number }> = {}): Promise<JsMsg> {
    const to = opts.expires ?? 0;
    const expires = nanos(to);
    const timeout = to > 2000 ? to + 2000 : 2000;

    const pullOpts: Partial<PullOptions> = {
      batch: 1,
      no_wait: expires === 0,
      expires,
    };

    const r = await this.nc.request(this.subject, jc.encode(pullOpts), {
      noMux: true,
      timeout,
    });

    const err = checkJsError(r);
    if (err) {
      throw err;
    }

    return toJsMsg(r);
  }

  read(
    opts: Partial<ConsumerReadOptions> = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    const limits = opts.inflight_limit || {};
    const batch = limits.batch || 1000;

    const qi = new QueuedIteratorImpl<JsMsg>();
    const inbox = createInbox();
    const to = 5000;
    // FIXME: there's some issue, the server is not sending
    //   more than one message with greater batches
    const payload = jc.encode({
      expires: nanos(to),
      batch,
    });

    let expected = 0;
    let last = Date.now();

    // this timer will check if last message or error was
    // received within the guard timer - if not, then
    // this means that we have a pull that expired, but we
    // got no notification or messages
    const guard = () => {
      if (Date.now() - last > to) {
        expected = 0;
        fn();
      }
    };

    let timer = 0;

    // clean the timer up when the iterator closes
    const cleanup = () => {
      if (timer > 0) {
        clearTimeout(timer);
      }
    };

    qi.iterClosed
      .then(() => {
        cleanup();
      })
      .catch(() => {
        cleanup();
      });

    // this is the pull fn - we initialize last, make a request
    // and initialize the guard to do a check slightly after
    // the pull is supposed to expire
    const fn = () => {
      if (timer > 0) {
        clearTimeout(timer);
      }
      last = Date.now();
      this.nc.publish(this.subject, payload, { reply: inbox });
      expected = batch;

      timer = setTimeout(guard, to + 1000);
    };

    try {
      const sub = this.nc.subscribe(inbox, {
        callback: (err, msg) => {
          last = Date.now();
          if (err) {
            qi.stop(err);
          }
          err = checkJsError(msg);
          if (err) {
            switch (err.code) {
              case ErrorCode.JetStream404NoMessages:
              case ErrorCode.JetStream408RequestTimeout:
                // poll again
                fn();
                return;
              default:
                qi.stop(err);
                sub.unsubscribe();
                return;
            }
          }
          if (isHeartbeatMsg(msg)) {
            return;
          }

          qi.push(toJsMsg(msg));
          // if we are here, we have a data message
          --expected;
          if (expected <= 0) {
            // we got what we asked, so ask for more
            fn();
          }
        },
      });
      sub.closed.then(() => {
        console.log("sub closed");
      });
    } catch (err) {
      qi.stop(err);
    }
    fn();
    return Promise.resolve(qi);
  }
}

export class ConsumerImpl implements Consumer {
  consumerAPI: ConsumerAPIImpl;
  ci: ConsumerInfo;
  streamName: string;
  consumerName: string;
  prefix: string;

  constructor(api: ConsumerAPIImpl, info: ConsumerInfo) {
    this.consumerAPI = api;
    this.ci = info;
    this.streamName = info.stream_name;
    this.consumerName = info.name;
    this.prefix = api.opts.apiPrefix!;
  }

  async next(opts: Partial<{ expires: number }> = {}): Promise<JsMsg> {
    if (typeof this.ci.config.deliver_subject === "string") {
      return Promise.reject(
        new Error("consumer configuration is not a pull consumer"),
      );
    }
    let timeout = this.consumerAPI.timeout;
    let expires = opts.expires ? opts.expires : 0;
    if (expires > timeout) {
      timeout = expires;
    }
    expires = expires < 0 ? 0 : nanos(expires);

    const pullOpts: Partial<PullOptions> = {
      batch: 1,
      no_wait: expires === 0,
      expires,
    };

    const { streamName, consumerName, prefix } = this;

    const msg = await this.consumerAPI.nc.request(
      `${prefix}.CONSUMER.MSG.NEXT.${streamName}.${consumerName}`,
      this.consumerAPI.jc.encode(pullOpts),
      { noMux: true, timeout },
    );
    const err = checkJsError(msg);
    if (err) {
      throw (err);
    }
    return toJsMsg(msg);
  }

  info(): Promise<ConsumerInfo> {
    return this.consumerAPI.info(this.ci.stream_name, this.ci.name);
  }

  async _handleWithIterator(
    opts: Partial<ConsumerReadOptions> = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    const qi = new QueuedIteratorImpl<JsMsg>();
    try {
      if (typeof this.ci.config.deliver_subject === "string") {
        await this.setupPush(qi);
      } else {
        let batch = opts.inflight_limit?.batch || 0;
        const max_bytes = opts.inflight_limit?.max_bytes || 0;
        if (max_bytes) {
          batch = 0;
        }
        const idle_heartbeat = opts.inflight_limit?.idle_heartbeat || 0;
        const expires = opts?.inflight_limit?.expires || 5000;
        const low = opts?.inflight_limit?.low || 0;

        this.pull(qi, { batch, max_bytes, idle_heartbeat, expires, low });
      }
    } catch (err) {
      qi.stop(err);
    }
    return qi;
  }

  _handleWithCallback(
    opts: Partial<ConsumerReadOptions> = {},
  ): Promise<JetStreamReader> {
    if (typeof opts.callback !== "function") {
      return Promise.reject("`callback` is required");
    }
    if (this.ci.config.deliver_subject) {
      throw new Error("push cb not implemented");
    } else {
      return Promise.resolve(
        this.pullCallback(opts.callback, opts?.inflight_limit),
      );
    }
  }

  read(
    opts: Partial<ConsumerReadOptions> = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    return typeof opts.callback !== "function"
      ? this._handleWithIterator(opts)
      : this._handleWithCallback(opts);
  }

  _processPullOptions(inbox: string, opts: Partial<ConsumerReadPullOptions>): {
    monitor: IdleHeartbeat | null;
    pullFn: (Uint8Array) => void;
    fullOptions: Uint8Array;
    partialOptions: Uint8Array;
    missed: Promise<void> | null;
  } {
    let { batch, max_bytes, idle_heartbeat, expires, low } = opts;
    expires = nanos(expires || 5000);

    const max = (max_bytes ? max_bytes : batch) || 0;
    low = !low ? Math.floor(max * .25) || 1 : low;

    const full = max_bytes
      ? {
        max_bytes,
        expires,
      }
      : { batch, expires } as Partial<PullOptions>;
    if (idle_heartbeat) {
      full.idle_heartbeat = nanos(idle_heartbeat);
    }
    const fullOptions = jc.encode(full);

    const partial = max_bytes
      ? { max_bytes: low, expires }
      : { batch: low, expires } as Partial<PullOptions>;
    if (idle_heartbeat) {
      partial.idle_heartbeat = nanos(idle_heartbeat);
    }
    const partialOptions = jc.encode(partial);

    const { streamName, consumerName, prefix } = this;
    const pullSubject =
      `${prefix}.CONSUMER.MSG.NEXT.${streamName}.${consumerName}`;

    let monitor: IdleHeartbeat | null = null;
    let missed: Promise<void> | null = null;
    if (idle_heartbeat) {
      missed = deferred();
      monitor = new IdleHeartbeat(idle_heartbeat, (v: number): boolean => {
        missed.reject(
          new NatsError(
            `${Js409Errors.IdleHeartbeatMissed}: ${v}`,
            ErrorCode.JetStreamIdleHeartBeat,
          ),
        );
        return true;
      });
    }

    function pull(payload: Uint8Array) {
      nc.publish(pullSubject, payload, { reply: sub.getSubject() });
    }

    return {
      monitor: monitor,
      pullFn: pull,
      fullOptions,
      partialOptions,
      missed,
    };
  }

  pullCallback(
    callback: (m: JsMsg) => void,
    opts: Partial<BufferedPullOptions> = {},
  ): JetStreamReader {
    const nc = this.consumerAPI.nc as NatsConnectionImpl;
    let { batch, max_bytes, idle_heartbeat, expires, low } = opts;
    expires = nanos(expires || 5000);

    const max = (max_bytes ? max_bytes : batch) || 0;
    low = !low ? Math.floor(max * .25) || 1 : low;
    let seen = 0;

    const full = max_bytes
      ? {
        max_bytes,
        expires,
      }
      : { batch, expires } as Partial<PullOptions>;
    if (idle_heartbeat) {
      full.idle_heartbeat = nanos(idle_heartbeat);
    }
    const fullOptions = jc.encode(full);

    const partial = max_bytes
      ? { max_bytes: low, expires }
      : { batch: low, expires } as Partial<PullOptions>;
    if (idle_heartbeat) {
      partial.idle_heartbeat = nanos(idle_heartbeat);
    }
    const partialOpts = jc.encode(partial);

    const { streamName, consumerName, prefix } = this;
    const pullSubject =
      `${prefix}.CONSUMER.MSG.NEXT.${streamName}.${consumerName}`;

    const _closed = deferred<null | NatsError>();

    const reader = {
      stop: (): Promise<null | NatsError> => {
        const done = deferred<null | NatsError>();
        sub.drain()
          .then(() => {
            done.resolve(null);
            _closed.resolve(null);
          })
          .catch((err) => {
            done.resolve(err);
            _closed.reject(err);
          });
        return done;
      },
      get closed(): Promise<null | NatsError> {
        return _closed;
      },
    };

    const sub = this.consumerAPI.nc.subscribe(
      createInbox(nc.options.inboxPrefix),
      {
        callback: (err, msg) => {
          if (err === null) {
            err = checkJsError(msg);
          }
          if (err !== null) {
            if (err.code === ErrorCode.JetStream408RequestTimeout) {
              pull(fullOptions);
              return;
            }
            if (isNatsError(err)) {
              _closed.reject(
                hideNonTerminalJsErrors(err) === null ? undefined : err,
              );
            } else {
              _closed.reject(err);
            }
            sub.unsubscribe();
          } else {
            callback(toJsMsg(msg));
            seen++;
            if (seen === low) {
              pull(partialOpts);
            }
          }
        },
      },
    );

    function pull(payload: Uint8Array) {
      seen = 0;
      nc.publish(pullSubject, payload, { reply: sub.getSubject() });
    }

    pull(fullOptions);
    return reader;
  }

  pull(
    qi: QueuedIteratorImpl<JsMsg> | null,
    opts: Partial<ConsumerReadPullOptions>,
  ) {
    const nc = this.consumerAPI.nc as NatsConnectionImpl;
    const inbox = createInbox(nc.options.inboxPrefix);
    let { batch, max_bytes, idle_heartbeat, expires, low } = opts;
    expires = nanos(expires || 5000);

    const max = (max_bytes ? max_bytes : batch) || 0;
    low = !low ? Math.floor(max * .25) || 1 : low;

    let seen = 0;

    const full = max_bytes
      ? {
        max_bytes,
        expires,
      }
      : { batch, expires } as Partial<PullOptions>;
    if (idle_heartbeat) {
      full.idle_heartbeat = nanos(idle_heartbeat);
    }
    const fullOptions = jc.encode(full);

    const partial = max_bytes
      ? { max_bytes: low, expires }
      : { batch: low, expires } as Partial<PullOptions>;
    if (idle_heartbeat) {
      partial.idle_heartbeat = nanos(idle_heartbeat);
    }
    const partialOpts = jc.encode(partial);

    const { streamName, consumerName, prefix } = this;
    const pullSubject =
      `${prefix}.CONSUMER.MSG.NEXT.${streamName}.${consumerName}`;

    const sub = this.consumerAPI.nc.subscribe(
      inbox,
      {
        callback: (err, msg) => {
          if (err === null) {
            err = checkJsError(msg);
          }
          if (err !== null) {
            if (err.code === ErrorCode.JetStream408RequestTimeout) {
              // pull right now
              pull(fullOptions);
              return;
            }
            if (isNatsError(err)) {
              qi.stop(hideNonTerminalJsErrors(err) === null ? undefined : err);
            } else {
              qi.stop(err);
            }
            sub.unsubscribe();
          } else {
            monitor?.work();
            if (isHeartbeatMsg(msg)) {
              return;
            }
            qi.received++;
            if (max_bytes) {
              seen += msg.data.length;
            } else {
              seen++;
            }
            qi.push(toJsMsg(msg));
            if (seen === low) {
              //@ts-ignore: pull when the user has processed the low message
              qi.push(() => {
                pull(partialOpts);
              });
            }
          }
        },
      },
    );

    let monitor: IdleHeartbeat | null = null;
    if (idle_heartbeat) {
      monitor = new IdleHeartbeat(idle_heartbeat, (v: number): boolean => {
        //@ts-ignore: pushing a fn
        qi.push(() => {
          // this will terminate the iterator
          qi.err = new NatsError(
            `${Js409Errors.IdleHeartbeatMissed}: ${v}`,
            ErrorCode.JetStreamIdleHeartBeat,
          );
        });
        return true;
      });
    }

    function pull(payload: Uint8Array) {
      seen = 0;
      nc.publish(pullSubject, payload, { reply: sub.getSubject() });
    }

    qi.iterClosed.then(() => {
      monitor?.cancel();
      sub.unsubscribe();
    });

    pull(fullOptions);
  }

  async setupPush(
    qi: QueuedIteratorImpl<JsMsg>,
  ) {
    const js = this.consumerAPI.nc.jetstream(this.consumerAPI.opts);
    const co = consumerOpts();
    co.bind(this.ci.stream_name, this.ci.name);
    co.manualAck();

    co.callback((err, m) => {
      if (err) {
        //@ts-ignore: stop
        qi.push(() => {
          qi.stop(err);
        });
      }
      if (m) {
        qi.push(m);
      }
    });
    const sub = await js.subscribe(">", co);
    sub.closed.then(() => {
      //@ts-ignore: stop
      qi.push(() => {
        qi.stop();
      });
    });
    qi.iterClosed.then(() => {
      sub.unsubscribe();
    });
  }
}

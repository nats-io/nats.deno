import {
  Consumer,
  ConsumerInfo,
  ExportedConsumer,
  JetStreamReader,
  NatsConnection,
  PullOptions,
} from "./types.ts";
import { JsMsg } from "https://raw.githubusercontent.com/nats-io/nats.deno/main/nats-base-client/types.ts";
import { checkJsError, isHeartbeatMsg, nanos } from "./jsutil.ts";
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

const jc = JSONCodec();

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
    opts: Partial<
      {
        inflight_limit: Partial<{ bytes: number; messages: number }>;
        callback: (m: JsMsg) => void;
      }
    > = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    const limits = opts.inflight_limit || {};
    const batch = limits.messages || 1000;

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
    _opts: Partial<
      {
        inflight_limit: Partial<{
          bytes: number;
          messages: number;
        }>;
        callback: (m: JsMsg) => void;
      }
    > = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    const qi = new QueuedIteratorImpl<JsMsg>();

    try {
      if (typeof this.ci.config.deliver_subject === "string") {
        await this.setupPush(qi);
      } else {
        const batch = _opts.inflight_limit?.messages ?? 1;
        this.pull(qi, batch);
      }
    } catch (err) {
      qi.stop(err);
    }
    return qi;
  }

  _handleWithCallback(
    _opts: Partial<
      {
        inflight_limit: Partial<{
          bytes: number;
          messages: number;
        }>;
        callback: (m: JsMsg) => void;
      }
    > = {},
  ): Promise<JetStreamReader> {
    if (typeof _opts.callback !== "function") {
      return Promise.reject("`callback` is required");
    }
    if (this.ci.config.deliver_subject) {
      throw new Error("push cb not implemented");
    } else {
      const batch = _opts.inflight_limit?.messages ?? 1;
      return Promise.resolve(this.pullCallback(_opts.callback, batch));
    }
  }

  read(
    _opts: Partial<
      {
        inflight_limit: Partial<{
          bytes: number;
          messages: number;
        }>;
        callback: (m: JsMsg) => void;
      }
    > = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    return typeof _opts.callback !== "function"
      ? this._handleWithIterator(_opts)
      : this._handleWithCallback(_opts);
  }

  #pullOptions(batch = 1, expires = 5000): Uint8Array {
    const jc = JSONCodec();
    return jc.encode({ batch, expires: nanos(expires) });
  }

  pullCallback(callback: (m: JsMsg) => void, batch = 1): JetStreamReader {
    const nc = this.consumerAPI.nc as NatsConnectionImpl;
    const low = Math.floor(batch * .25) ?? 1;
    let seen = 0;

    const { streamName, consumerName, prefix } = this;
    const pullSubject =
      `${prefix}.CONSUMER.MSG.NEXT.${streamName}.${consumerName}`;

    const fullPullOpts = this.#pullOptions(batch, 5000);
    const partialPullOpts = this.#pullOptions(low, 5000);
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
              pull(fullPullOpts);
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
              pull(partialPullOpts);
            }
          }
        },
      },
    );

    function pull(payload: Uint8Array) {
      seen = 0;
      nc.publish(pullSubject, payload, { reply: sub.getSubject() });
    }

    pull(fullPullOpts);
    return reader;
  }

  pull(qi: QueuedIteratorImpl<JsMsg>, batch = 1) {
    const nc = this.consumerAPI.nc as NatsConnectionImpl;

    const low = Math.floor(batch * .25) || 1;
    let seen = 0;

    const { streamName, consumerName, prefix } = this;
    const pullSubject =
      `${prefix}.CONSUMER.MSG.NEXT.${streamName}.${consumerName}`;

    const fullPullOpts = this.#pullOptions(batch, 5000);
    const partialPullOpts = this.#pullOptions(low, 5000);

    const sub = this.consumerAPI.nc.subscribe(
      createInbox(nc.options.inboxPrefix),
      {
        callback: (err, msg) => {
          if (err === null) {
            err = checkJsError(msg);
          }
          if (err !== null) {
            if (err.code === ErrorCode.JetStream408RequestTimeout) {
              // pull right now
              pull(fullPullOpts);
              return;
            }
            if (isNatsError(err)) {
              qi.stop(hideNonTerminalJsErrors(err) === null ? undefined : err);
            } else {
              qi.stop(err);
            }
            sub.unsubscribe();
          } else {
            qi.received++;
            seen++;
            qi.push(toJsMsg(msg));
            if (seen === low) {
              //@ts-ignore: pull when the user has processed the low message
              qi.push(() => {
                pull(partialPullOpts);
              });
            }
          }
        },
      },
    );

    function pull(payload: Uint8Array) {
      seen = 0;
      nc.publish(pullSubject, payload, { reply: sub.getSubject() });
    }

    qi.iterClosed.then(() => {
      sub.unsubscribe();
    });

    pull(fullPullOpts);
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

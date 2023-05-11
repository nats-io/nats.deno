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
import {
  AckPolicy,
  ConsumeMessages,
  ConsumeOptions,
  Consumer,
  ConsumerAPI,
  ConsumerCallbackFn,
  ConsumerConfig,
  ConsumerEvents,
  ConsumerInfo,
  ConsumerMessages,
  DeliverPolicy,
  FetchMessages,
  FetchOptions,
  JsMsg,
  NextOptions,
  ReplayPolicy,
} from "./types.ts";
import { deferred, timeout } from "./util.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import {
  OrderedConsumerMessages,
  PullConsumerMessagesImpl,
} from "./consumermessages.ts";
import { nuid } from "./nuid.ts";
import { nanos } from "./jsutil.ts";

enum PullConsumerType {
  Unset = -1,
  Consume,
  Fetch,
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
  filterSubjects: string[];
  deliver_policy: DeliverPolicy;
  opt_start_seq: number;
  opt_start_time: string;
  replay_policy: ReplayPolicy;
  inactive_threshold: number;
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

    if (Array.isArray(this.consumerOpts.filterSubjects)) {
      config.filter_subjects = this.consumerOpts.filterSubjects;
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
      if (config.deliver_policy === DeliverPolicy.LastPerSubject) {
        config.filter_subjects = config.filter_subjects || [">"];
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

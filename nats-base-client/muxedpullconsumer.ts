import {
  Consumer,
  ConsumerAPI,
  ConsumerInfo,
  ConsumerReadOptions,
  JetStreamReader,
  JsMsg,
  Msg,
  NatsError,
  PullOptions,
} from "./types.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { JsMuxedInbox } from "./jsmuxed_inbox.ts";
import { deferred, toJsMsg } from "./mod.ts";

enum HandlerType {
  UnSet,
  Cb = 1,
  Iter = 2,
}

export class MuxedPullConsumer implements Consumer {
  api: ConsumerAPIImpl;
  ci: ConsumerInfo;
  prefix: string;
  maxConcurrent: number;
  minbox: JsMuxedInbox<PullOptions>;
  pulls: PullOptions[];
  qi!: QueuedIteratorImpl<JsMsg>;
  userCb!: (m: JsMsg) => void;
  handlerType: HandlerType;

  constructor(api: ConsumerAPI, ci: ConsumerInfo) {
    this.api = api as ConsumerAPIImpl;
    this.ci = ci;
    this.prefix = this.api.prefix;
    this.maxConcurrent = 4;
    this.pulls = [];
    this.minbox = new JsMuxedInbox<PullOptions>(this.api.nc, this.dispatcher);
    this.handlerType = HandlerType.UnSet;
  }

  dispatcher(
    cookie: PullOptions | null,
    err: NatsError | null,
    msg: Msg | null,
  ): void {
    if (cookie) {
      // this is a control message
      console.log(`handle control message: ${cookie}`);
      if (err) {
        console.log(`handle control message error: ${err.message}`);
      }
      return;
    } else if (err) {
      console.log(`got an error: ${err.message}`);
      return;
    }
    const jsMsg = toJsMsg(msg);
    this.userCb(jsMsg);
  }

  info(): Promise<ConsumerInfo> {
    return this.api.info(this.ci.stream_name, this.ci.name);
  }

  next(opts?: Partial<{ expires: number }>): Promise<JsMsg> {
    return Promise.reject(new Error("not implemented"));
  }

  read(
    opts: Partial<ConsumerReadOptions> = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    // check that we have not been materialized
    switch (this.handlerType) {
      case HandlerType.UnSet:
        // set the type
        this.handlerType = typeof opts.callback === "function"
          ? HandlerType.Cb
          : HandlerType.Iter;
        break;
      case HandlerType.Cb:
        return Promise.reject(
          new Error("Consumer already materialized as a polled consumer"),
        );
      case HandlerType.Iter:
        return Promise.reject(new Error("Consumer is already materialized"));
    }
    const { callback } = opts;
    if (callback) {
      this.userCb = callback;
      const done = deferred<null | NatsError>();
      const self = this;
      const reader = {
        stop: (): Promise<null | NatsError> => {
          self.minbox.drain()
            .then(() => {
              done.resolve();
            })
            .catch((err: Error) => {
              done.resolve(err);
            });
          return done;
        },
        get closed(): Promise<null | NatsError> {
          return done;
        },
      } as JetStreamReader;
      return Promise.resolve(reader);
    } else {
      this.qi = new QueuedIteratorImpl<JsMsg>();
      this.userCb = (msg) => {
        this.qi.push(msg);
      };
      return Promise.resolve(this.qi);
    }
  }
}

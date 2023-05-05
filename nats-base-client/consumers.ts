/*
 * Copyright 2023 The NATS Authors
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

import { Consumer, ConsumerAPI, Consumers } from "./types.ts";
import {
  OrderedConsumerOptions,
  OrderedPullConsumerImpl,
  PullConsumerImpl,
} from "./consumer.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import { StreamAPIImpl } from "./jsmstream_api.ts";
import { Feature } from "./semver.ts";

export class ConsumersImpl implements Consumers {
  api: ConsumerAPI;
  notified: boolean;
  constructor(api: ConsumerAPI) {
    this.api = api;
    this.notified = false;
  }

  checkVersion(ordered = false): Promise<void> {
    if (!this.notified) {
      this.notified = true;
      console.log(
        `\u001B[33m >> consumers framework is beta functionality \u001B[0m`,
      );
    }
    const fv = (this.api as ConsumerAPIImpl).nc.features.get(
      Feature.JS_SIMPLIFICATION,
    );
    if (!fv.ok) {
      return Promise.reject(
        new Error(
          `consumers framework is only supported on servers ${fv.min} or better`,
        ),
      );
    }
    if (ordered) {
      const fv = (this.api as ConsumerAPIImpl).nc.features.get(
        Feature.JS_CONSUMER_FILTER_SUBJECTS,
      );
      if (!fv.ok) {
        return Promise.reject(
          new Error(
            `consumers framework's ordered consumer is only supported on servers ${fv.min} or better`,
          ),
        );
      }
    }
    return Promise.resolve();
  }

  async get(
    stream: string,
    name: string | Partial<OrderedConsumerOptions> = {},
  ): Promise<Consumer> {
    if (typeof name === "object") {
      return this.ordered(stream, name);
    }
    // check we have support for pending msgs and header notifications
    await this.checkVersion();

    return this.api.info(stream, name)
      .then((ci) => {
        if (ci.config.deliver_subject !== undefined) {
          return Promise.reject(new Error("push consumer not supported"));
        }
        return new PullConsumerImpl(this.api, ci);
      })
      .catch((err) => {
        return Promise.reject(err);
      });
  }

  async ordered(
    stream: string,
    opts?: Partial<OrderedConsumerOptions>,
  ): Promise<Consumer> {
    await this.checkVersion(true);

    const impl = this.api as ConsumerAPIImpl;
    const sapi = new StreamAPIImpl(impl.nc, impl.opts);
    return sapi.info(stream)
      .then((_si) => {
        return Promise.resolve(
          new OrderedPullConsumerImpl(this.api, stream, opts),
        );
      })
      .catch((err) => {
        return Promise.reject(err);
      });
  }
}

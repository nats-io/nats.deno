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

import { BaseApiClient } from "./base_api.ts";
import { Lister, ListerFieldFilter, ListerImpl } from "./lister.ts";
import { NatsConnection } from "../internal_mod.ts";
import {
  ConsumerConfig,
  ConsumerInfo,
  ConsumerListResponse,
  CreateConsumerRequest,
  SuccessResponse,
} from "./types.ts";
import { validateDurableName, validateStreamName } from "./util.ts";
import { JetStreamOptions } from "./jetstream.ts";

export interface ConsumerAPI {
  info(stream: string, consumer: string): Promise<ConsumerInfo>;

  add(stream: string, cfg: Partial<ConsumerConfig>): Promise<ConsumerInfo>;

  delete(stream: string, consumer: string): Promise<boolean>;

  list(stream: string): Lister<ConsumerInfo>;
}

// export interface PushConsumerConfig extends ConsumerConfig {
//   "deliver_subject": string;
// }
//
// export interface PushConsumer extends Consumer {
//   config: PushConsumerConfig;
// }

export class ConsumerAPIImpl extends BaseApiClient implements ConsumerAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(
    stream: string,
    cfg: ConsumerConfig,
  ): Promise<ConsumerInfo> {
    validateStreamName(stream);

    const cr = {} as CreateConsumerRequest;
    cr.config = cfg;
    cr.stream_name = stream;

    if (cfg.durable_name) {
      validateDurableName(cfg.durable_name);
    }

    const subj = cfg.durable_name
      ? `${this.prefix}.CONSUMER.DURABLE.CREATE.${stream}.${cfg.durable_name}`
      : `${this.prefix}.CONSUMER.CREATE.${stream}`;
    const r = await this._request(subj, cr);
    return r as ConsumerInfo;
  }

  async info(stream: string, name: string): Promise<ConsumerInfo> {
    validateStreamName(stream);
    validateDurableName(name);
    const r = await this._request(
      `${this.prefix}.CONSUMER.INFO.${stream}.${name}`,
    );
    return r as ConsumerInfo;
  }

  async delete(stream: string, name: string): Promise<boolean> {
    validateStreamName(stream);
    validateDurableName(name);
    const r = await this._request(
      `${this.prefix}.CONSUMER.DELETE.${stream}.${name}`,
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  list(stream: string): Lister<ConsumerInfo> {
    validateStreamName(stream);
    const filter: ListerFieldFilter<ConsumerInfo> = (
      v: unknown,
    ): ConsumerInfo[] => {
      const clr = v as ConsumerListResponse;
      return clr.consumers;
    };
    const subj = `${this.prefix}.CONSUMER.LIST.${stream}`;
    return new ListerImpl<ConsumerInfo>(subj, filter, this);
  }
}

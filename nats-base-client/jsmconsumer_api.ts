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
  Consumer,
  ConsumerAPI,
  ConsumerConfig,
  ConsumerInfo,
  ConsumerListResponse,
  ConsumerUpdateConfig,
  CreateConsumerRequest,
  ExportedConsumer,
  JetStreamOptions,
  Lister,
  NatsConnection,
  SuccessResponse,
} from "./types.ts";
import { BaseApiClient } from "./jsbaseclient_api.ts";
import { ListerFieldFilter, ListerImpl } from "./jslister.ts";
import {
  validateDurableName,
  validateStreamName,
  validName,
} from "./jsutil.ts";
import { NatsConnectionImpl } from "./nats.ts";
import { Feature } from "./semver.ts";
import { ConsumerImpl, ExportedConsumerImpl } from "./consumer.ts";

export class ConsumerAPIImpl extends BaseApiClient implements ConsumerAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(
    stream: string,
    cfg: ConsumerConfig,
  ): Promise<ConsumerInfo> {
    validateStreamName(stream);

    if (cfg.deliver_group && cfg.flow_control) {
      throw new Error(
        "jetstream flow control is not supported with queue groups",
      );
    }
    if (cfg.deliver_group && cfg.idle_heartbeat) {
      throw new Error(
        "jetstream idle heartbeat is not supported with queue groups",
      );
    }

    const cr = {} as CreateConsumerRequest;
    cr.config = cfg;
    cr.stream_name = stream;

    if (cr.config.durable_name) {
      validateDurableName(cr.config.durable_name);
    }

    const nci = this.nc as NatsConnectionImpl;
    const { min, ok: newAPI } = nci.features.get(
      Feature.JS_NEW_CONSUMER_CREATE_API,
    );

    const name = cfg.name === "" ? undefined : cfg.name;
    if (name && !newAPI) {
      throw new Error(`consumer 'name' requires server ${min}`);
    }
    if (name) {
      const m = validName(name);
      if (m.length) {
        throw new Error(`consumer 'name' ${m}`);
      }
    }

    let subj;
    let consumerName = "";
    if (newAPI) {
      consumerName = cfg.name ?? cfg.durable_name ?? "";
    }
    if (consumerName !== "") {
      let fs = cfg.filter_subject ?? undefined;
      if (fs === ">") {
        fs = undefined;
      }
      subj = fs !== undefined
        ? `${this.prefix}.CONSUMER.CREATE.${stream}.${consumerName}.${fs}`
        : `${this.prefix}.CONSUMER.CREATE.${stream}.${consumerName}`;
    } else {
      subj = cfg.durable_name
        ? `${this.prefix}.CONSUMER.DURABLE.CREATE.${stream}.${cfg.durable_name}`
        : `${this.prefix}.CONSUMER.CREATE.${stream}`;
    }
    const r = await this._request(subj, cr);
    return r as ConsumerInfo;
  }

  async update(
    stream: string,
    durable: string,
    cfg: ConsumerUpdateConfig,
  ): Promise<ConsumerInfo> {
    const ci = await this.info(stream, durable);
    const changable = cfg as ConsumerConfig;
    return this.add(stream, Object.assign(ci.config, changable));
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

  get(stream: string, name: string): Promise<Consumer> {
    return this.info(stream, name)
      .then((ci) => {
        return Promise.resolve(new ConsumerImpl(this, ci));
      });
  }

  exportedConsumer(subject: string): ExportedConsumer {
    return new ExportedConsumerImpl(this.nc, subject);
  }
}

/*
 * Copyright 2021-2023 The NATS Authors
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
import { BaseApiClient } from "./jsbaseclient_api.ts";
import { Lister, ListerFieldFilter, ListerImpl } from "./jslister.ts";
import {
  minValidation,
  validateDurableName,
  validateStreamName,
} from "./jsutil.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";
import { Feature } from "../nats-base-client/semver.ts";
import { JetStreamOptions, NatsConnection } from "../nats-base-client/core.ts";
import {
  ConsumerApiAction,
  ConsumerConfig,
  ConsumerInfo,
  ConsumerListResponse,
  ConsumerUpdateConfig,
  CreateConsumerRequest,
  SuccessResponse,
} from "./jsapi_types.ts";

export interface ConsumerAPI {
  /**
   * Returns the ConsumerInfo for the specified consumer in the specified stream.
   * @param stream
   * @param consumer
   */
  info(stream: string, consumer: string): Promise<ConsumerInfo>;

  /**
   * Adds a new consumer to the specified stream with the specified consumer options.
   * @param stream
   * @param cfg
   */
  add(stream: string, cfg: Partial<ConsumerConfig>): Promise<ConsumerInfo>;

  /**
   * Updates the consumer configuration for the specified consumer on the specified
   * stream that has the specified durable name.
   * @param stream
   * @param durable
   * @param cfg
   */
  update(
    stream: string,
    durable: string,
    cfg: Partial<ConsumerUpdateConfig>,
  ): Promise<ConsumerInfo>;

  /**
   * Deletes the specified consumer name/durable from the specified stream.
   * @param stream
   * @param consumer
   */
  delete(stream: string, consumer: string): Promise<boolean>;

  /**
   * Lists all the consumers on the specfied streams
   * @param stream
   */
  list(stream: string): Lister<ConsumerInfo>;
}

export class ConsumerAPIImpl extends BaseApiClient implements ConsumerAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(
    stream: string,
    cfg: ConsumerConfig,
    action = ConsumerApiAction.Create,
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
    cr.action = action;

    if (cr.config.durable_name) {
      validateDurableName(cr.config.durable_name);
    }

    const nci = this.nc as NatsConnectionImpl;
    let { min, ok: newAPI } = nci.features.get(
      Feature.JS_NEW_CONSUMER_CREATE_API,
    );

    const name = cfg.name === "" ? undefined : cfg.name;
    if (name && !newAPI) {
      throw new Error(`consumer 'name' requires server ${min}`);
    }
    if (name) {
      try {
        minValidation("name", name);
      } catch (err) {
        // if we have a cannot contain the message, massage a bit
        const m = err.message;
        const idx = m.indexOf("cannot contain");
        if (idx !== -1) {
          throw new Error(`consumer 'name' ${m.substring(idx)}`);
        }
        throw err;
      }
    }

    let subj;
    let consumerName = "";
    // new api doesn't support multiple filter subjects
    // this delayed until here because the consumer in an update could have
    // been created with the new API, and have a `name`
    if (Array.isArray(cfg.filter_subjects)) {
      const { min, ok } = nci.features.get(Feature.JS_MULTIPLE_CONSUMER_FILTER);
      if (!ok) {
        throw new Error(`consumer 'filter_subjects' requires server ${min}`);
      }
      newAPI = false;
    }
    if (cfg.metadata) {
      const { min, ok } = nci.features.get(Feature.JS_STREAM_CONSUMER_METADATA);
      if (!ok) {
        throw new Error(`consumer 'metadata' requires server ${min}`);
      }
    }
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
    return this.add(
      stream,
      Object.assign(ci.config, changable),
      ConsumerApiAction.Update,
    );
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

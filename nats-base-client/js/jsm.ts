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

import type { NatsConnection } from "../internal_mod.ts";
import { QueuedIterator } from "../internal_mod.ts";
import { BaseApiClient } from "./base_api.ts";
import { StreamAPI, StreamAPIImpl } from "./stream_api.ts";
import { ConsumerAPI, ConsumerAPIImpl } from "./consumer_api.ts";
import {
  AccountInfoResponse,
  Advisory,
  AdvisoryKind,
  ApiResponse,
  JetStreamAccountStats,
} from "./types.ts";
import { JetStreamOptions } from "./jetstream.ts";

export interface JSM {
  consumers: ConsumerAPI;
  streams: StreamAPI;

  getAccountInfo(): Promise<JetStreamAccountStats>;

  advisories(): AsyncIterable<Advisory>;
}

export class JetStreamManagerImpl extends BaseApiClient implements JSM {
  streams: StreamAPI;
  consumers: ConsumerAPI;
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
    this.streams = new StreamAPIImpl(nc, opts);
    this.consumers = new ConsumerAPIImpl(nc, opts);
  }

  async getAccountInfo(): Promise<JetStreamAccountStats> {
    const r = await this._request(`${this.prefix}.INFO`);
    return r as AccountInfoResponse;
  }

  advisories(): AsyncIterable<Advisory> {
    const iter = new QueuedIterator<Advisory>();
    this.nc.subscribe(`$JS.EVENT.ADVISORY.>`, {
      callback: (err, msg) => {
        try {
          const d = this.parseJsResponse(msg) as ApiResponse;
          const chunks = d.type.split(".");
          const kind = chunks[chunks.length - 1];
          iter.push({ kind: kind as AdvisoryKind, data: d });
        } catch (err) {
          iter.stop(err);
        }
      },
    });

    return iter;
  }
}

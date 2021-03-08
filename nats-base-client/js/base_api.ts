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
  defaultPrefix,
  defaultTimeout,
  JetstreamNotEnabled,
  JetStreamOptions,
} from "./jetstream.ts";

import type { Codec, NatsConnection, RequestOptions } from "../internal_mod.ts";
import { Empty, JSONCodec, Msg, NatsError } from "../internal_mod.ts";
import { ApiResponse, StreamNameBySubject, StreamNames } from "./types.ts";

export class BaseApiClient {
  nc: NatsConnection;
  opts: JetStreamOptions;
  prefix: string;
  timeout: number;
  jc: Codec<unknown>;

  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    this.nc = nc;
    this.opts = opts ? opts : {} as JetStreamOptions;
    this._parseOpts();
    this.prefix = this.opts.apiPrefix!;
    this.timeout = this.opts.timeout!;
    this.jc = JSONCodec();
  }

  _parseOpts() {
    let prefix = this.opts.apiPrefix || defaultPrefix;
    if (!prefix || prefix.length === 0) {
      throw new Error("invalid empty prefix");
    }
    const c = prefix[prefix.length - 1];
    if (c === ".") {
      prefix = prefix.substr(0, prefix.length - 1);
    }
    this.opts.apiPrefix = prefix;
    this.opts.timeout = this.opts.timeout || defaultTimeout;
  }

  async _request(
    subj: string,
    data: unknown = null,
    opts?: RequestOptions,
  ): Promise<unknown> {
    opts = opts || {} as RequestOptions;
    opts.timeout = this.timeout;

    let a: Uint8Array = Empty;
    if (data) {
      a = this.jc.encode(data);
    }

    const m = await this.nc.request(
      subj,
      a,
      opts,
    );
    return this.parseJsResponse(m);
  }

  async findStream(subject: string): Promise<string> {
    const q = { subject } as StreamNameBySubject;
    const r = await this._request(`${this.prefix}.STREAM.NAMES`, q);
    const names = r as StreamNames;
    if (!names.streams || names.streams.length !== 1) {
      throw new Error("no stream matches subject");
    }
    return names.streams[0];
  }

  parseJsResponse(m: Msg): unknown {
    const v = this.jc.decode(m.data);
    const r = v as ApiResponse;

    if (r.error) {
      if (r.error.code === 503) {
        throw NatsError.errorForCode(
          JetstreamNotEnabled,
          new Error(r.error.description),
        );
      }
      throw new NatsError(r.error.description, `${r.error.code}`);
    }
    return v;
  }
}

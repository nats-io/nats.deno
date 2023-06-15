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
import { StreamAPIImpl } from "./jsmstream_api.ts";
import { ConsumerAPI, ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import { QueuedIteratorImpl } from "../nats-base-client/queued_iterator.ts";
import {
  Advisory,
  AdvisoryKind,
  DirectMsg,
  DirectMsgHeaders,
  DirectStreamAPI,
  JetStreamClient,
  JetStreamManager,
  StoredMsg,
  StreamAPI,
} from "./types.ts";
import {
  JetStreamOptions,
  Msg,
  MsgHdrs,
  NatsConnection,
  ReviverFn,
} from "../nats-base-client/core.ts";
import {
  AccountInfoResponse,
  ApiResponse,
  DirectMsgRequest,
  JetStreamAccountStats,
  LastForMsgRequest,
} from "./jsapi_types.ts";
import { checkJsError, validateStreamName } from "./jsutil.ts";
import { Empty, TD } from "../nats-base-client/encoders.ts";
import { Codec, JSONCodec } from "../nats-base-client/codec.ts";

export class DirectStreamAPIImpl extends BaseApiClient
  implements DirectStreamAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async getMessage(
    stream: string,
    query: DirectMsgRequest,
  ): Promise<StoredMsg> {
    validateStreamName(stream);
    // if doing a last_by_subj request, we append the subject
    // this allows last_by_subj to be subject to permissions (KV)
    let qq: DirectMsgRequest | null = query;
    const { last_by_subj } = qq as LastForMsgRequest;
    if (last_by_subj) {
      qq = null;
    }

    const payload = qq ? this.jc.encode(qq) : Empty;
    const pre = this.opts.apiPrefix || "$JS.API";
    const subj = last_by_subj
      ? `${pre}.DIRECT.GET.${stream}.${last_by_subj}`
      : `${pre}.DIRECT.GET.${stream}`;

    const r = await this.nc.request(
      subj,
      payload,
    );

    // response is not a JS.API response
    const err = checkJsError(r);
    if (err) {
      return Promise.reject(err);
    }
    const dm = new DirectMsgImpl(r);
    return Promise.resolve(dm);
  }
}

export class DirectMsgImpl implements DirectMsg {
  data: Uint8Array;
  header: MsgHdrs;
  static jc?: Codec<unknown>;

  constructor(m: Msg) {
    if (!m.headers) {
      throw new Error("headers expected");
    }
    this.data = m.data;
    this.header = m.headers;
  }

  get subject(): string {
    return this.header.get(DirectMsgHeaders.Subject);
  }

  get seq(): number {
    const v = this.header.get(DirectMsgHeaders.Sequence);
    return typeof v === "string" ? parseInt(v) : 0;
  }

  get time(): Date {
    return new Date(Date.parse(this.timestamp));
  }

  get timestamp(): string {
    return this.header.get(DirectMsgHeaders.TimeStamp);
  }

  get stream(): string {
    return this.header.get(DirectMsgHeaders.Stream);
  }

  json<T = unknown>(reviver?: ReviverFn): T {
    return JSONCodec<T>(reviver).decode(this.data);
  }

  string(): string {
    return TD.decode(this.data);
  }
}

export class JetStreamManagerImpl extends BaseApiClient
  implements JetStreamManager {
  streams: StreamAPI;
  consumers: ConsumerAPI;
  direct: DirectStreamAPI;
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
    this.streams = new StreamAPIImpl(nc, opts);
    this.consumers = new ConsumerAPIImpl(nc, opts);
    this.direct = new DirectStreamAPIImpl(nc, opts);
  }

  async getAccountInfo(): Promise<JetStreamAccountStats> {
    const r = await this._request(`${this.prefix}.INFO`);
    return r as AccountInfoResponse;
  }

  jetstream(): JetStreamClient {
    return this.nc.jetstream(this.getOptions());
  }

  advisories(): AsyncIterable<Advisory> {
    const iter = new QueuedIteratorImpl<Advisory>();
    this.nc.subscribe(`$JS.EVENT.ADVISORY.>`, {
      callback: (err, msg) => {
        if (err) {
          throw err;
        }
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

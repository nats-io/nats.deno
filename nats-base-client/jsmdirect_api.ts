/*
 * Copyright 2022 The NATS Authors
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
import {
  DirectMsg,
  DirectMsgHeaders,
  DirectMsgRequest,
  DirectStreamAPI,
  Empty,
  JetStreamOptions,
  LastForMsgRequest,
  Msg,
  NatsConnection,
  StoredMsg,
} from "./types.ts";
import { checkJsError, validateStreamName } from "./jsutil.ts";
import { MsgHdrs } from "./headers.ts";

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
    const subj = last_by_subj
      ? `$JS.API.DIRECT.GET.${stream}.${last_by_subj}`
      : `$JS.API.DIRECT.GET.${stream}`;
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
    return new Date(Date.parse(this.header.get(DirectMsgHeaders.TimeStamp)));
  }

  get stream(): string {
    return this.header.get(DirectMsgHeaders.Stream);
  }
}

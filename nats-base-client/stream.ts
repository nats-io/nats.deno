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

import {
  Consumer,
  MsgRequest,
  StoredMsg,
  Stream,
  StreamAlternate,
  StreamAPI,
  StreamInfo,
  Streams,
} from "./types.ts";
import { StreamAPIImpl } from "./jsmstream_api.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import { OrderedConsumerOptions } from "./consumer.ts";
import { ConsumersImpl } from "./consumers.ts";

export class StreamsImpl implements Streams {
  api: StreamAPIImpl;

  constructor(api: StreamAPI) {
    this.api = api as StreamAPIImpl;
  }

  get(stream: string): Promise<Stream> {
    return this.api.info(stream)
      .then((si) => {
        return new StreamImpl(this.api, si);
      });
  }
}

export class StreamImpl implements Stream {
  api: StreamAPIImpl;
  _info: StreamInfo;

  constructor(api: StreamAPI, info: StreamInfo) {
    this.api = api as StreamAPIImpl;
    this._info = info;
  }

  get name(): string {
    return this._info.config.name;
  }

  alternates(): Promise<StreamAlternate[]> {
    return this.info()
      .then((si) => {
        return si.alternates ? si.alternates : [];
      });
  }

  async best(): Promise<Stream> {
    await this.info();
    if (this._info.alternates) {
      const asi = await this.api.info(this._info.alternates[0].name);
      return new StreamImpl(this.api, asi);
    } else {
      return this;
    }
  }

  info(cached = false): Promise<StreamInfo> {
    if (cached) {
      return Promise.resolve(this._info);
    }
    return this.api.info(this.name)
      .then((si) => {
        this._info = si;
        return this._info;
      });
  }

  getConsumer(
    name?: string | Partial<OrderedConsumerOptions>,
  ): Promise<Consumer> {
    return new ConsumersImpl(new ConsumerAPIImpl(this.api.nc, this.api.opts))
      .get(this.name, name);
  }

  getMessage(query: MsgRequest): Promise<StoredMsg> {
    return this.api.getMessage(this.name, query);
  }
}

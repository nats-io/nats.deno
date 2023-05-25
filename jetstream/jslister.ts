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
import {
  ApiPaged,
  ApiPagedRequest,
  ApiResponse,
  ConsumerListResponse,
  StreamListResponse,
} from "./jsapi_types.ts";

/**
 * An interface for listing. Returns a promise with typed list.
 */
export interface Lister<T> {
  [Symbol.asyncIterator](): AsyncIterator<T>;

  next(): Promise<T[]>;
}

export type ListerFieldFilter<T> = (v: unknown) => T[];

export class ListerImpl<T> implements Lister<T>, AsyncIterable<T> {
  err?: Error;
  offset: number;
  pageInfo: ApiPaged;
  subject: string;
  jsm: BaseApiClient;
  filter: ListerFieldFilter<T>;
  payload: unknown;

  constructor(
    subject: string,
    filter: ListerFieldFilter<T>,
    jsm: BaseApiClient,
    payload?: unknown,
  ) {
    if (!subject) {
      throw new Error("subject is required");
    }
    this.subject = subject;
    this.jsm = jsm;
    this.offset = 0;
    this.pageInfo = {} as ApiPaged;
    this.filter = filter;
    this.payload = payload || {};
  }

  async next(): Promise<T[]> {
    if (this.err) {
      return [];
    }
    if (this.pageInfo && this.offset >= this.pageInfo.total) {
      return [];
    }

    const offset = { offset: this.offset } as ApiPagedRequest;
    if (this.payload) {
      Object.assign(offset, this.payload);
    }
    try {
      const r = await this.jsm._request(
        this.subject,
        offset,
        { timeout: this.jsm.timeout },
      );
      this.pageInfo = r as ApiPaged;
      // offsets are reported in total, so need to count
      // all the entries returned
      this.offset += this.countResponse(r as ApiResponse);
      const a = this.filter(r);
      return a;
    } catch (err) {
      this.err = err;
      throw err;
    }
  }

  countResponse(r?: ApiResponse): number {
    switch (r?.type) {
      case "io.nats.jetstream.api.v1.stream_names_response":
      case "io.nats.jetstream.api.v1.stream_list_response":
        return (r as StreamListResponse).streams.length;
      case "io.nats.jetstream.api.v1.consumer_list_response":
        return (r as ConsumerListResponse).consumers.length;
      default:
        console.error(
          `jslister.ts: unknown API response for paged output: ${r?.type}`,
        );
        // has to be a stream...
        return (r as StreamListResponse).streams?.length || 0;
    }
    return 0;
  }

  async *[Symbol.asyncIterator]() {
    let page = await this.next();
    while (page.length > 0) {
      for (const item of page) {
        yield item;
      }
      page = await this.next();
    }
  }
}

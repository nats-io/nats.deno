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
import { ApiPaged, ApiPagedRequest } from "./types.ts";

export type ListerFieldFilter<T> = (v: unknown) => T[];

export interface Lister<T> {
  next(): Promise<T[]>;
}

export class ListerImpl<T> implements Lister<T> {
  err?: Error;
  offset: number;
  pageInfo: ApiPaged;
  subject: string;
  jsm: BaseApiClient;
  filter: ListerFieldFilter<T>;

  constructor(
    subject: string,
    filter: ListerFieldFilter<T>,
    jsm: BaseApiClient,
  ) {
    if (!subject) {
      throw new Error("subject is required");
    }
    this.subject = subject;
    this.jsm = jsm;
    this.offset = 0;
    this.pageInfo = {} as ApiPaged;
    this.filter = filter;
  }

  async next(): Promise<T[]> {
    if (this.err) {
      return [];
    }
    if (this.pageInfo && this.offset >= this.pageInfo.total) {
      return [];
    }

    const offset = { offset: this.offset } as ApiPagedRequest;
    try {
      const r = await this.jsm._request(
        this.subject,
        offset,
        { timeout: this.jsm.timeout },
      );
      this.pageInfo = r as ApiPaged;
      const a = this.filter(r);
      this.offset += a.length;
      return a;
    } catch (err) {
      this.err = err;
      throw err;
    }
  }
}

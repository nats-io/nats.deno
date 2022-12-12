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
import {
  Empty,
  NatsConnection,
  RequestManyOptions,
  RequestStrategy,
} from "./types.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import {
  JSONCodec,
  SchemaInfo,
  ServiceInfo,
  ServiceStats,
  ServiceVerb,
} from "./mod.ts";
import { ServiceIdentity, ServiceImpl, ServiceSchema } from "./service.ts";

export interface ServiceClient {
  /**
   * Pings services
   * @param name - optional
   * @param id - optional
   */
  ping(name?: string, id?: string): Promise<QueuedIterator<ServiceIdentity>>;

  /**
   * Requests all the stats from services
   * @param name
   * @param id
   */
  stats(name?: string, id?: string): Promise<QueuedIterator<ServiceStats>>;
  /**
   * Requests schema information from services
   * @param name
   * @param id
   */
  schema(name?: string, id?: string): Promise<QueuedIterator<ServiceSchema>>;

  /**
   * Requests info from services
   * @param name
   * @param id
   */
  info(name?: string, id?: string): Promise<QueuedIterator<ServiceInfo>>;
}

export class ServiceClientImpl implements ServiceClient {
  nc: NatsConnection;
  prefix: string | undefined;
  opts: RequestManyOptions;
  constructor(
    nc: NatsConnection,
    opts: RequestManyOptions = {
      strategy: RequestStrategy.JitterTimer,
      maxWait: 2000,
    },
    prefix?: string,
  ) {
    this.nc = nc;
    this.prefix = prefix;
    this.opts = opts;
  }

  ping(
    name = "",
    id = "",
  ): Promise<QueuedIterator<ServiceIdentity>> {
    return this.q<ServiceIdentity>(ServiceVerb.PING, name, id);
  }

  stats(
    name = "",
    id = "",
  ): Promise<QueuedIterator<ServiceStats>> {
    return this.q<ServiceStats>(ServiceVerb.STATS, name, id);
  }

  schema(
    name = "",
    id = "",
  ): Promise<QueuedIterator<ServiceSchema>> {
    return this.q<ServiceIdentity & { schema: SchemaInfo }>(
      ServiceVerb.SCHEMA,
      name,
      id,
    );
  }

  info(
    name = "",
    id = "",
  ): Promise<QueuedIterator<ServiceInfo>> {
    return this.q<ServiceInfo>(ServiceVerb.INFO, name, id);
  }

  async q<T>(
    v: ServiceVerb,
    name = "",
    id = "",
  ): Promise<QueuedIterator<T>> {
    const iter = new QueuedIteratorImpl<T>();
    const jc = JSONCodec<T>();
    const subj = ServiceImpl.controlSubject(v, name, id, this.prefix);
    const responses = await this.nc.requestMany(subj, Empty, this.opts);
    (async () => {
      for await (const m of responses) {
        try {
          const s = jc.decode(m.data);
          iter.push(s);
        } catch (err) {
          // @ts-ignore: pushing fn
          iter.push(() => {
            iter.stop(err);
          });
        }
      }
      //@ts-ignore: push a fn
      iter.push(() => {
        iter.stop();
      });
    })().catch((err) => {
      iter.stop(err);
    });
    return iter;
  }
}

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
import { Deferred, deferred } from "./util.ts";
import {
  Empty,
  Msg,
  MsgHdrs,
  Nanos,
  NatsConnection,
  NatsError,
  PublishOptions,
  Sub,
} from "./types.ts";
import { headers } from "./headers.ts";
import { JSONCodec } from "./codec.ts";
import { nuid } from "./nuid.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { nanos, validName } from "./jsutil.ts";
import { parseSemVer } from "./semver.ts";

/**
 * Services have common backplane subject pattern:
 *
 * `$SRV.PING|STATS|INFO|SCHEMA` - pings or retrieves status for all services
 * `$SRV.PING|STATS|INFO|SCHEMA.<name>` - pings or retrieves status for all services having the specified name
 * `$SRV.PING|STATS|INFO|SCHEMA.<name>.<id>` - pings or retrieves status of a particular service
 *
 * Note that <name> and <id> are upper-cased.
 */
export const ServiceApiPrefix = "$SRV";
export const ServiceErrorHeader = "Nats-Service-Error";
export const ServiceErrorCodeHeader = "Nats-Service-Error-Code";

export enum ServiceVerb {
  PING = "PING",
  STATS = "STATS",
  INFO = "INFO",
  SCHEMA = "SCHEMA",
}

export interface ServiceIdentity {
  /**
   * The kind of the service reporting the stats
   */
  name: string;
  /**
   * The unique ID of the service reporting the stats
   */
  id: string;
  /**
   * A version for the service
   */
  version: string;
}

export interface ServiceMsg extends Msg {
  respondError(
    code: number,
    description: string,
    data?: Uint8Array,
    opts?: PublishOptions,
  ): boolean;
}

export class ServiceMsgImpl implements ServiceMsg {
  msg: Msg;
  constructor(msg: Msg) {
    this.msg = msg;
  }

  get data(): Uint8Array {
    return this.msg.data;
  }

  get sid(): number {
    return this.msg.sid;
  }

  get subject(): string {
    return this.msg.subject;
  }

  respond(data?: Uint8Array, opts?: PublishOptions): boolean {
    return this.msg.respond(data, opts);
  }

  respondError(
    code: number,
    description: string,
    data?: Uint8Array,
    opts?: PublishOptions,
  ): boolean {
    opts = opts || {};
    opts.headers = opts.headers || headers();
    opts.headers?.set(ServiceErrorCodeHeader, `${code}`);
    opts.headers?.set(ServiceErrorHeader, description);
    return this.msg.respond(data, opts);
  }
}

export interface Service extends QueuedIterator<ServiceMsg> {
  /**
   * A promise that gets resolved to null or Error once the service ends.
   * If an error, then service exited because of an error.
   */
  stopped: Promise<null | Error>;
  /**
   * True if the service is stopped
   */
  // FIXME: this would be better as stop - but the queued iterator may be an issue perhaps call it `isStopped`
  isStopped: boolean;
  /**
   * Returns the stats for the service.
   */
  stats(): Promise<ServiceStats>;

  /**
   * Returns a service info for the service
   */
  info(): ServiceInfo;
  /**
   * Resets all the stats
   */
  reset(): void;
  /**
   * Stop the service returning a promise once the service completes.
   * If the service was stopped due to an error, that promise resolves to
   * the specified error
   */
  stop(err?: Error): Promise<null | Error>;
}

/**
 * Statistics for an endpoint
 */
export type EndpointStats = ServiceIdentity & {
  /**
   * The number of requests received by the endpoint
   */
  num_requests: number;
  /**
   * Number of errors that the endpoint has raised
   */
  num_errors: number;
  /**
   * If set, the last error triggered by the endpoint
   */
  last_error?: string;
  /**
   * A field that can be customized with any data as returned by stats handler see {@link ServiceConfig}
   */
  data?: unknown;
  /**
   * Total processing_time for the service
   */
  processing_time: Nanos;
  /**
   * Average processing_time is the total processing_time divided by the num_requests
   */
  average_processing_time: Nanos;

  /**
   * ISO Date string when the service started
   */
  started: string;
};

export type ServiceSchema = ServiceIdentity & {
  schema: SchemaInfo;
};

export type SchemaInfo = {
  request: string;
  response: string;
};

export type ServiceInfo = ServiceIdentity & {
  /**
   * Description for the service
   */
  description: string;
  /**
   * Subject where the service can be invoked
   */
  subject: string;
};

export type ServiceConfig = {
  /**
   * A type for a service
   */
  name: string;
  /**
   * A version identifier for the service
   */
  version: string;
  /**
   * Description for the service
   */
  description?: string;
  /**
   * Schema for the service
   */
  schema?: SchemaInfo;
  /**
   * A list of endpoints, typically one, but some services may
   * want more than one endpoint
   */
  endpoint: Endpoint;
  /**
   * A customized handler for the stats of an endpoint. The
   * data returned by the endpoint will be serialized as is
   * @param endpoint
   */
  statsHandler?: (
    endpoint: Endpoint,
  ) => Promise<unknown | null>;
};

/**
 * A service Endpoint
 */
export type Endpoint = {
  /**
   * Subject where the endpoint is listening
   */
  subject: string;
  /**
   * Handler for the endpoint - if not set the service is an iterator
   * @param err
   * @param msg
   */
  handler?: (err: NatsError | null, msg: ServiceMsg) => void;
};

/**
 * The stats of a service
 */
export type ServiceStats = ServiceIdentity & EndpointStats;

type InternalEndpoint = {
  name: string;
} & Endpoint;

type ServiceSubscription<T = unknown> =
  & Endpoint
  & {
    internal: boolean;
    sub: Sub<T>;
  };

// FIXME: perhaps the client is presented with a Request = Msg, but adds a respondError(code, description)
export class ServiceError extends Error {
  code: number;
  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }

  static isServiceError(msg: Msg): boolean {
    return ServiceError.toServiceError(msg) !== null;
  }

  static toServiceError(msg: Msg): ServiceError | null {
    const scode = msg?.headers?.get(ServiceErrorCodeHeader) || "";
    if (scode !== "") {
      const code = parseInt(scode) || 400;
      const description = msg?.headers?.get(ServiceErrorHeader) || "";
      return new ServiceError(code, description.length ? description : scode);
    }
    return null;
  }
}

export class ServiceImpl extends QueuedIteratorImpl<ServiceMsg>
  implements Service {
  nc: NatsConnection;
  _id: string;
  config: ServiceConfig;
  handler: ServiceSubscription;
  internal: ServiceSubscription[];
  _stopped: boolean;
  _done: Deferred<Error | null>;
  _stats!: EndpointStats;
  _lastException?: Error;
  _schema?: Uint8Array;

  /**
   * @param verb
   * @param name
   * @param id
   * @param prefix - this is only supplied by tooling when building control subject that crosses an account
   */
  static controlSubject(
    verb: ServiceVerb,
    name = "",
    id = "",
    prefix?: string,
  ) {
    // the prefix is used as is, because it is an
    // account boundary permission
    const pre = prefix ?? ServiceApiPrefix;
    if (name === "" && id === "") {
      return `${pre}.${verb}`;
    }
    name = name.toUpperCase();
    id = id.toUpperCase();
    return id !== ""
      ? `${pre}.${verb}.${name}.${id}`
      : `${pre}.${verb}.${name}`;
  }

  constructor(
    nc: NatsConnection,
    config: ServiceConfig,
  ) {
    super();
    this.nc = nc;
    this.config = config;
    const n = validName(this.name);
    if (n !== "") {
      throw new Error(`name ${n}`);
    }
    // this will throw if not semver
    parseSemVer(this.config.version);

    this.noIterator = typeof config?.endpoint?.handler === "function";
    if (!this.noIterator) {
      this.config.endpoint.handler = (err, msg): void => {
        err ? this.stop(err).catch() : this.push(new ServiceMsgImpl(msg));
      };
    }
    // initialize the stats
    this.reset();

    this._id = nuid.next();
    this.handler = config.endpoint as ServiceSubscription;
    this.internal = [] as ServiceSubscription[];
    this._done = deferred();
    this._stopped = false;

    // close if the connection closes
    this.nc.closed()
      .then(() => {
        this.close().catch();
      })
      .catch((err) => {
        this.close(err).catch();
      });

    // close the service if the iterator closes
    if (!this.noIterator) {
      this.iterClosed.then(() => {
        this.close().catch();
      });
    }
  }

  get subject(): string {
    const { subject } = <Endpoint> this.config.endpoint;
    if (subject !== "") {
      return subject;
    }
    return "";
  }

  get id(): string {
    return this._id;
  }

  get name(): string {
    return this.config.name;
  }

  get description(): string {
    return this.config.description ?? "";
  }

  get version(): string {
    return this.config.version;
  }

  errorToHeader(err: Error): MsgHdrs {
    const h = headers();
    if (err instanceof ServiceError) {
      const se = err as ServiceError;
      h.set(ServiceErrorHeader, se.message);
      h.set(ServiceErrorCodeHeader, `${se.code}`);
    } else {
      h.set(ServiceErrorHeader, err.message);
      h.set(ServiceErrorCodeHeader, "500");
    }
    return h;
  }

  countError(err: Error): void {
    this._lastException = err;
    this._stats.num_errors++;
    this._stats.last_error = err.message;
  }

  setupNATS(h: Endpoint, internal = false) {
    // internals don't use a queue
    const queue = internal ? "" : "q";
    const { subject, handler } = h as Endpoint;
    const sv = h as ServiceSubscription;
    sv.internal = internal;
    if (internal) {
      this.internal.push(sv);
    }

    const countLatency = (start: number): void => {
      if (internal) return;
      this._stats.num_requests++;
      this._stats.processing_time = nanos(Date.now() - start);
      this._stats.average_processing_time = Math.round(
        this._stats.processing_time / this._stats.num_requests,
      );
    };

    const callback = handler
      ? (err: NatsError | null, msg: Msg) => {
        if (err) {
          this.close(err);
          return;
        }
        const start = Date.now();
        try {
          handler(err, new ServiceMsgImpl(msg));
        } catch (err) {
          this.countError(err);
          msg?.respond(Empty, { headers: this.errorToHeader(err) });
        } finally {
          countLatency(start);
        }
      }
      : undefined;

    sv.sub = this.nc.subscribe(subject, {
      callback,
      queue,
    });

    sv.sub.closed
      .then(() => {
        if (!this._stopped) {
          this.close(new Error(`required subscription ${h.subject} stopped`))
            .catch();
        }
      })
      .catch((err) => {
        if (!this._stopped) {
          const ne = new Error(
            `required subscription ${h.subject} errored: ${err.message}`,
          );
          ne.stack = err.stack;
          this.close(ne).catch();
        }
      });
  }

  info(): ServiceInfo {
    return {
      name: this.name,
      id: this.id,
      version: this.version,
      description: this.description,
      subject: (this.config.endpoint as Endpoint).subject,
    } as ServiceInfo;
  }

  async stats(): Promise<ServiceStats> {
    if (typeof this.config.statsHandler === "function") {
      try {
        this._stats.data = await this.config.statsHandler(
          this.handler as Endpoint,
        );
      } catch (err) {
        this.countError(err);
      }
    }

    if (!this.noIterator) {
      this._stats.processing_time = this.time;
      this._stats.num_requests = this.processed;

      this._stats.average_processing_time = this.time > 0 && this.processed > 0
        ? this.time / this.processed
        : 0;
    }

    return Object.assign(
      {
        name: this.name,
        id: this.id,
        version: this.version,
      },
      this._stats,
    );
  }

  addInternalHandler(
    verb: ServiceVerb,
    handler: (err: NatsError | null, msg: Msg) => Promise<void>,
  ) {
    const v = `${verb}`.toUpperCase();
    this._doAddInternalHandler(`${v}-all`, verb, handler);
    this._doAddInternalHandler(`${v}-kind`, verb, handler, this.name);
    this._doAddInternalHandler(
      `${v}`,
      verb,
      handler,
      this.name,
      this.id,
    );
  }

  _doAddInternalHandler(
    name: string,
    verb: ServiceVerb,
    handler: (err: NatsError | null, msg: Msg) => Promise<void>,
    kind = "",
    id = "",
  ) {
    const endpoint = {} as InternalEndpoint;
    endpoint.name = name;
    endpoint.subject = ServiceImpl.controlSubject(verb, kind, id);
    endpoint.handler = handler;
    this.setupNATS(endpoint, true);
  }

  start(): Promise<Service> {
    const jc = JSONCodec();
    const statsHandler = (err: Error | null, msg: Msg): Promise<void> => {
      if (err) {
        this.close(err);
        return Promise.reject(err);
      }
      return this.stats().then((s) => {
        msg?.respond(jc.encode(s));
        return Promise.resolve();
      });
    };

    const infoHandler = (err: Error | null, msg: Msg): Promise<void> => {
      if (err) {
        this.close(err);
        return Promise.reject(err);
      }
      msg?.respond(jc.encode(this.info()));
      return Promise.resolve();
    };

    const ping = jc.encode({
      name: this.name,
      id: this.id,
      version: this.version,
    });
    const pingHandler = (err: Error | null, msg: Msg): Promise<void> => {
      if (err) {
        this.close(err).then().catch();
        return Promise.reject(err);
      }
      msg.respond(ping);
      return Promise.resolve();
    };

    const schemaHandler = (err: Error | null, msg: Msg): Promise<void> => {
      if (err) {
        this.close(err);
        return Promise.reject(err);
      }
      msg?.respond(this.schema);
      return Promise.resolve();
    };

    this.addInternalHandler(ServiceVerb.PING, pingHandler);
    this.addInternalHandler(ServiceVerb.STATS, statsHandler);
    this.addInternalHandler(ServiceVerb.INFO, infoHandler);
    this.addInternalHandler(ServiceVerb.SCHEMA, schemaHandler);

    // now the actual service
    const handlers = [this.handler];
    handlers.forEach((h) => {
      const { subject } = h as Endpoint;
      if (typeof subject !== "string") {
        return;
      }
      this.setupNATS(h as unknown as Endpoint);
    });

    return Promise.resolve(this);
  }

  close(err?: Error): Promise<null | Error> {
    if (this._stopped) {
      return this._done;
    }
    this._stopped = true;
    const buf: Promise<void>[] = [];
    if (!this.nc.isClosed()) {
      buf.push(this.handler.sub.drain());
      this.internal.forEach((serviceSub) => {
        buf.push(serviceSub.sub.drain());
      });
    }
    Promise.allSettled(buf)
      .then(() => {
        this._done.resolve(err ? err : null);
      });
    return this._done;
  }

  get stopped(): Promise<null | Error> {
    return this._done;
  }

  get isStopped(): boolean {
    return this._stopped;
  }

  stop(err?: Error): Promise<null | Error> {
    return this.close(err);
  }
  get schema(): Uint8Array {
    const jc = JSONCodec();
    if (!this._schema) {
      this._schema = jc.encode({
        name: this.name,
        id: this.id,
        version: this.version,
        schema: this.config.schema,
      });
    }
    return this._schema;
  }

  reset(): void {
    this._lastException = undefined;
    this._stats = {
      name: this.name,
      num_requests: 0,
      num_errors: 0,
      processing_time: 0,
      average_processing_time: 0,
      started: new Date().toISOString(),
    } as EndpointStats;
    if (!this.noIterator) {
      this.processed = 0;
      this.time = 0;
    }
  }
}

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
  Sub,
} from "./types.ts";
import { headers, JSONCodec, nuid } from "./mod.ts";
import { nanos, strictValidName } from "./jsutil.ts";

// FIXME: implement service iterator

/**
 * Services have common backplane subject pattern:
 *
 * `$SRV.PING|STATS|INFO|SCHEMA` - pings or retrieves status for all services
 * `$SRV.PING|STATS|INFO|SCHEMA.<kind>` - pings or retrieves status for all services having the specified kind
 * `$SRV.PING|STATS|INFO|SCHEMA.<kind>.<id>` - pings or retrieves status of a particular service
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

export type Service = {
  /**
   * ID for the service
   */
  id: string;
  /**
   * The name of the service
   */
  name: string;
  /**
   * The description for the service
   */
  description: string;
  /**
   * A version for the service
   */
  version: string;
  /**
   * A promise that gets resolved to null or Error once the service ends.
   * If an error, then service exited because of an error.
   */
  done: Promise<null | Error>;
  /**
   * True if the service is stopped
   */
  stopped: boolean;
  /**
   * Returns the stats for the service.
   * @param internal if true, aggregates stats for the generated internal endpoints.
   */
  stats(internal: boolean): ServiceStats;
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
};

/**
 * Statistics for an endpoint
 */
export type EndpointStats = {
  /**
   * Name of the endpoint
   */
  name: string;
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
  last_error?: Error;
  /**
   * A field that can be customized with any data as returned by stats handler see {@link ServiceConfig}
   */
  data?: unknown;
  /**
   * Total latency for the service
   */
  total_latency: Nanos;
  /**
   * Average latency is the total latency divided by the num_requests
   */
  average_latency: Nanos;
};

export type ServiceSchema = {
  request: string;
  response: string;
};

export type ServiceInfo = {
  /**
   * The kind of the service reporting the stats
   */
  name: string;
  /**
   * The unique ID of the service reporting the stats
   */
  id: string;
  /**
   * Description for the service
   */
  description: string;
  /**
   * Version of the service
   */
  version: string;

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
  version?: string;
  /**
   * Description for the service
   */
  description?: string;
  /**
   * Schema for the service
   */
  schema?: ServiceSchema;
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
   * Handler for the endpoint
   * @param err
   * @param msg
   */
  handler: (err: NatsError | null, msg: Msg) => Promise<void>;
};

type InternalEndpoint = {
  name: string;
} & Endpoint;

type ServiceSubscription<T = unknown> =
  & Endpoint
  & {
    internal: boolean;
    sub: Sub<T>;
  };

/**
 * The stats of a service
 */
export type ServiceStats = {
  /**
   * Name for the endpoint
   */
  name: string;
  /**
   * The unique ID of the service reporting the stats
   */
  id: string;
  /**
   * The version identifier for the service
   */
  version?: string;
  /**
   * An EndpointStats per each endpoint on the service
   */
  stats: EndpointStats[];
};

/**
 * Creates a service that uses the specified connection
 * @param nc
 * @param config
 */
export function addService(
  nc: NatsConnection,
  config: ServiceConfig,
): Promise<Service> {
  console.log(
    `\u001B[33m >> service framework is preview functionality \u001B[0m`,
  );
  const s = new ServiceImpl(nc, config);
  try {
    return s.start();
  } catch (err) {
    return Promise.reject(err);
  }
}

export class ServiceError extends Error {
  code: number;
  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }
}

const jc = JSONCodec();

export class ServiceImpl implements Service {
  nc: NatsConnection;
  _id: string;
  config: ServiceConfig;
  _done: Deferred<Error | null>;
  handler: ServiceSubscription;
  internal: ServiceSubscription[];
  stopped: boolean;
  watched: Promise<void>[];
  allStats: Map<Endpoint, EndpointStats>;
  interval!: number;

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
    this.nc = nc;
    this.config = config;
    const n = strictValidName(this.name);
    if (n !== "") {
      throw new Error(`name ${n}`);
    }

    this._id = nuid.next();
    this.handler = config.endpoint as ServiceSubscription;
    this.internal = [] as ServiceSubscription[];
    this.watched = [];
    this._done = deferred();
    this.stopped = false;
    this.allStats = new Map<ServiceSubscription, EndpointStats>();

    this.nc.closed()
      .then(() => {
        this.close();
      })
      .catch((err) => {
        this.close(err);
      });
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
    return this.config.version ?? "0.0.0";
  }

  errorToHeader(err: Error): MsgHdrs {
    const h = headers();
    if (err instanceof ServiceError) {
      const se = err as ServiceError;
      h.set(ServiceErrorHeader, se.message);
      h.set(ServiceErrorCodeHeader, `${se.code}`);
    } else {
      h.set(ServiceErrorHeader, err.message);
      h.set(ServiceErrorCodeHeader, "400");
    }
    return h;
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
    const { name } = h as InternalEndpoint;
    const stats: EndpointStats = {
      name: name ? name : this.name,
      num_requests: 0,
      num_errors: 0,
      total_latency: 0,
      average_latency: 0,
    };

    const countLatency = function (start: number) {
      stats.total_latency = nanos(Date.now() - start);
      stats.average_latency = Math.round(
        stats.total_latency / stats.num_requests,
      );
    };
    const countError = function (err: Error) {
      stats.num_errors++;
      stats.last_error = err;
    };

    sv.sub = this.nc.subscribe(subject, {
      callback: (err, msg) => {
        if (err) {
          this.close(err);
          return;
        }
        const start = Date.now();
        stats.num_requests++;
        try {
          handler(err, msg)
            .catch((err) => {
              countError(err);
              msg?.respond(Empty, { headers: this.errorToHeader(err) });
            }).finally(() => {
              countLatency(start);
            });
        } catch (err) {
          msg?.respond(Empty, { headers: this.errorToHeader(err) });
          countError(err);
          countLatency(start);
        }
      },
      queue,
    });
    this.allStats.set(h, stats);

    sv.sub.closed
      .then(() => {
        if (!this.stopped) {
          this.close(new Error(`required subscription ${h.subject} stopped`))
            .catch();
        }
      })
      .catch((err) => {
        if (!this.stopped) {
          const ne = new Error(
            `required subscription ${h.subject} errored: ${err.message}`,
          );
          ne.stack = err.stack;
          this.close(ne).catch();
        }
      });
  }

  stats(internal = false): ServiceStats {
    const ss: ServiceStats = {
      // stats: stats ? stats : null,
      name: this.name,
      id: this.id,
      version: this.version,
      stats: [],
    };

    // the stats for the service handler
    const stats = this.allStats.get(this.handler);
    if (stats) {
      if (typeof this.config.statsHandler === "function") {
        try {
          stats.data = this.config.statsHandler(this.handler as Endpoint);
        } catch (err) {
          stats.last_error = err;
        }
      }
      ss.stats.push(stats);
    }

    if (internal) {
      this.internal.forEach((h) => {
        const stats = this.allStats.get(h);
        if (stats) {
          ss.stats.push(stats);
        }
      });
    }

    return ss;
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
    const statsHandler = (err: Error | null, msg: Msg): Promise<void> => {
      if (err) {
        this.close(err);
        return Promise.reject(err);
      }
      let internal = true;
      try {
        if (msg.data) {
          const arg = jc.decode(msg.data) as { internal: boolean };
          internal = arg?.internal;
        }
      } catch (_err) {
        // ignored
      }

      const stats = this.stats(internal);
      msg?.respond(jc.encode(stats));
      return Promise.resolve();
    };

    const infoHandler = (err: Error | null, msg: Msg): Promise<void> => {
      if (err) {
        this.close(err);
        return Promise.reject(err);
      }
      const info = {
        name: this.name,
        id: this.id,
        version: this.version,
        description: this.description,
        subject: (this.config.endpoint as Endpoint).subject,
      } as ServiceInfo;
      msg?.respond(jc.encode(info));
      return Promise.resolve();
    };

    const ping = jc.encode({ name: this.name, id: this.id });
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

      msg?.respond(jc.encode(this.config.schema));
      return Promise.resolve();
    };

    this.addInternalHandler(ServiceVerb.PING, pingHandler);
    this.addInternalHandler(ServiceVerb.STATS, statsHandler);
    this.addInternalHandler(ServiceVerb.INFO, infoHandler);
    if (
      this.config.schema?.request !== "" || this.config.schema?.response !== ""
    ) {
      this.addInternalHandler(ServiceVerb.SCHEMA, schemaHandler);
    }

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
    if (this.stopped) {
      return this._done;
    }
    this.stopped = true;
    const buf: Promise<void>[] = [];
    clearInterval(this.interval);
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

  get done(): Promise<null | Error> {
    return this._done;
  }

  stop(err?: Error): Promise<null | Error> {
    return this.close(err);
  }

  reset(): void {
    const iter = this.allStats.values();
    for (const s of iter) {
      s.average_latency = 0;
      s.num_errors = 0;
      s.num_requests = 0;
      s.total_latency = 0;
      s.data = undefined;
    }
  }
}

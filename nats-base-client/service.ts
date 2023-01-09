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
import { nanos, validateName } from "./jsutil.ts";
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

export enum ServiceResponseType {
  STATS = "io.nats.micro.v1.stats_response",
  INFO = "io.nats.micro.v1.info_response",
  PING = "io.nats.micro.v1.ping_response",
  SCHEMA = "io.nats.micro.v1.schema_response",
}

export interface ServiceResponse {
  /**
   * Response type schema
   */
  type: ServiceResponseType;
}

export interface ServiceIdentity extends ServiceResponse {
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

export interface ServiceGroup {
  /**
   * The name or {name, subject} for configuring the endpoint.
   * The name of the endpoint must be a simple subject token with no wildcards
   * @param name
   * @param handler
   */
  addEndpoint(
    name: string | { name: string; subject: string },
    handler?: ServiceHandler,
  ): QueuedIterator<ServiceMsg>;

  /**
   * A group is a subject prefix from which endpoints can be added.
   * Can be empty to allow for prefixes or tokens that are set at runtime
   * without requiring editing of the service.
   * @param subject
   */
  addGroup(subject?: string): ServiceGroup;
}

export class ServiceGroupImpl implements ServiceGroup {
  subject: string;
  srv: ServiceImpl;
  constructor(parent: ServiceGroup, name = "") {
    if (name !== "") {
      validInternalToken("service group", name);
    }
    let root = "";
    if (parent instanceof ServiceImpl) {
      this.srv = parent as ServiceImpl;
      root = "";
    } else if (parent instanceof ServiceGroupImpl) {
      const sg = parent as ServiceGroupImpl;
      this.srv = sg.srv;
      root = sg.subject;
    } else {
      throw new Error("unknown ServiceGroup type");
    }
    this.subject = this.calcSubject(root, name);
  }

  calcSubject(root: string, name = ""): string {
    if (name === "") {
      return root;
    }
    return root !== "" ? `${root}.${name}` : name;
  }
  addEndpoint(
    name: string | { name: string; subject: string } = "",
    handler?: ServiceHandler,
  ): QueuedIterator<ServiceMsg> {
    if (typeof name === "string") {
      name = { name, subject: name };
    }
    const ns = typeof name === "string"
      ? { name, subject: name }
      : name as { name: string; subject: string };
    validateName("endpoint", name.name);
    ns.subject = ns.subject || ns.name;
    validSubjectName("endpoint subject", ns.subject);
    const subj = this.calcSubject(this.subject, ns.subject);
    return this.srv._addEndpoint(ns.name, subj, handler);
  }

  addGroup(name = ""): ServiceGroup {
    return new ServiceGroupImpl(this, name);
  }
}

function validSubjectName(context: string, subj: string) {
  if (subj === "") {
    throw new Error(`${context} cannot be empty`);
  }
  if (subj.indexOf(" ") !== -1) {
    throw new Error(`${context} cannot contain spaces: '${subj}'`);
  }
  const tokens = subj.split(".");
  tokens.forEach((v, idx) => {
    if (v === ">" && idx !== tokens.length - 1) {
      throw new Error(`${context} cannot have internal '>': '${subj}'`);
    }
  });
}

function validInternalToken(context: string, subj: string) {
  if (subj.indexOf(" ") !== -1) {
    throw new Error(`${context} cannot contain spaces: '${subj}'`);
  }
  const tokens = subj.split(".");
  tokens.forEach((v) => {
    if (v === ">") {
      throw new Error(`${context} name cannot contain internal '>': '${subj}'`);
    }
  });
}

export interface Service extends ServiceGroup, QueuedIterator<ServiceMsg> {
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

type Stats = {
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
};

export type NamedEndpointStats = Stats & {
  name: string;
  subject: string;
};

/**
 * Statistics for an endpoint
 */
export type EndpointStats = ServiceIdentity & {
  endpoints?: NamedEndpointStats[];
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
  subjects: string[];
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
   * An optional endpoint mapping a handler to a subject.
   * More complex multi-endpoint services can be achieved by
   * {@link Service}.addEndpoint() and addGroup().
   */
  endpoint?: Endpoint;
  /**
   * A customized handler for the stats of an endpoint. The
   * data returned by the endpoint will be serialized as is
   * @param endpoint
   */
  statsHandler?: (
    endpoint: Endpoint,
  ) => Promise<unknown | null>;
};

export type ServiceHandler = (err: NatsError | null, msg: ServiceMsg) => void;

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
  handler?: ServiceHandler;
};
/**
 * The stats of a service
 */
export type ServiceStats = ServiceIdentity & EndpointStats;

type NamedEndpoint = {
  name: string;
} & Endpoint;

type ServiceSubscription<T = unknown> =
  & NamedEndpoint
  & {
    internal: boolean;
    sub: Sub<T>;
    qi?: QueuedIterator<T>;
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
  handlers: ServiceSubscription[];
  internal: ServiceSubscription[];
  _stopped: boolean;
  _done: Deferred<Error | null>;
  _stats!: EndpointStats;
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
    validateName("control subject name", name);
    if (id !== "") {
      validateName("control subject id", id);
      return `${pre}.${verb}.${name}.${id}`;
    }
    return `${pre}.${verb}.${name}`;
  }

  constructor(
    nc: NatsConnection,
    config: ServiceConfig = { name: "", version: "" },
  ) {
    super();
    this.nc = nc;
    this.config = config;
    // this will throw if no name
    validateName("name", this.config.name);
    // this will throw if not semver
    parseSemVer(this.config.version);
    this._id = nuid.next();
    this.internal = [] as ServiceSubscription[];
    this._done = deferred();
    this._stopped = false;
    this.handlers = [];
    this.noIterator = true;
    // initialize the stats
    this.reset();
    if (this.config.endpoint) {
      this._addEndpoint(
        "default",
        this.config.endpoint?.subject,
        this.config.endpoint?.handler,
        true,
      );
    }

    // close if the connection closes
    this.nc.closed()
      .then(() => {
        this.close().catch();
      })
      .catch((err) => {
        this.close(err).catch();
      });
  }

  get subjects(): string[] {
    return this.handlers.filter((s) => {
      return s.internal === false;
    }).map((s) => {
      return s.subject;
    });
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

  countError(err: Error, name: string, subject: string): void {
    const ns = this._stats.endpoints?.find((e) => {
      return e.name === name && e.subject === subject;
    });
    if (ns) {
      ns.num_errors++;
      ns.last_error = err.message;
    }
  }

  setupHandler(
    h: NamedEndpoint,
    internal = false,
  ): ServiceSubscription {
    // internals don't use a queue
    const queue = internal ? "" : "q";
    const { name, subject, handler } = h as NamedEndpoint;
    const sv = h as ServiceSubscription;
    sv.internal = internal;
    if (internal) {
      this.internal.push(sv);
    }

    const countLatency = (start: number): void => {
      if (internal) return;
      let s: NamedEndpointStats | undefined;
      if (name) {
        s = this._stats.endpoints?.find((v) => {
          return v.name === name && v.subject === subject;
        });
      }
      if (s) {
        s.num_requests++;
        s.processing_time = nanos(Date.now() - start);
        s.average_processing_time = Math.round(
          s.processing_time / s.num_requests,
        );
      }
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
          this.countError(err, h.name, h.subject);
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
    return sv;
  }

  info(): ServiceInfo {
    return {
      type: ServiceResponseType.INFO,
      name: this.name,
      id: this.id,
      version: this.version,
      description: this.description,
      subjects: this.subjects,
    } as ServiceInfo;
  }

  getNamedStats(name: string, subject: string): NamedEndpointStats {
    return this._stats.endpoints?.find((s) => {
      return s.name === name && s.subject === subject;
    })!;
  }

  async stats(): Promise<ServiceStats> {
    if (typeof this.config.statsHandler === "function") {
      for (const h of this.handlers) {
        try {
          if (h.handler === null) {
            continue;
          }
          const stats = this.getNamedStats(h.name, h.subject);
          stats.data = await this.config.statsHandler(h);

          const qi = h.qi as QueuedIteratorImpl<ServiceMsg>;
          if (qi) {
            if (!qi.noIterator) {
              // grab stats in the iterator
              stats.processing_time = qi.time;
              stats.num_requests = qi.processed;
              stats.average_processing_time =
                stats.processing_time > 0 && stats.num_requests > 0
                  ? stats.processing_time / stats.num_requests
                  : 0;
            }
          }
        } catch (err) {
          this.countError(err, h.name, h.subject);
        }
      }
    }
    const stats = Object.assign(
      {
        type: ServiceResponseType.STATS,
        name: this.name,
        id: this.id,
        version: this.version,
      },
      this._stats,
    );
    return stats;
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
    const endpoint = {} as NamedEndpoint;
    endpoint.name = name;
    endpoint.subject = ServiceImpl.controlSubject(verb, kind, id);
    endpoint.handler = handler;
    this.setupHandler(endpoint, true);
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
      type: ServiceResponseType.PING,
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
    this.handlers.forEach((h) => {
      const { subject } = h as Endpoint;
      if (typeof subject !== "string") {
        return;
      }
      // this is expected in cases where main subject is just
      // a root subject for multiple endpoints - user can disable
      // listening to the root endpoint, by specifying null
      if (h.handler === null) {
        return;
      }
      this.setupHandler(h as unknown as NamedEndpoint);
    });

    return Promise.resolve(this);
  }

  close(err?: Error): Promise<null | Error> {
    if (this._stopped) {
      return this._done;
    }
    this._stopped = true;
    let buf: Promise<void>[] = [];
    if (!this.nc.isClosed()) {
      buf = this.handlers.concat(this.internal).map((h) => {
        return h.sub.drain();
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
        type: ServiceResponseType.SCHEMA,
        name: this.name,
        id: this.id,
        version: this.version,
        schema: this.config.schema,
      });
    }
    return this._schema;
  }

  reset(): void {
    // pretend we restarted
    this._stats = this._stats ? this._stats : {} as EndpointStats;
    this._stats.started = new Date().toISOString();

    if (this.handlers) {
      for (const h of this.handlers) {
        if (h.handler === null) {
          continue;
        }
        const stats = this.getNamedStats(h.name, h.subject);
        stats.subject = h.subject;
        stats.num_requests = 0;
        stats.num_errors = 0, stats.processing_time = 0;
        stats.average_processing_time = 0;
        stats.num_errors = 0;
        stats.last_error = undefined;
        const qi = h.qi as QueuedIteratorImpl<ServiceMsg>;
        if (qi) {
          if (!qi.noIterator) {
            // grab stats in the iterator
            qi.time = 0;
            qi.processed = 0;
          }
        }
      }
    }
  }

  addGroup(name: string): ServiceGroup {
    return new ServiceGroupImpl(this, name);
  }

  addEndpoint(
    name: string,
    handler?: ServiceHandler,
  ): QueuedIterator<ServiceMsg> {
    const sg = new ServiceGroupImpl(this);
    return sg.addEndpoint(name, handler);
  }

  _addEndpoint(
    name: string,
    subject: string,
    handler?: ServiceHandler,
    main = false,
  ): QueuedIterator<ServiceMsg> {
    const qi = main ? this : new QueuedIteratorImpl<ServiceMsg>();
    qi.noIterator = typeof handler === "function";
    if (!qi.noIterator) {
      handler = (err, msg): void => {
        err ? this.stop(err).catch() : qi.push(new ServiceMsgImpl(msg));
      };
      // close the service if the iterator closes
      qi.iterClosed.then(() => {
        this.close().catch();
      });
    }
    // track the iterator for stats
    const ss = this.setupHandler({ name, subject, handler }, false);
    ss.qi = qi;
    this.handlers.push(ss);
    this._stats.endpoints = !this._stats.endpoints ? [] : this._stats.endpoints;
    this._stats.endpoints.push(newEndpointStats(name, subject));
    return qi;
  }
}

function newEndpointStats(name: string, subject: string): NamedEndpointStats {
  return {
    name,
    subject,
    num_requests: 0,
    num_errors: 0,
    processing_time: 0,
    average_processing_time: 0,
  };
}

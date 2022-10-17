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
  Consumer,
  JsMsg,
  Msg,
  NatsConnection,
  NatsError,
  Sub,
} from "./types.ts";
import { JSONCodec, nuid } from "./mod.ts";
import { validName } from "./jsutil.ts";

/**
 * Services have common backplane subject pattern:
 *
 * `$SRV.PING|STATUS|INFO|SCHEMA` - pings or retrieves status for all services
 * `$SRV.PING|STATUS|INFO|SCHEMA.<kind>` - pings or retrieves status for all services having the specified kind
 * `$SRV.PING|STATUS|INFO|SCHEMA.<kind>.<id>` - pings or retrieves status of a particular service
 */
export const SrvPrefix = "$SRV";

export enum SrvVerb {
  PING = "PING",
  STATUS = "STATUS",
  INFO = "INFO",
  SCHEMA = "SCHEMA",
}

export type Service = {
  id: string;
  status(internal: boolean): ServiceStatus;
  stop(): Promise<null | Error>;
  done: Promise<null | Error>;
  stopped: boolean;
};

export type EndpointStatus = {
  name: string;
  requests: number;
  errors: number;
  lastError?: Error;
  data?: unknown;
};

export type ServiceSchema = {
  request: string;
  response: string;
};

export type ServiceInfo = {
  /**
   * The kind of the service reporting the status
   */
  name: string;
  /**
   * The unique ID of the service reporting the status
   */
  id: string;
  /**
   * Description for the service
   */
  description?: string;
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
  endpoints:
    | Endpoint
    | JetStreamEndpoint;
  /**
   * A customized handler for the status of an endpoint. The
   * data returned by the endpoint will be serialized as is
   * @param endpoint
   */
  statusHandler?: (
    endpoint: Endpoint | JetStreamEndpoint,
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
  handler: (err: NatsError | null, msg: Msg) => void;
};

type InternalEndpoint = {
  name: string;
} & Endpoint;

type JetStreamEndpoint = {
  consumer: Consumer;
  handler: (err: Error | null, msg: JsMsg) => void;
};

type ServiceSubscription<T = unknown> =
  & Endpoint
  & JetStreamEndpoint
  & {
    internal: boolean;
    sub: Sub<T>;
  };

/**
 * The status of a service
 */
export type ServiceStatus = {
  /**
   * Name for the endpoint
   */
  name: string;
  /**
   * The unique ID of the service reporting the status
   */
  id: string;
  /**
   * The version identifier for the service
   */
  version?: string;
  /**
   * An EndpointStatus per each endpoint on the service
   */
  endpoints: EndpointStatus[];
};

/**
 * Creates a service that uses the specied connection
 * @param nc
 * @param config
 */
export function addService(
  nc: NatsConnection,
  config: ServiceConfig,
): Promise<Service> {
  const s = new ServiceImpl(nc, config);
  try {
    return s.start();
  } catch (err) {
    return Promise.reject(err);
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
  statuses: Map<Endpoint | JetStreamEndpoint, EndpointStatus>;
  interval!: number;

  static controlSubject(verb: SrvVerb, name = "", id = "") {
    if (name === "" && id === "") {
      return `${SrvPrefix}.${verb}`;
    }
    name = name.toUpperCase();
    id = id.toUpperCase();
    return id !== ""
      ? `${SrvPrefix}.${verb}.${name}.${id}`
      : `${SrvPrefix}.${verb}.${name}`;
  }

  constructor(
    nc: NatsConnection,
    config: ServiceConfig,
  ) {
    this.nc = nc;
    this.config = config;
    const n = validName(this.config.name);
    if (n !== "") {
      throw new Error(`name ${n}`);
    }

    this._id = nuid.next();
    this.config.name = this.config.name;
    this.handler = config.endpoints as ServiceSubscription;
    this.internal = [] as ServiceSubscription[];
    this.watched = [];
    this._done = deferred();
    this.stopped = false;
    this.statuses = new Map<ServiceSubscription, EndpointStatus>();

    this.nc.closed()
      .then(() => {
        this.close();
      })
      .catch((err) => {
        this.close(err);
      });
  }

  get id(): string {
    return this._id;
  }

  // async setupJetStream(h: JetStreamSrvHandler): Promise<void> {
  //   const sv = h as SrvHandlerSub;
  //   const status: SrvStatusEntry = {
  //     requests: 0,
  //     errors: 0,
  //   };
  //   try {
  //     const c = await h.consumer.read({
  //       callback: (jsmsg) => {
  //         status.requests++;
  //         h.handler(null, jsmsg);
  //       },
  //     }) as JetStreamReader;
  //
  //     c.closed
  //       .then(() => {
  //         this.close(
  //           new Error(`required consumer for handler ${h.name} stopped`),
  //         ).catch();
  //       })
  //       .catch((err) => {
  //         status.errors++;
  //         status.lastError = err;
  //         this.close(
  //           new Error(
  //             `required consumer for handler ${h.name} errored: ${err.message}`,
  //           ),
  //         ).catch();
  //       });
  //
  //     this.statuses.set(h, status);
  //   } catch (err) {
  //     this.close(err);
  //   }
  // }

  setupNATS(h: Endpoint, internal = false) {
    const queue = internal ? "" : "x";
    const { subject, handler } = h as Endpoint;
    const sv = h as ServiceSubscription;
    sv.internal = internal;
    if (internal) {
      this.internal.push(sv);
    }
    let { name } = h as InternalEndpoint;
    const status: EndpointStatus = {
      name: name ? name : this.config.name,
      requests: 0,
      errors: 0,
    };

    const sub = this.nc.subscribe(subject, {
      callback: (err, msg) => {
        if (err) {
          status.errors++;
          status.lastError = err;
        }
        status.requests++;
        handler(err, msg);
      },
      queue,
    });
    sv.sub = sub;
    this.statuses.set(h, status);

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

  status(internal = false): ServiceStatus {
    const ss: ServiceStatus = {
      // status: status ? status : null,
      name: this.config.name,
      id: this.id,
      version: this.config.version,
      endpoints: [],
    };

    // the status for the service handler
    const status = this.statuses.get(this.handler);
    if (status) {
      if (typeof this.config.statusHandler === "function") {
        try {
          status.data = this.config.statusHandler(this.handler as Endpoint);
        } catch (err) {
          status.lastError = err;
        }
      }
      ss.endpoints.push(status);
    }

    if (internal) {
      this.internal.forEach((h) => {
        const status = this.statuses.get(h);
        if (status) {
          ss.endpoints.push(status);
        }
      });
    }

    return ss;
  }

  addInternalHandler(
    verb: SrvVerb,
    handler: (err: NatsError | null, msg: Msg) => void,
  ) {
    const v = `${verb}`.toLowerCase();
    this._doAddInternalHandler(`${v}-all`, verb, handler);
    this._doAddInternalHandler(`${v}-kind`, verb, handler, this.config.name);
    this._doAddInternalHandler(
      `${v}`,
      verb,
      handler,
      this.config.name,
      this.id,
    );
  }

  _doAddInternalHandler(
    name: string,
    verb: SrvVerb,
    handler: (err: NatsError | null, msg: Msg) => void,
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
    const statusHandler = (err: Error | null, msg: Msg): void => {
      if (err) {
        this.close(err);
        return;
      }
      let internal = true;
      try {
        if (msg.data) {
          const arg = jc.decode(msg.data) as { internal: boolean };
          internal = arg?.internal;
        }
      } catch (err) {
        // ignored
      }

      const status = this.status(internal);
      msg?.respond(jc.encode(status));
    };

    const pingHandler = (err: Error | null, msg: Msg): void => {
      if (err) {
        this.close(err);
        return;
      }
      msg?.respond(
        jc.encode({
          name: this.config.name,
          id: this.id,
          description: this.config.description,
        }),
      );
    };

    const infoHandler = (err: Error | null, msg: Msg): void => {
      if (err) {
        this.close(err);
        return;
      }
      const info = {
        name: this.config.name,
        id: this.id,
        version: this.config.version,
      } as ServiceInfo;
      msg?.respond(jc.encode(info));
    };

    const schemaHandler = (err: Error | null, msg: Msg): void => {
      if (err) {
        this.close(err);
        return;
      }

      msg?.respond(jc.encode(this.config.schema));
    };

    this.addInternalHandler(SrvVerb.PING, pingHandler);
    this.addInternalHandler(SrvVerb.STATUS, statusHandler);
    this.addInternalHandler(SrvVerb.INFO, infoHandler);
    if (
      this.config.schema?.request !== "" || this.config.schema?.response !== ""
    ) {
      this.addInternalHandler(SrvVerb.SCHEMA, schemaHandler);
    }

    // now the actual service
    const handlers = [this.handler];
    handlers.forEach((h) => {
      const { subject } = h as Endpoint;
      if (typeof subject !== "string") {
        // this is a jetstream service
        return;
      }
      this.setupNATS(h as unknown as Endpoint);
    });

    handlers.forEach((h) => {
      const { consumer } = h as JetStreamEndpoint;
      if (!consumer) {
        // this is a nats service
        return;
      }
      // this.setupJetStream(h as unknown as JetStreamSrvHandler);
    });

    return Promise.resolve(this);
  }

  close(err?: Error): Promise<null | Error> {
    if (this.stopped) {
      return Promise.resolve(null);
    }
    this.stopped = true;
    clearInterval(this.interval);
    if (!this.nc.isClosed()) {
      const user = this.handler.sub.drain();
      const internal = this.internal.map((serviceSub) => {
        return serviceSub.sub.drain();
      });

      Promise.all([user].concat(internal))
        .then(() => {
          this._done.resolve(err ? err : null);
        });
    } else {
      this._done.resolve(null);
    }

    return this._done;
  }

  get done(): Promise<null | Error> {
    return this._done;
  }

  stop(): Promise<null | Error> {
    return this.close();
  }
}

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
  // JetStreamReader,
  JsMsg,
  Msg,
  NatsConnection,
  Sub,
} from "./types.ts";
import { JSONCodec } from "./mod.ts";

/**
 * Services have common backplane subject pattern:
 *
 * `$SRV.PING|STATUS` - pings or retrieves status for all services
 * `$SRV.PING|STATUS.<kind>` - pings or retrieves status for all services having the specified kind
 * `$SRV.PING|STATUS.<kind>.<id>` - pings or retrieves status of a particular service
 * `$SRV.HEARTBEAT.<kind>.<id>` - heartbeat from a service
 */
export const SrvPrefix = "$SRV";

export enum SrvVerb {
  PING = "PING",
  STATUS = "STATUS",
  HEARTBEAT = "HEARTBEAT",
}

export type Service = {
  status(internal: boolean): ServiceStatus;
  stop(): Promise<null | Error>;
  done: Promise<null | Error>;
  stopped: boolean;
  heartbeatInterval: number;
};

export type EndpointStatus = {
  name: string;
  requests: number;
  errors: number;
  lastError?: Error;
  data?: unknown;
};

export type ServiceConfig = {
  /**
   * A type for a service
   */
  kind: string;
  /**
   * Unique ID for a service of a particular kind
   */
  id: string;
  /**
   * Description for the service
   */
  description?: string;
  /**
   * Schema for the service
   */
  schema?: {
    request: string;
    response: string;
  };
  /**
   * A list of endpoints, typically one, but some services may
   * want more than one endpoint
   */
  endpoints:
    | Endpoint
    | JetStreamEndpoint
    | (Endpoint | JetStreamEndpoint)[];
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
   * Name for the endpoint
   */
  name: string;
  /**
   * Subject where the endpoint is listening
   */
  subject: string;
  /**
   * Handler for the endpoint
   * @param err
   * @param msg
   */
  handler: (err: Error | null, msg: Msg) => void;
  /**
   * Queue group for the endpoint
   */
  queueGroup?: string;
};

type JetStreamEndpoint = {
  name: string;
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
   * The kind of the service reporting the status
   */
  kind: string;
  /**
   * The unique ID of the service reporting the status
   */
  id: string;
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
  config: ServiceConfig;
  _done: Deferred<Error | null>;
  handlers: ServiceSubscription[];
  internal: ServiceSubscription[];
  _heartbeatInterval: number;
  stopped: boolean;
  watched: Promise<void>[];
  statuses: Map<Endpoint | JetStreamEndpoint, EndpointStatus>;
  interval!: number;

  static controlSubject(verb: SrvVerb, kind = "", id = "") {
    if (kind === "" && id === "") {
      return `${SrvPrefix}.${verb}`;
    }
    kind = kind.toUpperCase();
    id = id.toUpperCase();
    return id !== ""
      ? `${SrvPrefix}.${verb}.${kind}.${id}`
      : `${SrvPrefix}.${verb}.${kind}`;
  }

  constructor(
    nc: NatsConnection,
    config: ServiceConfig,
  ) {
    this.nc = nc;
    this.config = config;
    this.config.id = this.config.id.toUpperCase();
    this.config.kind = this.config.kind.toUpperCase();

    this.handlers = Array.isArray(config.endpoints)
      ? config.endpoints as ServiceSubscription[]
      : [config.endpoints] as ServiceSubscription[];
    this.internal = [] as ServiceSubscription[];
    this.watched = [];
    this._done = deferred();
    this.stopped = false;
    this.statuses = new Map<ServiceSubscription, EndpointStatus>();
    this._heartbeatInterval = 15 * 1000;

    this.nc.closed()
      .then(() => {
        this.close();
      })
      .catch((err) => {
        this.close(err);
      });
  }

  get heartbeatInterval() {
    return this._heartbeatInterval;
  }

  set heartbeatInterval(n: number) {
    this._heartbeatInterval = n;
    if (this.interval) {
      clearInterval(this.interval);
    }
    this.interval = setInterval(() => {
      if (this.nc.isClosed()) {
        return;
      }
      const d = this.status(true);
      this.nc.publish(
        ServiceImpl.controlSubject(
          SrvVerb.HEARTBEAT,
          this.config.kind,
          this.config.id,
        ),
        jc.encode(d),
      );
    }, this._heartbeatInterval);
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
    const { subject, handler, queueGroup: queue } = h as Endpoint;
    const sv = h as ServiceSubscription;
    sv.internal = internal;
    const status: EndpointStatus = {
      name: sv.name,
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
      kind: this.config.kind,
      id: this.config.id,
      endpoints: [],
    };

    const handlers = internal
      ? this.handlers.concat(this.internal)
      : this.handlers;
    for (let i = 0; i < handlers.length; i++) {
      const h = handlers[i];
      const status = this.statuses.get(h);
      if (status) {
        if (typeof this.config.statusHandler === "function") {
          try {
            status.data = this.config.statusHandler(h as Endpoint);
          } catch (err) {
            status.lastError = err;
          }
        }
        ss.endpoints.push(status);
      }
    }
    return ss;
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
      msg?.respond();
    };

    const internal = [
      {
        name: "ping",
        subject: ServiceImpl.controlSubject(
          SrvVerb.PING,
          this.config.kind,
          this.config.id,
        ),
        handler: pingHandler,
      },
      {
        name: "ping-kind",
        subject: ServiceImpl.controlSubject(SrvVerb.PING, this.config.kind),
        handler: pingHandler,
      },
      {
        name: "ping-all",
        subject: ServiceImpl.controlSubject(SrvVerb.PING),
        handler: pingHandler,
      },
      {
        name: "status",
        subject: ServiceImpl.controlSubject(
          SrvVerb.STATUS,
          this.config.kind,
          this.config.id,
        ),
        handler: statusHandler,
      },
      {
        name: "status-kind",
        subject: ServiceImpl.controlSubject(SrvVerb.STATUS, this.config.kind),
        handler: statusHandler,
      },
      {
        name: "status-all",
        subject: ServiceImpl.controlSubject(SrvVerb.STATUS),
        handler: statusHandler,
      },
    ] as Endpoint[];

    internal.forEach((e) => {
      this.setupNATS(e, true);
    });

    this.internal = internal as unknown as ServiceSubscription[];
    this.handlers.forEach((h) => {
      const { subject } = h as Endpoint;
      if (typeof subject !== "string") {
        // this is a jetstream service
        return;
      }
      this.setupNATS(h as unknown as Endpoint);
    });

    this.handlers.forEach((h) => {
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
    clearInterval(this.interval);
    this.stopped = true;
    this._done.resolve(err ? err : null);
    return this._done;
  }

  get done(): Promise<null | Error> {
    return this._done;
  }

  stop(): Promise<null | Error> {
    return this.close();
  }
}

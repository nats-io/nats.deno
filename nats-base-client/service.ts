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
  handlers: Endpoint[];
  status(internal: boolean): ServiceStatus[];
  stop(): Promise<null | Error>;
  done: Promise<null | Error>;
  stopped: boolean;
  heartbeatInterval: number;
};

export type ServiceStatusEntry = {
  requests: number;
  errors: number;
  lastError?: Error;
  data?: unknown;
};

export type ServiceConfig = {
  kind: string;
  id: string;
  description?: string;
  schema?: {
    request: string;
    response: string;
  };
  endpoints:
    | Endpoint
    | JetStreamEndpoint
    | (Endpoint | JetStreamEndpoint)[];
  statusHandler?: (
    endpoint: Endpoint | JetStreamEndpoint,
  ) => Promise<unknown | null>;
};

export type Endpoint = {
  name: string;
  subject: string;
  handler: (err: Error | null, msg: Msg) => void;
  queueGroup?: string;
};

export type JetStreamEndpoint = {
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

export type ServiceStatus = {
  name: string;
  status: ServiceStatusEntry | null;
  error: Error | null;
};

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
  statuses: Map<Endpoint | JetStreamEndpoint, ServiceStatusEntry>;
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
    this.statuses = new Map<ServiceSubscription, ServiceStatusEntry>();
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
    const status: ServiceStatusEntry = {
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

  status(internal = false): ServiceStatus[] {
    const statuses: ServiceStatus[] = [];

    const handlers = internal
      ? this.handlers.concat(this.internal)
      : this.handlers;
    for (let i = 0; i < handlers.length; i++) {
      const h = handlers[i];
      const status = this.statuses.get(h);
      let re;
      if (typeof this.config.statusHandler === "function") {
        try {
          status!.data = this.config.statusHandler(h as Endpoint);
        } catch (err) {
          re = err;
        }
      }
      const ss: ServiceStatus = {
        name: h.name,
        status: status ? status : null,
        error: re,
      };
      statuses.push(ss);
    }
    return statuses;
  }

  start(): Promise<Service> {
    const internal = [
      {
        name: "ping",
        subject: ServiceImpl.controlSubject(
          SrvVerb.PING,
          this.config.kind,
          this.config.id,
        ),
        handler: (err, msg) => {
          if (err) {
            this.close(err);
            return;
          }
          msg?.respond();
        },
      },
      {
        name: "ping-kind",
        subject: ServiceImpl.controlSubject(SrvVerb.PING, this.config.kind),
        handler: (err, msg) => {
          if (err) {
            this.close(err);
            return;
          }
          msg?.respond();
        },
      },
      {
        name: "ping-all",
        subject: ServiceImpl.controlSubject(SrvVerb.PING),
        handler: (err, msg) => {
          if (err) {
            this.close(err);
            return;
          }
          msg?.respond();
        },
      },
      {
        name: "status",
        subject: ServiceImpl.controlSubject(
          SrvVerb.STATUS,
          this.config.kind,
          this.config.id,
        ),
        handler: (err, msg) => {
          if (err) {
            this.close(err);
            return;
          }
          const status = this.status(true);
          msg?.respond(jc.encode(status));
        },
      },
      {
        name: "status-kind",
        subject: ServiceImpl.controlSubject(SrvVerb.STATUS, this.config.kind),
        handler: (err, msg) => {
          if (err) {
            this.close(err);
            return;
          }
          const status = this.status(true);
          msg?.respond(jc.encode(status));
        },
      },
      {
        name: "status-all",
        subject: ServiceImpl.controlSubject(SrvVerb.STATUS),
        handler: (err, msg) => {
          if (err) {
            this.close(err);
            return;
          }
          const status = this.status(true);
          msg?.respond(jc.encode(status));
        },
      },
    ] as Endpoint[];

    internal.forEach((e) => {
      this.setupNATS(e, true);
    });

    this.internal = internal as unknown as ServiceSubscription[];
    this.handlers.forEach((h) => {
      const { subject } = h as Endpoint;
      console.log(subject);
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

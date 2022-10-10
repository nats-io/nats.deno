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

export type Srv = {
  handlers: MsgSrvHandler[];
  status(internal: boolean): SrvStatus[];
  stop(): Promise<null | Error>;
  done: Promise<null | Error>;
  stopped: boolean;
  heartbeatInterval: number;
};

export type SrvStatusEntry = {
  requests: number;
  errors: number;
  lastError?: Error;
  data?: unknown;
};

export type SrvHandler = {
  name: string;
  description?: string;
  schema?: {
    request: string;
    response: string;
  };
  statusHandler?: (srv: SrvHandler) => Promise<unknown | null>;
};

export type MsgSrvHandler = {
  subject: string;
  handler: (err: Error | null, msg: Msg) => void;
  queueGroup?: string;
} & SrvHandler;

export type JetStreamSrvHandler = {
  consumer: Consumer;
  handler: (err: Error | null, msg: JsMsg) => void;
} & SrvHandler;

type SrvHandlerSub<T = unknown> = MsgSrvHandler & JetStreamSrvHandler & {
  internal: boolean;
  sub: Sub<T>;
};

export type SrvStatus = {
  name: string;
  status: SrvStatusEntry | null;
  error: Error | null;
};

export function addService(
  nc: NatsConnection,
  kind: string,
  id: string,
  handler:
    | MsgSrvHandler
    | JetStreamSrvHandler
    | (MsgSrvHandler | JetStreamSrvHandler)[],
): Promise<Srv> {
  const s = new SrvImpl(nc, kind, id, handler);
  try {
    return s.start();
  } catch (err) {
    return Promise.reject(err);
  }
}

const jc = JSONCodec();

export class SrvImpl implements Srv {
  nc: NatsConnection;
  kind: string;
  id: string;
  _done: Deferred<Error | null>;
  handlers: SrvHandlerSub[];
  internal: SrvHandlerSub[];
  _heartbeatInterval: number;
  stopped: boolean;
  watched: Promise<void>[];
  statuses: Map<SrvHandler, SrvStatusEntry>;
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
    kind: string,
    id: string,
    handler:
      | MsgSrvHandler
      | JetStreamSrvHandler
      | (MsgSrvHandler | JetStreamSrvHandler)[],
  ) {
    this.nc = nc;
    this.kind = kind.toUpperCase();
    this.id = id.toUpperCase();
    this.handlers = [handler] as SrvHandlerSub[];
    this.internal = [] as SrvHandlerSub[];
    this.watched = [];
    this._done = deferred();
    this.stopped = false;
    this.statuses = new Map<SrvHandlerSub, SrvStatusEntry>();
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
        `${SrvPrefix}.${SrvVerb.HEARTBEAT}.${this.kind}.${this.id}`,
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

  setupNATS(h: MsgSrvHandler, internal = false) {
    const { subject, handler, queueGroup: queue } = h as MsgSrvHandler;
    const sv = h as SrvHandlerSub;
    sv.internal = internal;
    const status: SrvStatusEntry = {
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

  status(internal = false): SrvStatus[] {
    const statuses: SrvStatus[] = [];

    const handlers = internal
      ? this.handlers.concat(this.internal)
      : this.handlers;
    for (let i = 0; i < handlers.length; i++) {
      const h = handlers[i];
      const status = this.statuses.get(h);
      let re;
      if (typeof h.statusHandler === "function") {
        try {
          status!.data = h.statusHandler(h as MsgSrvHandler);
        } catch (err) {
          re = err;
        }
      }
      const ss: SrvStatus = {
        name: h.name,
        status: status ? status : null,
        error: re,
      };
      statuses.push(ss);
    }
    return statuses;
  }

  start(): Promise<Srv> {
    const internal = [
      {
        name: "ping",
        subject: `${SrvPrefix}.${SrvVerb.PING}.${this.kind}.${this.id}`,
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
        subject: `${SrvPrefix}.${SrvVerb.PING}.${this.kind}`,
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
        subject: `${SrvPrefix}.${SrvVerb.PING}`,
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
        subject: `${SrvPrefix}.${SrvVerb.STATUS}.${this.kind}.${this.id}`,
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
        subject: `${SrvPrefix}.${SrvVerb.STATUS}.${this.kind}`,
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
        subject: `${SrvPrefix}.${SrvVerb.STATUS}`,
        handler: (err, msg) => {
          if (err) {
            this.close(err);
            return;
          }
          const status = this.status(true);
          msg?.respond(jc.encode(status));
        },
      },
    ] as MsgSrvHandler[];

    internal.forEach((e) => {
      this.setupNATS(e, true);
    });

    this.internal = internal as unknown as SrvHandlerSub[];
    this.handlers.forEach((h) => {
      const { subject } = h as MsgSrvHandler;
      if (typeof subject !== "string") {
        // this is a jetstream service
        return;
      }
      this.setupNATS(h as unknown as MsgSrvHandler);
    });

    this.handlers.forEach((h) => {
      const { consumer } = h as JetStreamSrvHandler;
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

/*
 * Copyright 2020 The NATS Authors
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
//@ts-ignore
import { NatsConnection } from "./nats.ts";
import { NatsError } from "./mod.ts";

export const CLOSE_EVT = "close";

export const DEFAULT_PORT = 4222;
export const DEFAULT_PRE = "nats://localhost:";
export const DEFAULT_URI = DEFAULT_PRE + DEFAULT_PORT;

// Reconnect Parameters, 2 sec wait, 10 tries
export const DEFAULT_RECONNECT_TIME_WAIT = 2 * 1000;
export const DEFAULT_MAX_RECONNECT_ATTEMPTS = 10;
export const DEFAULT_JITTER = 100;
export const DEFAULT_JITTER_TLS = 1000;

// Ping interval
export const DEFAULT_PING_INTERVAL = 2 * 60 * 1000; // 2 minutes
export const DEFAULT_MAX_PING_OUT = 2;

export interface ConnectFn {
  (opts: ConnectionOptions): Promise<NatsConnection>;
}

export enum Payload {
  STRING = "string",
  JSON = "json",
  BINARY = "binary",
}

export interface ConnectionOptions {
  debug?: boolean;
  name?: string;
  noEcho?: boolean;
  pass?: string;
  payload?: Payload;
  pedantic?: boolean;
  pingInterval?: number;
  timeout?: number;
  token?: string;
  url: string;
  user?: string;
  userJWT?: (() => string) | string;
  verbose?: boolean;

  maxPingOut?: number;
  maxReconnectAttempts?: number;
  noRandomize?: boolean;
  port?: number;
  reconnect?: boolean;
  reconnectTimeWait?: number;
  reconnectJitter?: number;
  reconnectJitterTLS?: number;
  reconnectDelayHandler?: () => number;
  servers?: Array<string>;
  tls?: boolean | TlsOptions;
  waitOnFirstConnect?: boolean;
  nonceSigner?: (nonce: string) => Uint8Array;
  nkey?: string;
  userCreds?: string;
  nkeyCreds?: string;
}

export interface TlsOptions {
  certFile?: string;
  // these may not be supported on all environments
  caFile?: string;
  keyFile?: string;
}

export interface Msg {
  subject: string;
  sid: number;
  reply?: string;
  data?: any;

  respond(data?: any): void;
}

export interface SubscriptionOptions {
  queue?: string;
  max?: number;
}

export interface Base {
  subject: string;
  callback: (error: NatsError | null, msg: Msg) => void;
  received: number;
  timeout?: number | null;
  max?: number | undefined;
  draining: boolean;
}

export interface Req extends Base {
  token: string;
}

export interface Sub extends Base {
  sid: number;
  queue?: string | null;
}

export function defaultSub(): Sub {
  return { sid: 0, subject: "", received: 0 } as Sub;
}

export function defaultReq(): Req {
  return { token: "", subject: "", received: 0, max: 1 } as Req;
}

export interface ServerInfo {
  tls_required?: boolean;
  tls_verify?: boolean;
  connect_urls?: string[];
  max_payload: number;
  client_id: number;
  proto: number;
  server_id: string;
  version: string;
  echo?: boolean;
  nonce?: string;
  nkey?: string;
}

export interface ServersChangedEvent {
  added: string[];
  deleted: string[];
}

/*
 * Copyright 2020-2023 The NATS Authors
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
export { NatsConnectionImpl } from "./nats.ts";
export { Nuid, nuid } from "./nuid.ts";

export type { ServiceClient, TypedSubscriptionOptions } from "./types.ts";

export { MsgImpl } from "./msg.ts";
export { setTransportFactory } from "./transport.ts";
export type { Transport, TransportFactory } from "./transport.ts";
export { Connect, INFO, ProtocolHandler } from "./protocol.ts";
export type { Backoff, Deferred, Perf, Timeout } from "./util.ts";
export {
  backoff,
  collect,
  deadline,
  deferred,
  delay,
  extend,
  render,
  timeout,
} from "./util.ts";
export { canonicalMIMEHeaderKey, headers, MsgHdrsImpl } from "./headers.ts";
export { Heartbeat } from "./heartbeats.ts";
export type { PH } from "./heartbeats.ts";
export { MuxSubscription } from "./muxsubscription.ts";
export { DataBuffer } from "./databuffer.ts";
export {
  buildAuthenticator,
  checkOptions,
  checkUnsupportedOption,
} from "./options.ts";
export { RequestOne } from "./request.ts";
export {
  credsAuthenticator,
  jwtAuthenticator,
  nkeyAuthenticator,
  tokenAuthenticator,
  usernamePasswordAuthenticator,
} from "./authenticator.ts";
export type { Codec } from "./codec.ts";
export { JSONCodec, StringCodec } from "./codec.ts";
export * from "./nkeys.ts";
export type {
  DispatchedFn,
  IngestionFilterFn,
  IngestionFilterFnResult,
  ProtocolFilterFn,
} from "./queued_iterator.ts";
export { QueuedIteratorImpl } from "./queued_iterator.ts";
export type { ParserEvent } from "./parser.ts";
export { Kind, Parser, State } from "./parser.ts";
export { DenoBuffer, MAX_SIZE, readAll, writeAll } from "./denobuffer.ts";
export { Bench, Metric } from "./bench.ts";
export type { BenchOpts } from "./bench.ts";
export { TD, TE } from "./encoders.ts";
export { isIP, parseIP } from "./ipparser.ts";
export { TypedSubscription } from "./typedsub.ts";
export type { MsgAdapter, TypedCallback } from "./typedsub.ts";
export {
  Base64KeyCodec,
  Bucket,
  defaultBucketOpts,
  NoopKvCodecs,
} from "../jetstream/kv.ts";

export type { SemVer } from "./semver.ts";

export { compare, parseSemVer } from "./semver.ts";

export { Empty } from "./types.ts";
export { extractProtocolMessage } from "./transport.ts";

export type {
  ApiError,
  Auth,
  Authenticator,
  ConnectionOptions,
  Dispatcher,
  Endpoint,
  EndpointInfo,
  EndpointOptions,
  EndpointStats,
  JwtAuth,
  Msg,
  MsgHdrs,
  NamedEndpointStats,
  Nanos,
  NatsConnection,
  NKeyAuth,
  NoAuth,
  Payload,
  PublishOptions,
  QueuedIterator,
  Request,
  RequestManyOptions,
  RequestOptions,
  ReviverFn,
  Server,
  ServerInfo,
  ServersChanged,
  Service,
  ServiceConfig,
  ServiceGroup,
  ServiceHandler,
  ServiceIdentity,
  ServiceInfo,
  ServiceMetadata,
  ServiceMsg,
  ServiceResponse,
  ServicesAPI,
  ServiceStats,
  Stats,
  Status,
  Sub,
  SubOpts,
  Subscription,
  SubscriptionOptions,
  SyncIterator,
  TlsOptions,
  TokenAuth,
  UserPass,
} from "./core.ts";
export {
  createInbox,
  DebugEvents,
  ErrorCode,
  Events,
  isNatsError,
  Match,
  NatsError,
  RequestStrategy,
  ServiceError,
  ServiceErrorCodeHeader,
  ServiceErrorHeader,
  ServiceResponseType,
  ServiceVerb,
  syncIterator,
} from "./core.ts";
export { SubscriptionImpl, Subscriptions } from "./protocol.ts";

/*
 * Copyright 2023 The NATS Authors
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

import { JetStreamClient, JetStreamManager } from "../jetstream/types.ts";
import { nuid } from "./nuid.ts";

/**
 * Events reported by the {@link NatsConnection#status} iterator.
 */
export enum Events {
  /** Client disconnected */
  Disconnect = "disconnect",
  /** Client reconnected */
  Reconnect = "reconnect",
  /** Client received a cluster update */
  Update = "update",
  /** Client received a signal telling it that the server is transitioning to Lame Duck Mode */
  LDM = "ldm",
  /** Client received an async error from the server */
  Error = "error",
}

/**
 * Other events that can be reported by the {@link NatsConnection#status} iterator.
 * These can usually be safely ignored, as higher-order functionality of the client
 * will handle them.
 */
export enum DebugEvents {
  Reconnecting = "reconnecting",
  PingTimer = "pingTimer",
  StaleConnection = "staleConnection",
}

export enum ErrorCode {
  // emitted by the client
  ApiError = "BAD API",
  BadAuthentication = "BAD_AUTHENTICATION",
  BadCreds = "BAD_CREDS",
  BadHeader = "BAD_HEADER",
  BadJson = "BAD_JSON",
  BadPayload = "BAD_PAYLOAD",
  BadSubject = "BAD_SUBJECT",
  Cancelled = "CANCELLED",
  ConnectionClosed = "CONNECTION_CLOSED",
  ConnectionDraining = "CONNECTION_DRAINING",
  ConnectionRefused = "CONNECTION_REFUSED",
  ConnectionTimeout = "CONNECTION_TIMEOUT",
  Disconnect = "DISCONNECT",
  InvalidOption = "INVALID_OPTION",
  InvalidPayload = "INVALID_PAYLOAD",
  MaxPayloadExceeded = "MAX_PAYLOAD_EXCEEDED",
  NoResponders = "503",
  NotFunction = "NOT_FUNC",
  RequestError = "REQUEST_ERROR",
  ServerOptionNotAvailable = "SERVER_OPT_NA",
  SubClosed = "SUB_CLOSED",
  SubDraining = "SUB_DRAINING",
  Timeout = "TIMEOUT",
  Tls = "TLS",
  Unknown = "UNKNOWN_ERROR",
  WssRequired = "WSS_REQUIRED",

  // jetstream
  JetStreamInvalidAck = "JESTREAM_INVALID_ACK",
  JetStream404NoMessages = "404",
  JetStream408RequestTimeout = "408",
  //@deprecated: use JetStream409
  JetStream409MaxAckPendingExceeded = "409",
  JetStream409 = "409",
  JetStreamNotEnabled = "503",
  JetStreamIdleHeartBeat = "IDLE_HEARTBEAT",

  // emitted by the server
  AuthorizationViolation = "AUTHORIZATION_VIOLATION",
  AuthenticationExpired = "AUTHENTICATION_EXPIRED",
  ProtocolError = "NATS_PROTOCOL_ERR",
  PermissionsViolation = "PERMISSIONS_VIOLATION",
  AuthenticationTimeout = "AUTHENTICATION_TIMEOUT",
}

export function isNatsError(err: NatsError | Error): err is NatsError {
  return typeof (err as NatsError).code === "string";
}

export interface ApiError {
  /**
   * HTTP like error code in the 300 to 500 range
   */
  code: number;
  /**
   * A human friendly description of the error
   */
  description: string;
  /**
   * The NATS error code unique to each kind of error
   */
  err_code?: number;
}

export class Messages {
  messages: Map<string, string>;

  constructor() {
    this.messages = new Map<string, string>();
    this.messages.set(
      ErrorCode.InvalidPayload,
      "Invalid payload type - payloads can be 'binary', 'string', or 'json'",
    );
    this.messages.set(ErrorCode.BadJson, "Bad JSON");
    this.messages.set(
      ErrorCode.WssRequired,
      "TLS is required, therefore a secure websocket connection is also required",
    );
  }

  static getMessage(s: string): string {
    return messages.getMessage(s);
  }

  getMessage(s: string): string {
    return this.messages.get(s) || s;
  }
}

// safari doesn't support static class members
const messages: Messages = new Messages();

export class NatsError extends Error {
  name: string;
  message: string;
  // TODO: on major version this should change to a number/enum
  code: string;
  permissionContext?: { operation: string; subject: string };
  chainedError?: Error;
  // these are for supporting jetstream
  api_error?: ApiError;

  /**
   * @param {String} message
   * @param {String} code
   * @param {Error} [chainedError]
   * @constructor
   *
   * @api private
   */
  constructor(message: string, code: string, chainedError?: Error) {
    super(message);
    this.name = "NatsError";
    this.message = message;
    this.code = code;
    this.chainedError = chainedError;
  }

  static errorForCode(code: string, chainedError?: Error): NatsError {
    const m = Messages.getMessage(code);
    return new NatsError(m, code, chainedError);
  }

  isAuthError(): boolean {
    return this.code === ErrorCode.AuthenticationExpired ||
      this.code === ErrorCode.AuthorizationViolation;
  }

  isAuthTimeout(): boolean {
    return this.code === ErrorCode.AuthenticationTimeout;
  }

  isPermissionError(): boolean {
    return this.code === ErrorCode.PermissionsViolation;
  }

  isProtocolError(): boolean {
    return this.code === ErrorCode.ProtocolError;
  }

  isJetStreamError(): boolean {
    return this.api_error !== undefined;
  }

  jsError(): ApiError | null {
    return this.api_error ? this.api_error : null;
  }
}

/**
 * Subscription Options
 */
export interface SubOpts<T> {
  /**
   * Optional queue name (subscriptions on the same subject that use queues
   * are horizontally load balanced when part of the same queue).
   */
  queue?: string;
  /**
   * Optional maximum number of messages to deliver to the subscription
   * before it is auto-unsubscribed.
   */
  max?: number;
  /**
   * Optional maximum number of milliseconds before a timer raises an error. This
   * useful to monitor a subscription that is expected to yield messages.
   * The timer is cancelled when the first message is received by the subscription.
   */
  timeout?: number;
  /**
   * An optional function that will handle messages. Typically, messages
   * are processed via an async iterator on the subscription. If this
   * option is provided, messages are processed by the specified function.
   * @param err
   * @param msg
   */
  callback?: (err: NatsError | null, msg: T) => void;
}

export interface DnsResolveFn {
  (h: string): Promise<string[]>;
}

export interface Status {
  type: Events | DebugEvents;
  data: string | ServersChanged | number;
  permissionContext?: { operation: string; subject: string };
}

/**
 * Subscription Options
 */
export type SubscriptionOptions = SubOpts<Msg>;

/**
 * ServerInfo represents information from the connected server
 */
export interface ServerInfo {
  /**
   * True if the server requires authentication
   */
  "auth_required"?: boolean;
  /**
   * Server-assigned client_id
   */
  "client_id": number;
  /**
   * The client's IP as seen by the server
   */
  "client_ip"?: string;
  /**
   * The name or ID of the cluster
   */
  cluster?: string;
  /**
   * Other servers available on the connected cluster
   */
  "connect_urls"?: string[];
  /**
   * Git commit information on the built server binary
   */
  "git_commit"?: string;
  /**
   * Version information on the Go stack used to build the server binary
   */
  go: string;
  /**
   * True if the server supports headers
   */
  headers?: boolean;
  /**
   * Hostname of the connected server
   */
  host: string;
  /**
   * True if the server supports JetStream
   */
  jetstream?: boolean;
  /**
   * True if the server is in Lame Duck Mode
   */
  ldm?: boolean;
  /**
   * Max number of bytes in message that can be sent to the server
   */
  "max_payload": number;
  /**
   * If the server required nkey or JWT authentication the nonce used during authentication.
   */
  nonce?: string;
  /**
   * The port where the server is listening
   */
  port: number;
  /**
   * Version number of the NATS protocol
   */
  proto: number;
  /**
   * The ID of the server
   */
  "server_id": string;
  /**
   * The name of the server
   */
  "server_name": string;
  /**
   * True if TLS is available
   */
  "tls_available"?: boolean;
  /**
   * True if TLS connections are required
   */
  "tls_required"?: boolean;
  /**
   * True if TLS client certificate verification is required
   */
  "tls_verify"?: boolean;
  /**
   * The nats-server version
   */
  version: string;
}

export interface Server {
  hostname: string;
  port: number;
  listen: string;
  src: string;
  tlsName: string;

  resolve(
    opts: Partial<{ fn: DnsResolveFn; randomize: boolean; debug?: boolean }>,
  ): Promise<Server[]>;
}

/**
 * ServerChanged records servers in the cluster that were added or deleted.
 */
export interface ServersChanged {
  /** list of added servers */
  readonly added: string[];
  /** list of deleted servers */
  readonly deleted: string[];
}

/**
 * Type alias for NATS core subscriptions
 */
export type Subscription = Sub<Msg>;

export enum Match {
  // Exact option is case sensitive
  Exact = 0,
  // Case sensitive, but key is transformed to Canonical MIME representation
  CanonicalMIME,
  // Case insensitive matches
  IgnoreCase,
}

export interface MsgHdrs extends Iterable<[string, string[]]> {
  hasError: boolean;
  status: string;
  code: number;
  description: string;

  get(k: string, match?: Match): string;

  set(k: string, v: string, match?: Match): void;

  append(k: string, v: string, match?: Match): void;

  has(k: string, match?: Match): boolean;

  keys(): string[];

  values(k: string, match?: Match): string[];

  delete(k: string, match?: Match): void;
}

export interface RequestOptions {
  /**
   * number of milliseconds before the request will timeout.
   */
  timeout: number;
  /**
   * MsgHdrs to include with the request.
   */
  headers?: MsgHdrs;
  /**
   * If true, the request API will create a regular NATS subscription
   * to process the response. Otherwise a shared muxed subscriptions is
   * used. Requires {@link reply}
   */
  noMux?: boolean;
  /**
   * The subject where the response should be sent to. Requires {@link noMux}
   */
  reply?: string;
}

export enum RequestStrategy {
  Timer = "timer",
  Count = "count",
  JitterTimer = "jitterTimer",
  SentinelMsg = "sentinelMsg",
}

export interface RequestManyOptions {
  strategy: RequestStrategy;
  maxWait: number;
  headers?: MsgHdrs;
  maxMessages?: number;
  noMux?: boolean;
  jitter?: number;
}

export interface Stats {
  /**
   * Number of bytes received by the client.
   */
  inBytes: number;
  /**
   * Number of bytes sent by the client.
   */
  outBytes: number;
  /**
   * Number of messages received by the client.
   */
  inMsgs: number;
  /**
   * Number of messages sent by the client.
   */
  outMsgs: number;
}

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
   * Requests info from services
   * @param name
   * @param id
   */
  info(name?: string, id?: string): Promise<QueuedIterator<ServiceInfo>>;
}

export interface ServicesAPI {
  /**
   * Adds a {@link Service}
   * @param config
   */
  add(config: ServiceConfig): Promise<Service>;

  /**
   * Returns a {@link ServiceClient} that can be used to interact with the
   * observability aspects of the service (discovery, monitoring)
   * @param opts
   * @param prefix
   */
  client(opts?: RequestManyOptions, prefix?: string): ServiceClient;
}

/**
 * Options to a JetStream options applied to all  JetStream or JetStreamManager requests.
 */
export interface JetStreamOptions {
  /**
   * Prefix required to interact with JetStream. Must match
   * server configuration.
   */
  apiPrefix?: string;
  /**
   * Number of milliseconds to wait for a JetStream API request.
   * @default ConnectionOptions.timeout
   * @see ConnectionOptions.timeout
   */
  timeout?: number;
  /**
   * Name of the JetStream domain. This value automatically modifies
   * the default JetStream apiPrefix.
   */
  domain?: string;
}

export interface JetStreamManagerOptions extends JetStreamOptions {
  /**
   * Allows disabling a check on the account for JetStream enablement see
   * {@link JetStreamManager.getAccountInfo()}.
   */
  checkAPI?: boolean;
}

export type Payload = Uint8Array | string;

export interface NatsConnection {
  /**
   * ServerInfo to the currently connected server or undefined
   */
  info?: ServerInfo;

  /**
   * Returns a promise that can be used to monitor if the client closes.
   * The promise can resolve an Error if the reason for the close was
   * an error. Note that the promise doesn't reject, but rather resolves
   * to the error if there was one.
   */
  closed(): Promise<void | Error>;

  /**
   * Close will close the connection to the server. This call will terminate
   * all pending requests and subscriptions. The returned promise resolves when
   * the connection closes.
   */
  close(): Promise<void>;

  /**
   * Publishes the specified data to the specified subject.
   * @param subject
   * @param payload
   * @param options
   */
  publish(
    subject: string,
    payload?: Payload,
    options?: PublishOptions,
  ): void;

  /**
   * Subscribe expresses interest in the specified subject. The subject may
   * have wildcards. Messages are delivered to the {@link SubOpts#callback |SubscriptionOptions callback}
   * if specified. Otherwise, the subscription is an async iterator for {@link Msg}.
   *
   * @param subject
   * @param opts
   */
  subscribe(subject: string, opts?: SubscriptionOptions): Subscription;

  /**
   * Publishes a request with specified data in the specified subject expecting a
   * response before {@link RequestOptions#timeout} milliseconds. The api returns a
   * Promise that resolves when the first response to the request is received. If
   * there are no responders (a subscription) listening on the request subject,
   * the request will fail as soon as the server processes it.
   *
   * @param subject
   * @param payload
   * @param opts
   */
  request(
    subject: string,
    payload?: Payload,
    opts?: RequestOptions,
  ): Promise<Msg>;

  /**
   * Publishes a request expecting multiple responses back. Several strategies
   * to determine when the request should stop gathering responses.
   * @param subject
   * @param payload
   * @param opts
   */
  requestMany(
    subject: string,
    payload?: Payload,
    opts?: Partial<RequestManyOptions>,
  ): Promise<AsyncIterable<Msg>>;

  /**
   * Returns a Promise that resolves when the client receives a reply from
   * the server. Use of this API is not necessary by clients.
   */
  flush(): Promise<void>;

  /**
   * Initiates a drain on the connection and returns a promise that resolves when the
   * drain completes and the connection closes.
   *
   * Drain is an ordered shutdown of the client. Instead of abruptly closing the client,
   * subscriptions are drained, that is messages not yet processed by a subscription are
   * handled before the subscription is closed. After subscriptions are drained it is not
   * possible to create a new subscription. Then all pending outbound messages are
   * sent to the server. Finally, the connection is closed.
   */
  drain(): Promise<void>;

  /**
   * Returns true if the client is closed.
   */
  isClosed(): boolean;

  /**
   * Returns true if the client is draining.
   */
  isDraining(): boolean;

  /**
   * Returns the hostport of the server the client is connected to.
   */
  getServer(): string;

  /**
   * Returns an async iterator of {@link Status} that may be
   * useful in understanding when the client looses a connection, or
   * reconnects, or receives an update from the cluster among other things.
   *
   * @return an AsyncIterable<Status>
   */
  status(): AsyncIterable<Status>;

  /**
   * Returns some metrics such as the number of messages and bytes
   * sent and recieved by the client.
   */
  stats(): Stats;

  /**
   * Returns a Promise to a {@link JetStreamManager} which allows the client
   * to access Streams and Consumers information.
   *
   * @param opts
   */
  jetstreamManager(opts?: JetStreamManagerOptions): Promise<JetStreamManager>;

  /**
   * Returns a {@link JetStreamClient} which allows publishing messages to
   * JetStream or consuming messages from streams.
   *
   * @param opts
   */
  jetstream(opts?: JetStreamOptions): JetStreamClient;

  /**
   * @return the number of milliseconds it took for a {@link flush}.
   */
  rtt(): Promise<number>;

  services: ServicesAPI;
}

/**
 * A reviver function
 */
//deno-lint-ignore no-explicit-any
export type ReviverFn = (key: string, value: any) => any;

/**
 * Represents a message delivered by NATS. This interface is used by
 * Subscribers.
 */
export interface Msg {
  /**
   * The subject the message was sent to
   */
  subject: string;
  /**
   * The subscription ID where the message was dispatched.
   */
  sid: number;
  /**
   * A possible subject where the recipient may publish a reply (in the cases
   * where the message represents a request).
   */
  reply?: string;
  /**
   * The message's data (or payload)
   */
  data: Uint8Array;
  /**
   * Possible headers that may have been set by the server or the publisher.
   */
  headers?: MsgHdrs;

  /**
   * Convenience to publish a response to the {@link reply} subject in the
   * message - this is the same as doing `nc.publish(msg.reply, ...)`.
   * @param payload
   * @param opts
   */
  respond(payload?: Payload, opts?: PublishOptions): boolean;

  /**
   * Convenience method to parse the message payload as JSON. This method
   * will throw an exception if there's a parsing error;
   * @param reviver a reviver function
   */
  json<T>(reviver?: ReviverFn): T;

  /**
   * Convenience method to parse the message payload as string. This method
   * may throw an exception if there's a conversion error
   */
  string(): string;
}

export type SyncIterator<T> = {
  next(): Promise<T | null>;
};

/**
 * syncIterator is a utility function that allows an AsyncIterator to be triggered
 * by calling next() - the utility will yield null if the underlying iterator is closed.
 * Note it is possibly an error to call use this function on an AsyncIterable that has
 * already been started (Symbol.asyncIterator() has been called) from a looping construct.
 */
export function syncIterator<T>(src: AsyncIterable<T>): SyncIterator<T> {
  const iter = src[Symbol.asyncIterator]();
  return {
    async next(): Promise<T | null> {
      const m = await iter.next();
      if (m.done) {
        return Promise.resolve(null);
      }
      return Promise.resolve(m.value);
    },
  };
}

/**
 * Basic interface to a Subscription type
 */
export interface Sub<T> extends AsyncIterable<T> {
  /** A promise that resolves when the subscription closes */
  closed: Promise<void>;

  /**
   * Stop the subscription from receiving messages. You can optionally
   * specify that the subscription should stop after the specified number
   * of messages have been received. Note this count is since the lifetime
   * of the subscription.
   * @param max
   */
  unsubscribe(max?: number): void;

  /**
   * Drain the subscription, closing it after processing all messages
   * currently in flight for the client. Returns a promise that resolves
   * when the subscription finished draining.
   */
  drain(): Promise<void>;

  /**
   * Returns true if the subscription is draining.
   */
  isDraining(): boolean;

  /**
   * Returns true if the subscription is closed.
   */
  isClosed(): boolean;

  /**
   * @ignore
   */
  callback(err: NatsError | null, msg: Msg): void;

  /**
   * Returns the subject that was used to create the subscription.
   */
  getSubject(): string;

  /**
   * Returns the number of messages received by the subscription.
   */
  getReceived(): number;

  /**
   * Returns the number of messages that have been processed by the subscription.
   */
  getProcessed(): number;

  /**
   * Returns the number of messages that are pending processing. Note that this
   * is method is only valid for iterators.
   */
  getPending(): number;

  /** @ignore */
  getID(): number;

  /**
   * Return the max number of messages before the subscription will unsubscribe.
   */
  getMax(): number | undefined;
}

export interface PublishOptions {
  /**
   * An optional subject where a response should be sent.
   * Note you must have a subscription listening on this subject
   * to receive the response.
   */
  reply?: string;
  /**
   * Optional headers to include with the message.
   */
  headers?: MsgHdrs;
}

// JetStream Server Types
/**
 * Value expressed as Nanoseconds - use the `nanos(millis)` function
 * to convert millis to nanoseconds. Note that in some environments this
 * could overflow.
 */
export type Nanos = number;

export interface ServiceMsg extends Msg {
  respondError(
    code: number,
    description: string,
    data?: Uint8Array,
    opts?: PublishOptions,
  ): boolean;
}

export type ServiceHandler = (err: NatsError | null, msg: ServiceMsg) => void;

/**
 * A service Endpoint
 */
export type Endpoint = {
  /**
   * Subject where the endpoint listens
   */
  subject: string;
  /**
   * An optional handler - if not set the service is an iterator
   * @param err
   * @param msg
   */
  handler?: ServiceHandler;
  /**
   * Optional metadata about the endpoint
   */
  metadata?: Record<string, string>;
  /**
   * Optional queue group to run this particular endpoint in. The service's configuration
   * queue configuration will be used. See {@link ServiceConfig}.
   */
  queue?: string;
};
export type EndpointOptions = Partial<Endpoint>;

export type EndpointInfo = {
  name: string;
  subject: string;
  metadata?: Record<string, string>;
  queue_group?: string;
};

export interface Dispatcher<T> {
  push(v: T): void;
}

export interface QueuedIterator<T> extends Dispatcher<T> {
  [Symbol.asyncIterator](): AsyncIterator<T>;

  stop(err?: Error): void;

  getProcessed(): number;

  getPending(): number;

  getReceived(): number;
}

export interface ServiceGroup {
  /**
   * The name of the endpoint must be a simple subject token with no wildcards
   * @param name
   * @param opts is either a handler or a more complex options which allows a
   *  subject, handler, and/or schema
   */
  addEndpoint(
    name: string,
    opts?: ServiceHandler | EndpointOptions,
  ): QueuedIterator<ServiceMsg>;

  /**
   * A group is a subject prefix from which endpoints can be added.
   * Can be empty to allow for prefixes or tokens that are set at runtime
   * without requiring editing of the service.
   * Note that an optional queue can be specified, all endpoints added to
   * the group, will use the specified queue unless the endpoint overrides it.
   * see {@link EndpointOptions} and {@link ServiceConfig}.
   * @param subject
   * @param queue
   */
  addGroup(subject?: string, queue?: string): ServiceGroup;
}

export type ServiceMetadata = {
  metadata?: Record<string, string>;
};

export enum ServiceResponseType {
  STATS = "io.nats.micro.v1.stats_response",
  INFO = "io.nats.micro.v1.info_response",
  PING = "io.nats.micro.v1.ping_response",
}

export interface ServiceResponse {
  /**
   * Response type schema
   */
  type: ServiceResponseType;
}

export type ServiceIdentity = ServiceResponse & ServiceMetadata & {
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
};
export type NamedEndpointStats = {
  /**
   * The name of the endpoint
   */
  name: string;
  /**
   * The subject the endpoint is listening on
   */
  subject: string;
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
   * The queue group the endpoint is listening on
   */
  queue_group?: string;
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
export type ServiceInfo = ServiceIdentity & {
  /**
   * Description for the service
   */
  description: string;
  /**
   * Service metadata
   */
  metadata?: Record<string, string>;
  /**
   * Information about the Endpoints
   */
  endpoints: EndpointInfo[];
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
   * A customized handler for the stats of an endpoint. The
   * data returned by the endpoint will be serialized as is
   * @param endpoint
   */
  statsHandler?: (
    endpoint: Endpoint,
  ) => Promise<unknown | null>;

  /**
   * Optional metadata about the service
   */
  metadata?: Record<string, string>;
  /**
   * Optional queue group to run the service in. By default,
   * then queue name is "q". Note that this configuration will
   * be the default for all endpoints and groups.
   */
  queue?: string;
};
/**
 * The stats of a service
 */
export type ServiceStats = ServiceIdentity & EndpointStats;

export interface Service extends ServiceGroup {
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
   * Returns the identity used by this service
   */
  ping(): ServiceIdentity;

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

export const ServiceErrorHeader = "Nats-Service-Error";
export const ServiceErrorCodeHeader = "Nats-Service-Error-Code";

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

export interface Publisher {
  publish(
    subject: string,
    data: Payload,
    options?: { reply?: string; headers?: MsgHdrs },
  ): void;
}

export function createInbox(prefix = ""): string {
  prefix = prefix || "_INBOX";
  if (typeof prefix !== "string") {
    throw (new Error("prefix must be a string"));
  }
  prefix.split(".")
    .forEach((v) => {
      if (v === "*" || v === ">") {
        throw new Error(`inbox prefixes cannot have wildcards '${prefix}'`);
      }
    });
  return `${prefix}.${nuid.next()}`;
}

export interface Request {
  token: string;
  requestSubject: string;
  received: number;

  resolver(err: Error | null, msg: Msg): void;

  cancel(err?: NatsError): void;
}

/**
 * @type {}
 */
export type NoAuth = void;

/**
 * @type {auth_token: string} the user token
 */
export interface TokenAuth {
  "auth_token": string;
}

/**
 * @type {user: string, pass?: string} the username and
 * optional password if the server requires.
 */
export interface UserPass {
  user: string;
  pass?: string;
}

/**
 * @type {nkey: string, sig: string} the public nkey for the user,
 * and a base64 encoded string for the calculated signature of the
 * challenge nonce.
 */
export interface NKeyAuth {
  nkey: string;
  sig: string;
}

/**
 * @type {jwt: string, nkey?: string, sig?: string} the user JWT,
 * and if not a bearer token also the public nkey for the user,
 * and a base64 encoded string for the calculated signature of the
 * challenge nonce.
 */
export interface JwtAuth {
  jwt: string;
  nkey?: string;
  sig?: string;
}

/**
 * @type NoAuth|TokenAuth|UserPass|NKeyAuth|JwtAuth
 */
export type Auth = NoAuth | TokenAuth | UserPass | NKeyAuth | JwtAuth;

/**
 * Authenticator is an interface that returns credentials.
 * @type function(nonce?: string) => Auth
 */
export interface Authenticator {
  (nonce?: string): Auth;
}

export interface ConnectionOptions {
  /**
   * When the server requires authentication, set an {@link Authenticator}.
   * An authenticator is created automatically for username/password and token
   * authentication configurations
   * if {@link user} and {@link pass} or the {@link token} options are set.
   */
  authenticator?: Authenticator | Authenticator[];
  /**
   * When set to `true` the client will print protocol messages that it receives
   * or sends to the server.
   */
  debug?: boolean;
  /**
   * Sets the maximum count of ping commands that can be awaiting a response
   * before rasing a stale connection status {@link DebugEvents#StaleConnection }
   * notification {@link NatsConnection#status} and initiating a reconnect.
   *
   * @see pingInterval
   */
  maxPingOut?: number;
  /**
   * Sets the maximum count of per-server reconnect attempts before giving up.
   * Set to `-1` to never give up.
   *
   * @default 10
   */
  maxReconnectAttempts?: number;
  /**
   * Sets the client name. When set, the server monitoring pages will display
   * this name when referring to this client.
   */
  name?: string;
  /**
   * When set to true, messages published by this client will not match
   * this client's subscriptions, so the client is guaranteed to never
   * receive self-published messages on a subject that it is listening on.
   */
  noEcho?: boolean;
  /**
   * If set to true, the client will not randomize its server connection list.
   */
  noRandomize?: boolean;
  /**
   * Sets the password for a client connection. Requires that the {@link user}
   * option be set. See {@link authenticator}.
   */
  pass?: string;
  /**
   * When set to true, the server may perform additional checks on protocol
   * message requests. This option is only useful for NATS client development
   * and shouldn't be used, as it affects server performance.
   */
  pedantic?: boolean;
  /**
   * Sets the number of milliseconds between client initiated ping commands.
   * See {@link maxPingOut}.
   * @default 2 minutes.
   */
  pingInterval?: number;
  /**
   * Sets the port number on the localhost (127.0.0.1) where the nats-server is running.
   * This option is mutually exclusive with {@link servers}.
   */
  port?: number;
  /**
   * When set to true, the server will attempt to reconnect so long as
   * {@link maxReconnectAttempts} doesn't prevent it.
   * @default true
   */
  reconnect?: boolean;
  /**
   * Set a function that dynamically determines the number of milliseconds
   * that the client should wait for the next reconnect attempt.
   */
  reconnectDelayHandler?: () => number;
  /**
   * Set the upper bound for a random delay in milliseconds added to
   * {@link reconnectTimeWait}.
   *
   * @default 100 millis
   */
  reconnectJitter?: number;
  /**
   * Set the upper bound for a random delay in milliseconds added to
   * {@link reconnectTimeWait}. This only affects TLS connections
   *
   * @default 1000 millis
   */
  reconnectJitterTLS?: number;
  /**
   * Set the number of millisecods between reconnect attempts.
   *
   * @default 2000 millis
   */
  reconnectTimeWait?: number;
  /**
   * Set the hostport(s) where the client should attempt to connect.
   * This option is mutually exclusive with {@link port}.
   *
   * @default 127.0.0.1:4222
   */
  servers?: Array<string> | string;
  /**
   * Sets the number of milliseconds the client should wait for a server
   * handshake to be established.
   *
   * @default 20000 millis
   */
  timeout?: number;
  /**
   * When set (can be an empty object), the client requires a secure connection.
   * TlsOptions honored depend on the runtime. Consult the specific NATS javascript
   * client GitHub repo/documentation. When set to null, the client should fail
   * should not connect using TLS. In the case where TLS is available on the server
   * a standard connection will be used. If TLS is required, the connection will fail.
   */
  tls?: TlsOptions | null;
  /**
   * Set to a client authentication token. Note that these tokens are
   * a specific authentication strategy on the nats-server. This option
   * is mutually exclusive of {@link user} and {@link pass}. See {@link authenticator}.
   */
  token?: string;
  /**
   * Sets the username for a client connection. Requires that the {@link pass}
   * option be set. See {@link authenticator}.
   */
  user?: string;
  /**
   * When set to true, the server will send response to all server commands.
   * This option is only useful for NATS client development and shouldn't
   * be used, as it affects server performance.
   */
  verbose?: boolean;
  /**
   * When set to true {@link maxReconnectAttempts} will not trigger until the client
   * has established one connection.
   */
  waitOnFirstConnect?: boolean;
  /**
   * When set to true, cluster information gossiped by the nats-server will
   * not augment the lists of server(s) known by the client.
   */
  ignoreClusterUpdates?: boolean;
  /**
   * A string prefix (must be a valid subject prefix) prepended to inboxes
   * generated by client. This allows a client with limited subject permissions
   * to specify a subject where requests can deliver responses.
   */
  inboxPrefix?: string;

  /**
   * By default, NATS clients will abort reconnect if they fail authentication
   * twice in a row with the same error, regardless of the reconnect policy.
   * This option should be used with care as it will disable this behaviour when true
   */
  ignoreAuthErrorAbort?: boolean;
}

/**
 * TlsOptions that can be specified to a client. Note that
 * the options are typically runtime specific, so some clients won't support
 * them at all. In other cases they will match to the runtime's TLS options.
 *
 * If no options are specified, but the argument for TlsOptions is an object,
 * the client is requesting to only use connections that are secured by TLS.
 */
export interface TlsOptions {
  certFile?: string;
  cert?: string;
  caFile?: string;
  ca?: string;
  keyFile?: string;
  key?: string;
}

export const DEFAULT_PORT = 4222;
export const DEFAULT_HOST = "127.0.0.1";

export interface ConnectFn {
  (opts: ConnectionOptions): Promise<NatsConnection>;
}

export interface Base {
  subject: string;
  callback: (error: NatsError | null, msg: Msg) => void;
  received: number;
  timeout?: number | null;
  max?: number | undefined;
  draining: boolean;
}

export interface URLParseFn {
  (u: string): string;
}

export enum ServiceVerb {
  PING = "PING",
  STATS = "STATS",
  INFO = "INFO",
}

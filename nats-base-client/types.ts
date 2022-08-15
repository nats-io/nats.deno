/*
 * Copyright 2020-2022 The NATS Authors
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
import { NatsError } from "./error.ts";
import type { MsgHdrs } from "./headers.ts";
import type { Authenticator } from "./authenticator.ts";
import { TypedSubscriptionOptions } from "./typedsub.ts";
import { QueuedIterator } from "./queued_iterator.ts";

export const Empty = new Uint8Array(0);

/**
 * Events reported by the {@link NatsConnection.status} iterator.
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

export interface Status {
  type: Events | DebugEvents;
  data: string | ServersChanged | number;
  permissionContext?: { operation: string; subject: string };
}

/**
 * Other events that can be reported by the {@link NatsConnection.status} iterator.
 * These can usually be safely ignored, as higher-order functionality of the client
 * will handle them.
 */
export enum DebugEvents {
  Reconnecting = "reconnecting",
  PingTimer = "pingTimer",
  StaleConnection = "staleConnection",
}

export const DEFAULT_PORT = 4222;
export const DEFAULT_HOST = "127.0.0.1";

// DISCONNECT Parameters, 2 sec wait, 10 tries
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
   * @param data
   * @param options
   */
  publish(subject: string, data?: Uint8Array, options?: PublishOptions): void;

  /**
   * Subscribe expresses interest in the specified subject. The subject may
   * have wildcards. Messages are delivered to the {@link SubscriptionOptions.callback}
   * if specified. Otherwise, the subscription is an async iterator for {@link Msg}.
   *
   * @param subject
   * @param opts
   */
  subscribe(subject: string, opts?: SubscriptionOptions): Subscription;

  /**
   * Publishes a request with specified data in the specified subject expecting a
   * response before {@link RequestOptions.timeout} milliseconds. The api returns a
   * Promise that resolves when the first response to the request is received. If
   * there are no responders (a subscription) listening on the request subject,
   * the request will fail as soon as the server processes it.
   *
   * @param subject
   * @param data
   * @param opts
   */
  request(
    subject: string,
    data?: Uint8Array,
    opts?: RequestOptions,
  ): Promise<Msg>;

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
  jetstreamManager(opts?: JetStreamOptions): Promise<JetStreamManager>;

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
}

/**
 * ConnectionOptions does something
 */
export interface ConnectionOptions {
  /**
   * When the server requires authentication, set an {@link Authenticator}.
   * An authenticator is created automatically for username/password and token
   * authentication configurations
   * if {@link user} and {@link pass} or the {@link token} options are set.
   */
  authenticator?: Authenticator;
  /**
   * When set to `true` the client will print protocol messages that it receives
   * or sends to the server.
   */
  debug?: boolean;
  /**
   * Sets the maximum count of ping commands that can be awaiting a response
   * before rasing a stale connection status {@link DebugEvents.StaleConnection }
   * notification {@link NatsConnection.status} and initiating a reconnect.
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
   * @param data
   * @param opts
   */
  respond(data?: Uint8Array, opts?: PublishOptions): boolean;
}

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

/**
 * Type alias for a SubOpts<Msg>
 */
export type SubscriptionOptions = SubOpts<Msg>;

export interface Base {
  subject: string;
  callback: (error: NatsError | null, msg: Msg) => void;
  received: number;
  timeout?: number | null;
  max?: number | undefined;
  draining: boolean;
}

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
    opts: Partial<{ fn: DnsResolveFn; randomize: boolean }>,
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

/**
 * Type alias for NATS core subscriptions
 */
export type Subscription = Sub<Msg>;

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

export interface URLParseFn {
  (u: string): string;
}

export interface DnsResolveFn {
  (h: string): Promise<string[]>;
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

/**
 * The API for interacting with JetStream resources
 */
export interface JetStreamManager {
  /**
   * JetStream API to interact with Consumers
   */
  consumers: ConsumerAPI;
  /**
   * JetStream API to interact with Streams
   */
  streams: StreamAPI;

  /**
   * Returns JetStreamAccountStats for the current client account.
   */
  getAccountInfo(): Promise<JetStreamAccountStats>;

  /**
   * Returns an async iteartor
   */
  advisories(): AsyncIterable<Advisory>;
}

/**
 * Options for a JetStream pull subscription which define how long
 * the pull request will remain open and limits the amount of data
 * that the server could return.
 */
export interface PullOptions {
  /**
   * Max number of messages to retrieve in a pull.
   */
  batch: number;
  /**
   * If true, the request for messages will end when received by the server
   */
  "no_wait": boolean;
  /**
   * If set, the number of milliseconds to wait for the number of messages
   * specified in {@link batch}
   */
  expires: number;
  /**
   * If set, the max number of bytes to receive. The server will limit the
   * number of messages in the batch to fit within this setting.
   */
  "max_bytes": number;
}

/**
 * The response returned by the JetStream server when a message is added to a stream.
 */
export interface PubAck {
  /**
   * The name of the stream
   */
  stream: string;
  /**
   * The domain of the JetStream
   */
  domain?: string;
  /**
   * The sequence number of the message as stored in JetStream
   */
  seq: number;
  /**
   * True if the message is a duplicate
   */
  duplicate: boolean;

  /**
   * Acknowledges the PubAck back to the server
   */
  ack(): void;
}

/**
 * Options for messages published to JetStream
 */
export interface JetStreamPublishOptions {
  /**
   * A string identifier used to detect duplicate published messages.
   * If the msgID is reused within the streams's `duplicate_window`,
   * the message will be rejected by the stream, and the {@link PubAck} will
   * mark it as a `duplicate`.
   */
  msgID: string;
  /**
   * The number of milliseconds to wait for the PubAck
   */
  timeout: number;
  // ackWait: Nanos;
  /**
   * Headers associated with the message. You can create an instance of
   * MsgHdrs with the headers() function.
   */
  headers: MsgHdrs;
  /**
   * Set of constraints that when specified are verified by the server.
   * If the constraint(s) doesn't match, the server will reject the message.
   * These settings allow you to implement deduplication and consistency
   * strategies.
   */
  expect: Partial<{
    /**
     * The expected last msgID of the last message received by the stream.
     */
    lastMsgID: string;
    /**
     * The expected stream capturing the message
     */
    streamName: string;
    /**
     * The expected last sequence on the stream.
     */
    lastSequence: number;
    /**
     * The expected last sequence on the stream for a message with this subject
     */
    lastSubjectSequence: number;
  }>;
}

/**
 * A JetStream interface that allows you to request the ConsumerInfo on the backing object.
 */
export interface ConsumerInfoable {
  consumerInfo(): Promise<ConsumerInfo>;
}

export interface Closed {
  /**
   * A promise that when resolves, indicates that the object is closed.
   */
  closed: Promise<void>;
}

/**
 * The JetStream Subscription object
 */
export type JetStreamSubscription =
  & Sub<JsMsg>
  & Destroyable
  & Closed
  & ConsumerInfoable;

export type JetStreamSubscriptionOptions = TypedSubscriptionOptions<JsMsg>;

export interface Pullable {
  /**
   * Sends a request from the client requesting the server for more messages.
   * @param opts
   */
  pull(opts?: Partial<PullOptions>): void;
}

export interface Destroyable {
  /**
   * Destroys a resource on the server. Returns a promise that resolves to true
   * whene the operation has been completed
   */
  destroy(): Promise<void>;
}

/**
 * The JetStream pull subscription object.
 */
export type JetStreamPullSubscription = JetStreamSubscription & Pullable;

/**
 * The signature a message handler for a JetStream subscription.
 */
export type JsMsgCallback = (err: NatsError | null, msg: JsMsg | null) => void;

/**
 * The interface for creating instances of different JetStream materialized views.
 */
export interface Views {
  /**
   * Gets or creates a JetStream KV store
   * @param name - name for the KV
   * @param opts - optional options to configure the KV and stream backing
   */
  kv: (name: string, opts?: Partial<KvOptions>) => Promise<KV>;
  os: (
    name: string,
    opts?: Partial<ObjectStoreOptions>,
  ) => Promise<ObjectStore>;
}

// FIXME: pulls must limit to maxAcksInFlight
/**
 * Interface for interacting with JetStream data
 */
export interface JetStreamClient {
  /**
   * Publishes a message to a stream. If not stream is configured to store the message, the
   * request will fail with {@link ErrorCode.NoResponders} error.
   *
   * @param subj - the subject for the message
   * @param data - the message's data
   * @param options - the optional message
   */
  publish(
    subj: string,
    data?: Uint8Array,
    options?: Partial<JetStreamPublishOptions>,
  ): Promise<PubAck>;

  /**
   * Retrieves a single message from JetStream
   * @param stream - the name of the stream
   * @param consumer - the consumer's durable name (if durable) or name if ephemeral
   * @param expires - the number of milliseconds to wait for a message
   */
  pull(stream: string, consumer: string, expires?: number): Promise<JsMsg>;

  /**
   * Similar to pull, but able to configure the number of messages, etc via PullOptions.
   * @param stream - the name of the stream
   * @param durable - the consumer's durable name (if durable) or name if ephemeral
   * @param opts
   */
  fetch(
    stream: string,
    durable: string,
    opts?: Partial<PullOptions>,
  ): QueuedIterator<JsMsg>;

  /**
   * Creates a pull subscription. A pull subscription relies on the client to request more
   * messages from the server. If the consumer doesn't exist, it will be created matching
   * the consumer options provided.
   *
   * It is recommended that a consumer be created first using JetStreamManager APIs and then
   * use the bind option to simply attach to the created consumer.
   *
   * If the filter subject is not specified in the options, the filter will be set to match
   * the specified subject.
   *
   * It is more efficient than {@link fetch} or {@link pull} because
   * a single subscription is used between invocations.
   *
   * @param subject - a subject used to locate the stream
   * @param opts
   */
  pullSubscribe(
    subject: string,
    opts: ConsumerOptsBuilder | Partial<ConsumerOpts>,
  ): Promise<JetStreamPullSubscription>;

  /**
   * Creates a push subscription. The JetStream server feeds messages to this subscription
   * without the client having to request them. The rate at which messages are provided can
   * be tuned by the consumer by specifying {@link ConsumerOpts.rate_limit_bps} and/or
   * {@link ConsumerOpts.maxAckPending}.
   *
   * It is recommended that a consumer be created first using JetStreamManager APIs and then
   * use the bind option to simply attach to the created consumer.
   *
   * If the filter subject is not specified in the options, the filter will be set to match
   * the specified subject.
   *
   * @param subject - a subject used to locate the stream
   * @param opts
   */
  subscribe(
    subject: string,
    opts: ConsumerOptsBuilder | Partial<ConsumerOpts>,
  ): Promise<JetStreamSubscription>;

  /**
   * Accessor for the JetStream materialized views API
   */
  views: Views;
}

export interface ConsumerOpts {
  /**
   * The consumer configuration
   */
  config: Partial<ConsumerConfig>;
  /**
   * Enable manual ack. When set to true, the client is responsible to ack messages.
   */
  mack: boolean;
  /**
   * The name of the stream
   */
  stream: string;
  /**
   * An optional callback to process messages - note that iterators are the preferred
   * way of processing messages.
   */
  callbackFn?: JsMsgCallback;
  /**
   * The consumer name
   */
  name?: string;
  /**
   * Only applicable to push consumers. When set to true, the consumer will be an ordered
   * consumer.
   */
  ordered: boolean;
  /**
   * Standard option for all subscriptions. Defines the maximum number of messages dispatched
   * by the server before stopping the subscription. For JetStream this may not be accurate as
   * JetStream can add additional protocol messages that could count towards this limit.
   */
  max?: number;
  /**
   * Only applicable to push consumers, allows the pull subscriber to horizontally load balance.
   */
  queue?: string;
  /**
   * If true, the client will only attempt to bind to the specified consumer name/durable on
   * the specified stream. If the consumer is not found, the subscribe will fail
   */
  isBind?: boolean;
}

/**
 * A builder API that creates a ConsumerOpt
 */
export interface ConsumerOptsBuilder {
  /**
   * User description of this consumer
   */
  description(description: string): this;
  /**
   * DeliverTo sets the subject where a push consumer receives messages
   * @param subject
   */
  deliverTo(subject: string): this;
  /**
   * Sets the durable name, when not set an ephemeral consumer is created
   * @param name
   */
  durable(name: string): this;
  /**
   * The consumer will start at the message with the specified sequence
   * @param seq
   */
  startSequence(seq: number): this;
  /**
   * consumer will start with messages received on the specified time/date
   * @param time
   */
  startTime(time: Date): this;
  /**
   * Consumer will start at first available message on the stream
   */
  deliverAll(): this;
  /**
   * Consumer will deliver all the last per messages per subject
   */
  deliverLastPerSubject(): this;
  /**
   * Consumer will start at the last message
   */
  deliverLast(): this;
  /**
   * Consumer will start with new messages (not yet in the stream)
   */
  deliverNew(): this;
  /**
   * Start delivering at the at a past point in time
   * @param millis
   */
  startAtTimeDelta(millis: number): this;
  /**
   * Messages delivered to the consumer will not have a payload. Instead
   * they will have the header `Nats-Msg-Size` indicating the number of bytes
   * in the message as stored by JetStream.
   */
  headersOnly(): this;
  /**
   * Consumer will not track ack for messages
   */
  ackNone(): this;
  /**
   * Ack'ing a message implicitly acks all messages with a lower sequence
   */
  ackAll(): this;
  /**
   * Consumer will ack all messages - not that unless {@link manualAck} is set
   * the client will auto ack messages after processing via its callback or when
   * the iterator continues processing.
   */
  ackExplicit(): this;
  /**
   * Sets the time a delivered message might remain unacknowledged before a redelivery is attempted
   * @param millis
   */
  ackWait(millis: number): this;
  /**
   * Max number of re-delivery attempts for a particular message
   * @param max
   */
  maxDeliver(max: number): this;
  /**
   * Consumer should filter the messages to those that match the specified filter.
   * @param s
   */
  filterSubject(s: string): this;
  /**
   * Replay messages as fast as possible
   */
  replayInstantly(): this;
  /**
   * Replay at the rate received
   */
  replayOriginal(): this;
  /**
   * Sample a subset of messages expressed as a percentage(0-100)
   * @param n
   */
  sample(n: number): this;
  /**
   * Limit message delivery to the specified rate in bits per second.
   * @param bps
   */
  limit(bps: number): this;
  /**
   * Pull subscriber option only. Limits the maximum outstanding messages scheduled
   * via batch pulls as pulls are additive.
   * @param max
   */
  maxWaiting(max: number): this;
  /**
   * Max number of outstanding acks before the server stops sending new messages
   * @param max
   */
  maxAckPending(max: number): this;
  /**
   * Push consumer only option - Enables idle heartbeats from the server. If the number of
   * specified millis is reached and no messages are available on the server, the server will
   * send a heartbeat (status code 100 message) indicating that the JetStream consumer is alive.
   * @param millis
   */
  idleHeartbeat(millis: number): this;
  /**
   * Push consumer flow control - the server sends a status code 100 and uses the delay on the
   * response to throttle inbound messages for a client and prevent slow consumer.
   */
  flowControl(): this;
  /**
   * Push consumer only option - Sets the name of the queue group - same as queue
   * @param name
   */
  deliverGroup(name: string): this;
  /**
   * Prevents the consumer implementation from auto-acking messages. Message callbacks
   * and iterators must explicitly ack messages.
   */
  manualAck(): this;
  /**
   * Standard NATS subscription option which automatically closes the subscription after the specified
   * number of messages (actual stream or flow control) are seen by the client.
   * @param max
   */
  maxMessages(max: number): this;
  /**
   * Push consumer only option - Standard NATS queue group option, same as {@link deliverGroup}
   * @param n
   */
  queue(n: string): this;
  /**
   * Use a callback to process messages. If not specified, you process messages by iterating
   * on the returned subscription object.
   * @param fn
   */
  callback(fn: JsMsgCallback): this;

  /**
   * Push consumer only - creates an ordered consumer - ordered consumers cannot be a pull consumer
   * nor specify durable, deliverTo, specify an ack policy, maxDeliver, or flow control.
   */
  orderedConsumer(): this;
  /**
   * Bind to the specified durable (or consumer name if ephemeral) on the specified stream.
   * If the consumer doesn't exist, the subscribe will fail. Bind the recommended way
   * of subscribing to a stream, as it requires the consumer to exist already.
   * @param stream
   * @param durable
   */
  bind(stream: string, durable: string): this;
  /**
   * Pull consumer only - Sets the max number of messages that can be pulled in a batch
   * that can be requested by a client during a pull.
   * @param n
   */
  maxPullBatch(n: number): this;

  /**
   * Pull consumer only - Sets the max amount of time before a pull request expires that
   * may be requested by a client during a pull.
   * @param millis
   */
  maxPullRequestExpires(millis: number): this;
  /**
   * Pull consumer only - Sets the max amount of time that an ephemeral consumer will be
   * allowed to live on the server. If the client doesn't perform any requests during the
   * specified interval the server will discard the consumer.
   * @param millis
   */
  inactiveEphemeralThreshold(millis: number): this;

  /**
   * Force the consumer state to be kept in memory rather than inherit the setting from
   * the Stream
   */
  memory(): this;

  /**
   * When set do not inherit the replica count from the stream but specifically set it to this amount
   */
  numReplicas(n: number): this;
}

/**
 * An interface for listing. Returns a promise with typed list.
 */
export interface Lister<T> {
  next(): Promise<T[]>;
}

export interface ConsumerAPI {
  /**
   * Returns the ConsumerInfo for the specified consumer in the specified stream.
   * @param stream
   * @param consumer
   */
  info(stream: string, consumer: string): Promise<ConsumerInfo>;
  /**
   * Adds a new consumer to the specified stream with the specified consumer options.
   * @param stream
   * @param cfg
   */
  add(stream: string, cfg: Partial<ConsumerConfig>): Promise<ConsumerInfo>;
  /**
   * Updates the consumer configuration for the specified consumer on the specified
   * stream that has the specified durable name.
   * @param stream
   * @param durable
   * @param cfg
   */
  update(
    stream: string,
    durable: string,
    cfg: Partial<ConsumerUpdateConfig>,
  ): Promise<ConsumerInfo>;
  /**
   * Deletes the specified consumer name/durable from the specified stream.
   * @param stream
   * @param consumer
   */
  delete(stream: string, consumer: string): Promise<boolean>;
  /**
   * Lists all the consumers on the specfied streams
   * @param stream
   */
  list(stream: string): Lister<ConsumerInfo>;
}

/**
 * Options for StreamAPI info requests
 */
export type StreamInfoRequestOptions = {
  /**
   * Include info on deleted subjects.
   */
  "deleted_details": boolean;
  /**
   * Only include information matching the specified subject filter
   */
  "subjects_filter": string;
};

export interface StreamAPI {
  /**
   * Returns the information about the specified stream
   * @param stream
   * @param opts
   */
  info(
    stream: string,
    opts?: Partial<StreamInfoRequestOptions>,
  ): Promise<StreamInfo>;
  /**
   * Adds a new stream with the specified stream configuration.
   * @param cfg
   */
  add(cfg: Partial<StreamConfig>): Promise<StreamInfo>;
  /**
   * Updates the stream configuration for the specified stream.
   * @param name
   * @param cfg
   */
  update(name: string, cfg: Partial<StreamUpdateConfig>): Promise<StreamInfo>;
  /**
   * Purges messages from a stream that match the specified purge options.
   * @param stream
   * @param opts
   */
  purge(stream: string, opts?: PurgeOpts): Promise<PurgeResponse>;
  /**
   * Deletes the specified stream
   * @param stream
   */
  delete(stream: string): Promise<boolean>;
  /**
   * Lists all streams stored by JetStream
   */
  list(): Lister<StreamInfo>;
  /**
   * Deletes the specified message sequence from the stream
   * @param stream
   * @param seq
   * @param erase - erase the message - by default true
   */
  deleteMessage(stream: string, seq: number, erase?: boolean): Promise<boolean>;
  /**
   * Retrieves the message matching the specified query. Messages can be
   * retrieved by sequence number or by last sequence matching a subject.
   * @param stream
   * @param query
   */
  getMessage(stream: string, query: MsgRequest): Promise<StoredMsg>;
  /**
   * Find the stream that stores the specified subject.
   * @param subject
   */
  find(subject: string): Promise<string>;
}

/**
 * Request the next stream message by sequence for the specified subject.
 */
export type NextMsgRequest = {
  /**
   * The seq to start looking. If the message under the specified sequence
   * matches, it will be returned.
   */
  seq: number;
  /**
   * The subject to look for
   */
  next_by_subj: string;
};

export type DirectMsgRequest =
  | SeqMsgRequest
  | LastForMsgRequest
  | NextMsgRequest;

/**
 * The Direct stream API is a bit more performant for retrieving messages,
 * but requires the stream to have enabled direct access.
 * See {@link StreamConfig.allow_direct}.
 */
export interface DirectStreamAPI {
  /**
   * Retrieves the message matching the specified query. Messages can be
   * retrieved by sequence number or by last sequence matching a subject, or
   * by looking for the next message sequence that matches a subject.
   * @param stream
   * @param query
   */
  getMessage(stream: string, query: DirectMsgRequest): Promise<StoredMsg>;
}

/**
 * Represents a message stored in JetStream
 */
export interface JsMsg {
  /**
   * True if the message was redelivered
   */
  redelivered: boolean;
  /**
   * The delivery info for the message
   */
  info: DeliveryInfo;
  /**
   * The sequence number for the message
   */
  seq: number;
  /**
   * Any headers associated with the message
   */
  headers: MsgHdrs | undefined;
  /**
   * The message's data
   */
  data: Uint8Array;
  /**
   * The subject on which the message was published
   */
  subject: string;
  /**
   * @ignore
   */
  sid: number;
  /**
   * Indicate to the JetStream server that the message was processed
   * successfully.
   */
  ack(): void;
  /**
   * Indicate to the JetStream server that processing of the message
   * failed, and that it should be resent after the spefied number of
   * milliseconds.
   * @param millis
   */
  nak(millis?: number): void;
  /**
   * Indicate to the JetStream server that processing of the message
   * is on going, and that the ack wait timer for the message should be
   * reset preventing a redelivery.
   */
  working(): void;
  /**
   * !! this is an experimental feature - and could be removed
   *
   * next() combines ack() and pull(), requires the subject for a
   * subscription processing to process a message is provided
   * (can be the same) however, because the ability to specify
   * how long to keep the request open can be specified, this
   * functionality doesn't work well with iterators, as an error
   * (408s) are expected and needed to re-trigger a pull in case
   * there was a timeout. In an iterator, the error will close
   * the iterator, requiring a subscription to be reset.
   */
  next(subj: string, ro?: Partial<PullOptions>): void;
  /**
   * Indicate to the JetStream server that processing of the message
   * failed and that the message should not be sent to the consumer again.
   */
  term(): void;
  /**
   * Indicate to the JetStream server that the message was processed
   * successfully and that the JetStream server should acknowledge back
   * that the acknowledgement was received.
   */
  ackAck(): Promise<boolean>;
}

export interface DeliveryInfo {
  /**
   * JetStream domain of the message if applicable.
   */
  domain: string;
  /**
   * The hash of the sending account if applicable.
   */
  "account_hash"?: string;
  /**
   * The stream where the message came from
   */
  stream: string;
  /**
   * The intended consumer for the message.
   */
  consumer: string;
  /**
   * The number of times the message has been redelivered.
   */
  redeliveryCount: number;
  /**
   * The sequence number of the message in the stream
   */
  streamSequence: number;
  /**
   * The client delivery sequence for the message
   */
  deliverySequence: number;
  /**
   * The timestamp for the message in nanoseconds. Convert with `millis(n)`,
   */
  timestampNanos: number;
  /**
   * The number of pending messages for the consumer at the time the
   * message was delivered.
   */
  pending: number;
  /**
   * True if the message has been redelivered.
   */
  redelivered: boolean;
}

/**
 * An interface representing a message that retrieved directly from JetStream.
 */
export interface StoredMsg {
  /**
   * The subject the message was originally received on
   */
  subject: string;
  /**
   * The sequence number of the message in the Stream
   */
  seq: number;
  /**
   * Headers for the message
   */
  header: MsgHdrs;
  /**
   * The payload of the message body
   */
  data: Uint8Array;
  /**
   * The time the message was received
   */
  time: Date;
}

export interface DirectMsg extends StoredMsg {
  /**
   * The name of the Stream storing message
   */
  stream: string;
}

/**
 * An advisory is an interesting event in the JetStream server
 */
export interface Advisory {
  /**
   * The type of the advisory
   */
  kind: AdvisoryKind;
  /**
   * Payload associated with the advisory
   */
  data: unknown;
}

/**
 * The different kinds of Advisories
 */
export enum AdvisoryKind {
  API = "api_audit",
  StreamAction = "stream_action",
  ConsumerAction = "consumer_action",
  SnapshotCreate = "snapshot_create",
  SnapshotComplete = "snapshot_complete",
  RestoreCreate = "restore_create",
  RestoreComplete = "restore_complete",
  MaxDeliver = "max_deliver",
  Terminated = "terminated",
  Ack = "consumer_ack",
  StreamLeaderElected = "stream_leader_elected",
  StreamQuorumLost = "stream_quorum_lost",
  ConsumerLeaderElected = "consumer_leader_elected",
  ConsumerQuorumLost = "consumer_quorum_lost",
}

// JetStream Server Types
/**
 * Value expressed as Nanoseconds - use the `nanos(millis)` function
 * to convert millis to nanoseconds. Note that in some environments this
 * could overflow.
 */
export type Nanos = number;

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

export interface ApiResponse {
  type: string;
  error?: ApiError;
}

export interface ApiPaged {
  total: number;
  offset: number;
  limit: number;
}

export interface ApiPagedRequest {
  offset: number;
}

/**
 * An alternate location to read mirrored data
 */
export interface StreamAlternate {
  /**
   * The mirror Stream name
   */
  name: string;
  /**
   * The name of the cluster holding the Stream
   */
  cluster: string;

  /**
   * The domain holding the Stream
   */
  domain: string;
}

/**
 * Stream configuration info
 */
export interface StreamInfo {
  /**
   * The active configuration for the Stream
   */
  config: StreamConfig;
  /**
   * Timestamp when the stream was created
   */
  created: Nanos;
  /**
   * Detail about the current State of the Stream
   */
  state: StreamState;
  /**
   * Cluster information for the stream if applicable
   */
  cluster?: ClusterInfo;
  /**
   * Information about an upstream stream source in a mirror
   */
  mirror?: StreamSourceInfo;
  /**
   * Sources for the Stream if applicable
   */
  sources?: StreamSourceInfo[];
  /**
   * Alternates for a stream if applicable. Alternates are listed
   * in order of TTL. With streams at the start of the Array potentially
   * closer and faster to access.
   */
  alternates?: StreamAlternate[];
}

export interface StreamConfig extends StreamUpdateConfig {
  /**
   * A unique name for the Stream
   */
  name: string;
  /**
   * How messages are retained in the Stream, once this is exceeded old messages are removed.
   */
  retention: RetentionPolicy;
  /**
   * The storage backend to use for the Stream.
   */
  storage: StorageType;
  /**
   * How many Consumers can be defined for a given Stream. -1 for unlimited.
   */
  "max_consumers": number;
  /**
   * Maintains a 1:1 mirror of another stream with name matching this property.
   * When a mirror is configured subjects and sources must be empty.
   */
  mirror?: StreamSource; // same as a source
  /**
   * Sealed streams do not allow messages to be deleted via limits or API,
   * sealed streams can not be unsealed via configuration update.
   * Can only be set on already created streams via the Update API
   */
  sealed: boolean;
}

/**
 * Stream options that can be updated
 */
export interface StreamUpdateConfig {
  /**
   * A list of subjects to consume, supports wildcards. Must be empty when a mirror is configured. May be empty when sources are configured.
   */
  subjects: string[];
  /**
   * A short description of the purpose of this stream
   */
  description?: string;
  /**
   * For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit
   */
  "max_msgs_per_subject": number;
  /**
   * How many messages may be in a Stream, oldest messages will be removed if the Stream exceeds this size. -1 for unlimited.
   */
  "max_msgs": number;
  /**
   * Maximum age of any message in the stream, expressed in nanoseconds. 0 for unlimited.
   */
  "max_age": Nanos;
  /**
   * How big the Stream may be, when the combined stream size exceeds this old messages are removed. -1 for unlimited.
   */
  "max_bytes": number;
  /**
   * The largest message that will be accepted by the Stream. -1 for unlimited.
   */
  "max_msg_size": number;
  /**
   * When a Stream reach its limits either old messages are deleted or new ones are denied
   */
  discard: DiscardPolicy;
  /**
   * Disables acknowledging messages that are received by the Stream.
   */
  "no_ack"?: boolean;
  /**
   * The time window to track duplicate messages for, expressed in nanoseconds. 0 for default
   * Set {@link JetStreamPublishOptions.msgID} to enable duplicate detection.
   */
  "duplicate_window": Nanos;
  /**
   * List of Stream names to replicate into this Stream
   */
  sources?: StreamSource[];
  /**
   * Allows the use of the {@link JsHeaders.RollupHdr} header to replace all contents of a stream,
   * or subject in a stream, with a single new message
   */
  "allow_rollup_hdrs": boolean;
  /**
   * How many replicas to keep for each message. Min 1, Max 5. Default 1.
   */
  "num_replicas": number;
  /**
   * Placement directives to consider when placing replicas of this stream, random placement when unset
   */
  placement?: Placement;
  /**
   * Restricts the ability to delete messages from a stream via the API.
   * Cannot be changed once set to true
   */
  "deny_delete": boolean;
  /**
   * Restricts the ability to purge messages from a stream via the API.
   * Cannot be change once set to true
   */
  "deny_purge": boolean;
  /**
   * Allow higher performance, direct access to get individual messages via the $JS.DS.GET API
   */
  "allow_direct": boolean;
  /**
   * Rules for republishing messages from a stream with subject mapping
   * onto new subjects for partitioning and more
   */
  republish?: Republish;
}

export interface Republish {
  /**
   * The source subject to republish
   */
  src: string;
  /**
   * The destination to publish to
   */
  dest: string;
  /**
   * Only send message headers, no bodies
   */
  "headers_only"?: boolean;
}

export interface StreamSource {
  /**
   * Name of the stream source
   */
  name: string;
  /**
   * An optional start sequence from which to start reading messages
   */
  "opt_start_seq"?: number;
  /**
   * An optional start time Date string
   */
  "opt_start_time"?: string;
  /**
   * An optional filter subject. If the filter matches the message will be
   * on-boarded.
   */
  "filter_subject"?: string;
}

export interface Placement {
  /**
   * The cluster to place the stream on
   */
  cluster: string;
  /**
   * Tags matching server configuration
   */
  tags: string[];
}

export enum RetentionPolicy {
  /**
   * Retain messages until the limits are reached, then trigger the discard policy.
   */
  Limits = "limits",
  /**
   * Retain messages while there is consumer interest on the particular subject.
   */
  Interest = "interest",
  /**
   * Retain messages until acknowledged
   */
  Workqueue = "workqueue",
}

export enum DiscardPolicy {
  /**
   * Discard old messages to make room for the new ones
   */
  Old = "old",
  /**
   * Discard the new messages
   */
  New = "new",
}

export enum StorageType {
  /**
   * Store persistently on files
   */
  File = "file",
  /**
   * Store in server memory - doesn't survive server restarts
   */
  Memory = "memory",
}

export enum DeliverPolicy {
  /**
   * Deliver all messages
   */
  All = "all",
  /**
   * Deliver starting with the last message
   */
  Last = "last",
  /**
   * Deliver starting with new messages
   */
  New = "new",
  /**
   * Deliver starting with the specified sequence
   */
  StartSequence = "by_start_sequence",
  /**
   * Deliver starting with the specified time
   */
  StartTime = "by_start_time",
  /**
   * Deliver starting with the last messages for every subject
   */
  LastPerSubject = "last_per_subject",
}

export enum AckPolicy {
  /**
   * Messages don't need to be Ack'ed.
   */
  None = "none",
  /**
   * Ack, acknowledges all messages with a lower sequence
   */
  All = "all",
  /**
   * All sequences must be explicitly acknowledged
   */
  Explicit = "explicit",
  /**
   * @ignore
   */
  NotSet = "",
}

export enum ReplayPolicy {
  /**
   * Replays messages as fast as possible
   */
  Instant = "instant",
  /**
   * Replays messages following the original delay between messages
   */
  Original = "original",
}

export interface StreamState {
  /**
   * Number of messages stored in the Stream
   */
  messages: number;
  /**
   * Combined size of all messages in the Stream
   */
  bytes: number;
  /**
   * Sequence number of the first message in the Stream
   */
  "first_seq": number;
  /**
   * The timestamp of the first message in the Stream
   */
  "first_ts": number;
  /**
   * Sequence number of the last message in the Stream
   */
  "last_seq": number;
  /**
   * The timestamp of the last message in the Stream
   */
  "last_ts": string;
  /**
   * The number of deleted messages
   */
  "num_deleted": number;
  /**
   * IDs of messages that were deleted using the Message Delete API or Interest based streams removing messages out of order
   * {@link StreamInfoRequestOptions.deleted_details} is specified on
   * the request.
   */
  deleted: number[];
  /**
   * Messages that were damaged and unrecoverable
   */
  lost: LostStreamData;
  /**
   * Number of Consumers attached to the Stream
   */
  "consumer_count": number;
  /**
   * The number of unique subjects held in the stream
   */
  num_subjects?: number;
  /**
   * Subjects and their message counts when a {@link StreamInfoRequestOptions.subjects_filter} was set
   */
  subjects?: Record<string, number>;
}

/**
 * Records messages that were damaged and unrecoverable
 */
export interface LostStreamData {
  /**
   * The messages that were lost
   */
  msgs: number[] | null;
  /**
   * The number of bytes that were lost
   */
  bytes: number;
}

export interface ClusterInfo {
  /**
   * The cluster name
   */
  name?: string;
  /**
   * The server name of the RAFT leader
   */
  leader?: string;
  /**
   * The members of the RAFT cluster
   */
  replicas?: PeerInfo[];
}

export interface PeerInfo {
  /**
   * The server name of the peer
   */
  name: string;
  /**
   * Indicates if the server is up to date and synchronised
   */
  current: boolean;
  /**
   * Indicates the node is considered offline by the group
   */
  offline: boolean;
  /**
   * Nanoseconds since this peer was last seen
   */
  active: Nanos;
  /**
   * How many uncommitted operations this peer is behind the leader
   */
  lag: number;
}

/**
 * Information about an upstream stream source in a mirror
 */
export interface StreamSourceInfo {
  /**
   * The name of the Stream being replicated
   */
  name: string;
  /**
   * How many messages behind the mirror operation is
   */
  lag: number;
  /**
   * When last the mirror had activity, in nanoseconds. Value will be -1 when there has been no activity.
   */
  active: Nanos;
  /**
   * A possible error
   */
  error?: ApiError;
}

export type PurgeOpts = PurgeBySeq | PurgeTrimOpts | PurgeBySubject;

export type PurgeBySeq = {
  /**
   * Restrict purging to messages that match this subject
   */
  filter?: string;
  /**
   * Purge all messages up to but not including the message with this sequence.
   */
  seq: number;
};

export type PurgeTrimOpts = {
  /**
   * Restrict purging to messages that match this subject
   */
  filter?: string;
  /**
   * Ensures this many messages are present after the purge.
   */
  keep: number;
};

export type PurgeBySubject = {
  /**
   * Restrict purging to messages that match this subject
   */
  filter: string;
};

export interface PurgeResponse extends Success {
  /**
   * Number of messages purged from the Stream
   */
  purged: number;
}

export interface CreateConsumerRequest {
  "stream_name": string;
  config: Partial<ConsumerConfig>;
}

export interface StreamMsgResponse extends ApiResponse {
  message: {
    subject: string;
    seq: number;
    data: string;
    hdrs: string;
    time: string;
  };
}

export interface SequenceInfo {
  "consumer_seq": number;
  "stream_seq": number;
  "last_active": Nanos;
}

export interface ConsumerInfo {
  /**
   * The stream hosting the consumer
   */
  "stream_name": string;
  /**
   * A unique name for the consumer, either machine generated or the durable name
   */
  name: string;
  /**
   * The time the Consumer was created
   */
  created: Nanos;
  /**
   * The consumer configuration
   */
  config: ConsumerConfig;
  /**
   * The last message delivered from this Consumer
   */
  delivered: SequenceInfo;
  /**
   * The highest contiguous acknowledged message
   */
  "ack_floor": SequenceInfo;
  /**
   * The number of messages pending acknowledgement
   * but yet to acknowledged by the client.
   */
  "num_ack_pending": number;
  /**
   * The number of redeliveries that have been performed
   */
  "num_redelivered": number;
  /**
   * The number of pull consumers waiting for messages
   */
  "num_waiting": number;
  /**
   * The number of messages left unconsumed in this Consumer
   */
  "num_pending": number;
  /**
   * The cluster where the consumer is defined
   */
  cluster?: ClusterInfo;
  /**
   * Indicates if any client is connected and receiving messages from a push consumer
   */
  "push_bound": boolean;
}

export interface ConsumerListResponse extends ApiResponse, ApiPaged {
  consumers: ConsumerInfo[];
}

export interface StreamListResponse extends ApiResponse, ApiPaged {
  streams: StreamInfo[];
}

export interface Success {
  /**
   * True if the operation succeeded
   */
  success: boolean;
}

export type SuccessResponse = ApiResponse & Success;

export interface LastForMsgRequest {
  /**
   * Retrieves the last message for the given subject
   */
  "last_by_subj": string;
}

export interface SeqMsgRequest {
  /**
   * Stream sequence number of the message to retrieve
   */
  seq: number;
}

// FIXME: remove number as it is deprecated
export type MsgRequest = SeqMsgRequest | LastForMsgRequest | number;

export interface MsgDeleteRequest extends SeqMsgRequest {
  /**
   * Default will securely remove a message and rewrite the data with random data,
   * set this to true to only remove the message
   */
  "no_erase"?: boolean;
}

export interface AccountLimits {
  /**
   * The maximum amount of Memory storage Stream Messages may consume
   */
  "max_memory": number;
  /**
   * The maximum amount of File storage Stream Messages may consume
   */
  "max_storage": number;
  /**
   * The maximum number of Streams an account can create
   */
  "max_streams": number;
  /**
   * The maximum number of Consumer an account can create
   */
  "max_consumers": number;
  /**
   * The maximum number of outstanding ACKs any consumer may configure
   */
  "max_ack_pending": number;
  /**
   * The maximum size any single memory stream may be
   */
  "memory_max_stream_bytes": number;
  /**
   * The maximum size any single storage based stream may be
   */
  "storage_max_stream_bytes": number;
  /**
   * Indicates if Streams created in this account requires the max_bytes property set
   */
  "max_bytes_required": number;
}

export interface JetStreamUsage {
  /**
   * Memory Storage being used for Stream Message storage
   */
  memory: number;
  /**
   * File Storage being used for Stream Message storage
   */
  storage: number;
  /**
   * Number of active Streams
   */
  streams: number;
  /**
   * "Number of active Consumers
   */
  consumers: number;
}

export interface JetStreamUsageAccountLimits extends JetStreamUsage {
  limits: AccountLimits;
}

export interface JetStreamAccountStats extends JetStreamUsageAccountLimits {
  api: JetStreamApiStats;
  domain?: string;
  tiers?: {
    R1?: JetStreamUsageAccountLimits;
    R3?: JetStreamUsageAccountLimits;
  };
}

export interface JetStreamApiStats {
  /**
   * Total number of API requests received for this account
   */
  total: number;
  /**
   * "API requests that resulted in an error response"
   */
  errors: number;
}

export interface AccountInfoResponse
  extends ApiResponse, JetStreamAccountStats {}

export interface ConsumerConfig extends ConsumerUpdateConfig {
  /**
   * The type of acknowledgment required by the Consumer
   */
  "ack_policy": AckPolicy;
  /**
   * Where to start consuming messages on the stream
   */
  "deliver_policy": DeliverPolicy;
  /**
   * Allows push consumers to form a queue group
   */
  "deliver_group"?: string;
  /**
   * A unique name for a durable consumer
   */
  "durable_name"?: string;
  /**
   * Deliver only messages that match the subject filter
   */
  "filter_subject"?: string;
  /**
   * For push consumers this will regularly send an empty mess with Status header 100
   * and a reply subject, consumers must reply to these messages to control
   * the rate of message delivery.
   */
  "flow_control"?: boolean;
  /**
   * If the Consumer is idle for more than this many nano seconds a empty message with
   * Status header 100 will be sent indicating the consumer is still alive
   */
  "idle_heartbeat"?: Nanos;
  /**
   * The sequence from which to start delivery messages.
   * Requires {@link DeliverPolicy.StartSequence}
   */
  "opt_start_seq"?: number;
  /**
   * The date time from which to start delivering messages
   * Requires {@link DeliverPolicy.StartTime}
   */
  "opt_start_time"?: string;
  /**
   * The rate at which messages will be delivered to clients, expressed in bit per second
   */
  "rate_limit_bps"?: number;
  /**
   * How messages are played back to the Consumer
   */
  "replay_policy": ReplayPolicy;
}

export interface ConsumerUpdateConfig {
  /**
   * A short description of the purpose of this consume
   */
  description?: string;
  /**
   * How long (in nanoseconds) to allow messages to remain un-acknowledged before attempting redelivery
   */
  "ack_wait"?: Nanos;
  /**
   * The number of times a message will be redelivered to consumers if not acknowledged in time
   */
  "max_deliver"?: number;
  /**
   * @ignore
   */
  "sample_freq"?: string;
  /**
   * The maximum number of messages without acknowledgement that can be outstanding,
   * once this limit is reached message delivery will be suspended
   */
  "max_ack_pending"?: number;
  /**
   * The number of pulls that can be outstanding on a pull consumer,
   * pulls received after this is reached are ignored
   */
  "max_waiting"?: number;
  /**
   * Delivers only the headers of messages in the stream and not the bodies. Additionally
   * adds Nats-Msg-Size {@link JsHeaders.MessageSizeHdr} header to indicate the size of
   * the removed payload
   */
  "headers_only"?: boolean;
  /**
   * The subject where the push consumer should be sent the messages
   */
  "deliver_subject"?: string;
  /**
   * The largest batch property that may be specified when doing a pull on a Pull Consumer
   */
  "max_batch"?: number;
  /**
   * The maximum expires value that may be set when doing a pull on a Pull Consumer
   */
  "max_expires"?: Nanos;
  /**
   * Duration that instructs the server to cleanup ephemeral consumers that are inactive for that long
   */
  "inactive_threshold"?: Nanos;
  /**
   * List of durations in nanoseconds format that represents a retry time scale for
   * NaK'd messages or those being normally retried
   */
  "backoff"?: Nanos[];
  /**
   * The maximum bytes value that maybe set when dong a pull on a Pull Consumer
   */
  "max_bytes"?: number;
  /**
   * When set do not inherit the replica count from the stream but specifically set it to this amount.
   */
  "num_replicas"?: number;
  /**
   * Force the consumer state to be kept in memory rather than inherit the setting from the stream
   */
  "mem_storage"?: boolean;
}

export interface Consumer {
  /**
   * The name of the Stream the consumer is bound to
   */
  "stream_name": string;
  /**
   * The consumer configuration
   */
  config: ConsumerConfig;
}

export interface StreamNames {
  streams: string[];
}

export interface StreamNameBySubject {
  subject: string;
}

export enum JsHeaders {
  /**
   * Set if message is from a stream source - format is `stream seq`
   */
  StreamSourceHdr = "Nats-Stream-Source",
  /**
   * Set for heartbeat messages
   */
  LastConsumerSeqHdr = "Nats-Last-Consumer",
  /**
   * Set for heartbeat messages
   */
  LastStreamSeqHdr = "Nats-Last-Stream",
  /**
   * Set for heartbeat messages if the consumer is stalled
   */
  ConsumerStalledHdr = "Nats-Consumer-Stalled",
  /**
   * Set for headers_only consumers indicates the number of bytes in the payload
   */
  MessageSizeHdr = "Nats-Msg-Size",
  // rollup header
  RollupHdr = "Nats-Rollup",
  // value for rollup header when rolling up a subject
  RollupValueSubject = "sub",
  // value for rollup header when rolling up all subjects
  RollupValueAll = "all",
}

export interface KvEntry {
  bucket: string;
  key: string;
  value: Uint8Array;
  created: Date;
  revision: number;
  delta?: number;
  operation: "PUT" | "DEL" | "PURGE";
  length: number;
}

/**
 * An interface for encoding and decoding values
 * before they are stored or returned to the client.
 */
export interface KvCodec<T> {
  encode(k: T): T;
  decode(k: T): T;
}

export interface KvCodecs {
  /**
   * Codec for Keys in the KV
   */
  key: KvCodec<string>;
  /**
   * Codec for Data in the KV
   */
  value: KvCodec<Uint8Array>;
}

export interface KvLimits {
  /**
   * Sets the specified description on the stream of the KV.
   */
  description: string;
  /**
   * Number of replicas for the KV (1,3,or 5).
   */
  replicas: number;
  /**
   * Number of maximum messages allowed per subject (key).
   */
  history: number;
  /**
   * The maximum number of bytes on the KV
   */
  max_bytes: number;
  /**
   * @deprecated use max_bytes
   */
  maxBucketSize: number;
  /**
   * The maximum size of a value on the KV
   */
  maxValueSize: number;
  /**
   * The maximum number of millis the key should live
   * in the KV. The server will automatically remove
   * keys older than this amount. Note that no delete
   * marker or notifications are performed.
   */
  ttl: number; // millis
  /**
   * The backing store of the stream hosting the KV
   */
  storage: StorageType;
  /**
   * Placement hints for the stream hosting the KV
   */
  placement: Placement;
  /**
   * Republishes edits to the KV on a NATS core subject.
   */
  republish: Republish;
  /**
   * @deprecated: use placement
   */
  placementCluster: string;

  /**
   * deprecated: use storage
   * FIXME: remove this on 1.8
   */
  backingStore: StorageType;
}

export interface KvStatus extends KvLimits {
  /**
   * The simple name for a Kv - this name is typically prefixed by `KV_`.
   */
  bucket: string;
  /**
   * Number of entries in the KV
   */
  values: number;

  /**
   * @deprecated
   * FIXME: remove this on 1.8
   */
  bucket_location: string;
}

export interface KvOptions extends KvLimits {
  /**
   * How long to wait in milliseconds for a response from the KV
   */
  timeout: number;
  /**
   * The underlying stream name for the KV
   */
  streamName: string;
  /**
   * An encoder/decoder for keys and values
   */
  codec: KvCodecs;
  /**
   * Doesn't attempt to create the KV stream if it doesn't exist.
   */
  bindOnly: boolean;
  /**
   * If true and on a recent server, changes the way the KV
   * retrieves values. This option is significantly faster,
   * but has the possibility of inconsistency during a read.
   */
  allow_direct: boolean;
}

/**
 * @deprecated use purge(k)
 */
export interface KvRemove {
  remove(k: string): Promise<void>;
}

export type KvWatchOptions = {
  /**
   * A key or wildcarded key following keys as if they were NATS subject names.
   */
  key?: string;
  /**
   * Notification should only include entry headers
   */
  headers_only?: boolean;
  /**
   * A callback that notifies when the watch has yielded all the initial values.
   * Subsequent notifications are updates since the initial watch was established.
   * @deprecated
   */
  initializedFn?: callbackFn;
};

export interface RoKV {
  /**
   * Returns the KvEntry stored under the key if it exists or null if not.
   * Note that the entry returned could be marked with a "DEL" or "PURGE"
   * operation which signifies the server stored the value, but it is now
   * deleted.
   * @param k
   * @param opts
   */
  get(k: string, opts?: { revision: number }): Promise<KvEntry | null>;

  /**
   * Returns an iterator of the specified key's history (or all keys).
   * @param opts
   */
  history(opts?: { key?: string }): Promise<QueuedIterator<KvEntry>>;

  /**
   * Returns an iterator that will yield KvEntry updates as they happen.
   * @param opts
   */
  watch(
    opts?: KvWatchOptions,
  ): Promise<QueuedIterator<KvEntry>>;

  /**
   * @deprecated - this api is removed.
   */
  close(): Promise<void>;

  /**
   * Returns information about the Kv
   */
  status(): Promise<KvStatus>;

  /**
   * Returns an iterator of all the keys optionally matching
   * the specified filter.
   * @param filter
   */
  keys(filter?: string): Promise<QueuedIterator<string>>;
}

export interface KV extends RoKV {
  /**
   * Creates a new entry ensuring that the entry does not exist.
   * If the entry already exists, this operation fails.
   * @param k
   * @param data
   */
  create(k: string, data: Uint8Array): Promise<number>;

  /**
   * Updates the existing entry provided that the previous sequence
   * for the Kv is at the specified version. This ensures that the
   * KV has not been modified prior to the update.
   * @param k
   * @param data
   * @param version
   */
  update(k: string, data: Uint8Array, version: number): Promise<number>;

  /**
   * Sets or updates the value stored under the specified key.
   * @param k
   * @param data
   * @param opts
   */
  put(
    k: string,
    data: Uint8Array,
    opts?: Partial<KvPutOptions>,
  ): Promise<number>;

  /**
   * Deletes the entry stored under the specified key.
   * Deletes are soft-deletes. The server will add a new
   * entry marked by a "DEL" operation.
   * Note that if the KV was created with an underlying limit
   * (such as a TTL on keys) it is possible for
   * a key or the soft delete marker to be removed without
   * additional notification on a watch.
   * @param k
   */
  delete(k: string): Promise<void>;

  /**
   * Deletes and purges the specified key and any value
   * history.
   * @param k
   */
  purge(k: string): Promise<void>;

  /**
   * Destroys the underlying stream used by the KV. This
   * effectively deletes all data stored under the KV.
   */
  destroy(): Promise<boolean>;
}

export interface KvPutOptions {
  /**
   * If set the KV must be at the current sequence or the
   * put will fail.
   */
  previousSeq: number;
}

export type ObjectStoreLink = {
  // name of other object store
  bucket: string;
  // link to single object, when empty this means the whole store
  name?: string;
};

export type ObjectStoreMetaOptions = {
  link?: ObjectStoreLink;
  max_chunk_size?: number;
};

export type ObjectStoreMeta = {
  name: string;
  description?: string;
  headers?: MsgHdrs;
  options?: ObjectStoreMetaOptions;
};

export interface ObjectInfo extends ObjectStoreMeta {
  bucket: string;
  nuid: string;
  size: number;
  chunks: number;
  digest: string;
  deleted: boolean;
  mtime: string;
}

export interface ObjectLink {
  bucket: string;
  name?: string;
}

export type ObjectStoreInfo = {
  bucket: string;
  description: string;
  ttl: Nanos;
  storage: StorageType;
  replicas: number;
  sealed: boolean;
  size: number;
  backingStore: string;
};

export type ObjectStoreOptions = {
  description?: string;
  ttl?: Nanos;
  storage: StorageType;
  replicas: number;
  "max_bytes": number;
  placement: Placement;
};

export type ObjectResult = {
  info: ObjectInfo;
  data: ReadableStream<Uint8Array>;
  error: Promise<Error | null>;
};

export interface ObjectStore {
  info(name: string): Promise<ObjectInfo | null>;
  list(): Promise<ObjectInfo[]>;
  get(name: string): Promise<ObjectResult | null>;
  put(
    meta: ObjectStoreMeta,
    rs: ReadableStream<Uint8Array>,
  ): Promise<ObjectInfo>;
  delete(name: string): Promise<PurgeResponse>;
  link(name: string, meta: ObjectInfo): Promise<ObjectInfo>;
  linkStore(name: string, bucket: ObjectStore): Promise<ObjectInfo>;
  watch(
    opts?: Partial<
      {
        ignoreDeletes?: boolean;
        includeHistory?: boolean;
      }
    >,
  ): Promise<QueuedIterator<ObjectInfo | null>>;
  seal(): Promise<ObjectStoreInfo>;
  status(opts?: Partial<StreamInfoRequestOptions>): Promise<ObjectStoreInfo>;
  update(name: string, meta: Partial<ObjectStoreMeta>): Promise<PubAck>;
  destroy(): Promise<boolean>;
}

export type callbackFn = () => void;

export enum DirectMsgHeaders {
  Stream = "Nats-Stream",
  Sequence = "Nats-Sequence",
  TimeStamp = "Nats-Time-Stamp",
  Subject = "Nats-Subject",
}

export enum RepublishHeaders {
  /**
   * The source stream of the message
   */
  Stream = "Nats-Stream",
  /**
   * The original subject of the message
   */
  Subject = "Nats-Subject",
  /**
   * The sequence of the republished message
   */
  Sequence = "Nats-Sequence",
  /**
   * The stream sequence id of the last message ingested to the same original subject (or 0 if none or deleted)
   */
  LastSequence = "Nats-Last-Sequence",
  /**
   * The size in bytes of the message's body - Only if {@link Republish.headers_only} is set.
   */
  Size = "Nats-Msg-Size",
}

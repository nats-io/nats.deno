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

import { Consumer, OrderedConsumerOptions } from "./consumer.ts";
import {
  JetStreamOptions,
  MsgHdrs,
  Nanos,
  NatsError,
  Payload,
  QueuedIterator,
  ReviverFn,
  Sub,
} from "../nats-base-client/core.ts";

export type {
  JetStreamManagerOptions,
  JetStreamOptions,
} from "../nats-base-client/core.ts";

import { TypedSubscriptionOptions } from "../nats-base-client/typedsub.ts";
import {
  AckPolicy,
  ConsumerConfig,
  ConsumerInfo,
  defaultConsumer,
  DeliverPolicy,
  DirectMsgRequest,
  JetStreamAccountStats,
  MsgRequest,
  Placement,
  PullOptions,
  PurgeOpts,
  PurgeResponse,
  ReplayPolicy,
  Republish,
  StorageType,
  StreamAlternate,
  StreamConfig,
  StreamInfo,
  StreamInfoRequestOptions,
  StreamSource,
  StreamUpdateConfig,
} from "./jsapi_types.ts";
import { JsMsg } from "./jsmsg.ts";
import { BaseApiClient } from "./jsbaseclient_api.ts";
import { ConsumerAPI } from "./jsmconsumer_api.ts";
import { nanos, validateDurableName } from "./jsutil.ts";
import { Lister } from "./jslister.ts";

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
}

/**
 * Options for messages published to JetStream
 */
export interface JetStreamPublishOptions {
  /**
   * A string identifier used to detect duplicate published messages.
   * If the msgID is reused within the stream's `duplicate_window`,
   * the message will be rejected by the stream, and the {@link PubAck} will
   * mark it as a `duplicate`.
   */
  msgID: string;
  /**
   * The number of milliseconds to wait for the PubAck
   */
  timeout: number;
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
  /** The consumer info for the consumer */
  consumerInfo(): Promise<ConsumerInfo>;
}

/**
 * An interface that reports via a promise when an object such as a connection
 * or subscription closes.
 */
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
   * @param subject - only return streams that include the specified subject
   */
  list(subject?: string): Lister<StreamInfo>;

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

  /**
   * Returns a list of KvStatus for all streams that are identified as
   * being a KV (that is having names that have the prefix `KV_`)
   */
  listKvs(): Lister<KvStatus>;

  /**
   * Returns a list of ObjectStoreInfo for all streams that are identified as
   * being a ObjectStore (that is having names that have the prefix `OBJ_`)
   */
  listObjectStores(): Lister<ObjectStoreStatus>;

  /**
   * Return a Lister of stream names
   * @param subject - if specified, the results are filtered to streams that contain the
   *  subject (can be wildcarded)
   */
  names(subject?: string): Lister<string>;

  /**
   * Returns a Stream object
   * @param name
   */
  get(name: string): Promise<Stream>;
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

  /**
   * Returns the {@link JetStreamOptions} used to create this
   * JetStreamManager
   */
  getOptions(): JetStreamOptions;

  /**
   * Returns a {@link JetStreamClient} created using the same
   * options as this JetStreamManager
   */
  jetstream(): JetStreamClient;
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
   * @param payload - the message's data
   * @param options - the optional message
   */
  publish(
    subj: string,
    payload?: Payload,
    options?: Partial<JetStreamPublishOptions>,
  ): Promise<PubAck>;

  /**
   * Retrieves a single message from JetStream
   * @param stream - the name of the stream
   * @param consumer - the consumer's durable name (if durable) or name if ephemeral
   * @param expires - the number of milliseconds to wait for a message
   * @deprecated - use {@link Consumer#fetch()}
   */
  pull(stream: string, consumer: string, expires?: number): Promise<JsMsg>;

  /**
   * Similar to pull, but able to configure the number of messages, etc. via PullOptions.
   * @param stream - the name of the stream
   * @param durable - the consumer's durable name (if durable) or name if ephemeral
   * @param opts
   * @deprecated - use {@link Consumer#fetch()}
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
   * @deprecated - use {@link Consumer#fetch()} or {@link Consumer#consume()}
   */
  pullSubscribe(
    subject: string,
    opts: ConsumerOptsBuilder | Partial<ConsumerOpts>,
  ): Promise<JetStreamPullSubscription>;

  /**
   * Creates a push subscription. The JetStream server feeds messages to this subscription
   * without the client having to request them. The rate at which messages are provided can
   * be tuned by the consumer by specifying {@link ConsumerConfig#rate_limit_bps | ConsumerConfig.rate_limit_bps} and/or
   * {@link ConsumerOpts | maxAckPending}.
   *
   * It is recommended that a consumer be created first using JetStreamManager APIs and then
   * use the bind option to simply attach to the created consumer.
   *
   * If the filter subject is not specified in the options, the filter will be set to match
   * the specified subject.
   *
   * @param subject - a subject used to locate the stream
   * @param opts
   * @deprecated - use {@link Consumer#fetch()} or {@link Consumer#consume()}
   */
  subscribe(
    subject: string,
    opts: ConsumerOptsBuilder | Partial<ConsumerOpts>,
  ): Promise<JetStreamSubscription>;

  /**
   * Accessor for the JetStream materialized views API
   */
  views: Views;

  /**
   * Returns the JS API prefix as processed from the JetStream Options
   */
  apiPrefix: string;

  /**
   * Returns the interface for accessing {@link Consumers}. Consumers
   * allow you to process messages stored in a stream. To create a
   * consumer use {@link JetStreamManager}.
   */
  consumers: Consumers;

  /**
   * Returns the interface for accessing {@link Streams}.
   */
  streams: Streams;

  /**
   * Returns a JetStreamManager that uses the same {@link JetStreamOptions}
   * as the current JetStream context
   */
  jetstreamManager(checkAPI?: boolean): Promise<JetStreamManager>;

  getOptions(): JetStreamOptions;
}

export interface Streams {
  get(stream: string): Promise<Stream>;
}

export interface Consumers {
  /**
   * Returns the Consumer configured for the specified stream having the specified name.
   * Consumers are typically created with {@link JetStreamManager}. If no name is specified,
   * the Consumers API will return an ordered consumer.
   *
   * An ordered consumer expects messages to be delivered in order. If there's
   * any inconsistency, the ordered consumer will recreate the underlying consumer at the
   * correct sequence. Note that ordered consumers don't yield messages that can be acked
   * because the client can simply recreate the consumer.
   *
   * {@link Consumer}.
   * @param stream
   * @param name or OrderedConsumerOptions - if not specified an ordered consumer is created
   *  with the specified options.
   */
  get(
    stream: string,
    name?: string | Partial<OrderedConsumerOptions>,
  ): Promise<Consumer>;
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
   * Messages delivered to the consumer will not have a payload. Instead,
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
   * This api can be called multiple times.
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
   * Specify the name of the stream, avoiding a lookup where the stream is located by
   * searching for a subject.
   * @param stream
   */
  bindStream(stream: string): this;

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

  /**
   * The raw ISO formatted date returned by the server
   */
  timestamp: string;

  /**
   * Convenience method to parse the message payload as JSON. This method
   * will throw an exception if there's a parsing error;
   * @param reviver
   */
  json<T>(reviver?: ReviverFn): T;

  /**
   * Convenience method to parse the message payload as string. This method
   * may throw an exception if there's a conversion error
   */
  string(): string;
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

export interface Stream {
  name: string;

  info(
    cached?: boolean,
    opts?: Partial<StreamInfoRequestOptions>,
  ): Promise<StreamInfo>;

  alternates(): Promise<StreamAlternate[]>;

  best(): Promise<Stream>;

  getConsumer(
    name?: string | Partial<OrderedConsumerOptions>,
  ): Promise<Consumer>;

  getMessage(query: MsgRequest): Promise<StoredMsg>;

  deleteMessage(seq: number, erase?: boolean): Promise<boolean>;
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
  /**
   * Set on protocol messages to indicate pull request message count that
   * was not honored.
   */
  PendingMessagesHdr = "Nats-Pending-Messages",
  /**
   * Set on protocol messages to indicate pull request byte count that
   * was not honored
   */
  PendingBytesHdr = "Nats-Pending-Bytes",
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

  /**
   * Convenience method to parse the entry payload as JSON. This method
   * will throw an exception if there's a parsing error;
   */
  json<T>(): T;

  /**
   * Convenience method to parse the entry payload as string. This method
   * may throw an exception if there's a conversion error
   */
  string(): string;
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
   * keys older than this amount. Note that deletion of
   * delete markers are not performed.
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
   * Maintains a 1:1 mirror of another kv stream with name matching this property.
   */
  mirror?: StreamSource;
  /**
   * List of Stream names to replicate into this KV
   */
  sources?: StreamSource[];
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

  /**
   * The StreamInfo backing up the KV
   */
  streamInfo: StreamInfo;

  /**
   * Size of the bucket in bytes
   */
  size: number;
  /**
   * Metadata field to store additional information about the stream. Note that
   * keys starting with `_nats` are reserved. This feature only supported on servers
   * 2.10.x and better.
   */
  metadata?: Record<string, string>;
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
  /**
   * Metadata field to store additional information about the kv. Note that
   * keys starting with `_nats` are reserved. This feature only supported on servers
   * 2.10.x and better.
   */
  metadata?: Record<string, string>;
}

/**
 * @deprecated use purge(k)
 */
export interface KvRemove {
  remove(k: string): Promise<void>;
}

export enum KvWatchInclude {
  /**
   * Include the last value for all the keys
   */
  LastValue = "",
  /**
   * Include all available history for all keys
   */
  AllHistory = "history",
  /**
   * Don't include history or last values, only notify
   * of updates
   */
  UpdatesOnly = "updates",
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
   */
  initializedFn?: () => void;
  /**
   * Skips notifying deletes.
   * @default: false
   */
  ignoreDeletes?: boolean;
  /**
   * Specify what to include in the watcher, by default all last values.
   */
  include?: KvWatchInclude;
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
   * Creates a new entry ensuring that the entry does not exist (or
   * the current version is deleted or the key is purged)
   * If the entry already exists, this operation fails.
   * @param k
   * @param data
   */
  create(k: string, data: Payload): Promise<number>;

  /**
   * Updates the existing entry provided that the previous sequence
   * for the Kv is at the specified version. This ensures that the
   * KV has not been modified prior to the update.
   * @param k
   * @param data
   * @param version
   */
  update(k: string, data: Payload, version: number): Promise<number>;

  /**
   * Sets or updates the value stored under the specified key.
   * @param k
   * @param data
   * @param opts
   */
  put(
    k: string,
    data: Payload,
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
  /**
   * name of object store storing the data
   */
  bucket: string;
  /**
   * link to single object, when empty this means the whole store
   */
  name?: string;
};
export type ObjectStoreMetaOptions = {
  /**
   * If set, the object is a reference to another entry.
   */
  link?: ObjectStoreLink;
  /**
   * The maximum size in bytes for each chunk.
   * Note that if the size exceeds the maximum size of a stream
   * entry, the number will be clamped to the streams maximum.
   */
  max_chunk_size?: number;
};
export type ObjectStoreMeta = {
  name: string;
  description?: string;
  headers?: MsgHdrs;
  options?: ObjectStoreMetaOptions;
  metadata?: Record<string, string>;
};

export interface ObjectInfo extends ObjectStoreMeta {
  /**
   * The name of the bucket where the object is stored.
   */
  bucket: string;
  /**
   * The current ID of the entries holding the data for the object.
   */
  nuid: string;
  /**
   * The size in bytes of the object.
   */
  size: number;
  /**
   * The number of entries storing the object.
   */
  chunks: number;
  /**
   * A cryptographic checksum of the data as a whole.
   */
  digest: string;
  /**
   * True if the object was deleted.
   */
  deleted: boolean;
  /**
   * An UTC timestamp
   */
  mtime: string;
  /**
   * The revision number for the entry
   */
  revision: number;
}

/**
 * A link reference
 */
export interface ObjectLink {
  /**
   * The object store the source data
   */
  bucket: string;
  /**
   * The name of the entry holding the data. If not
   * set it is a complete object store reference.
   */
  name?: string;
}

export type ObjectStoreStatus = {
  /**
   * The bucket name
   */
  bucket: string;
  /**
   * the description associated with the object store.
   */
  description: string;
  /**
   * The time to live for entries in the object store in nanoseconds.
   * Convert to millis using the `millis()` function.
   */
  ttl: Nanos;
  /**
   * The object store's underlying stream storage type.
   */
  storage: StorageType;
  /**
   * The number of replicas associated with this object store.
   */
  replicas: number;
  /**
   * Set to true if the object store is sealed and will reject edits.
   */
  sealed: boolean;
  /**
   * The size in bytes that the object store occupies.
   */
  size: number;
  /**
   * The underlying storage for the object store. Currently, this always
   * returns "JetStream".
   */
  backingStore: string;
  /**
   * The StreamInfo backing up the ObjectStore
   */
  streamInfo: StreamInfo;
  /**
   * Metadata the object store. Note that
   * keys starting with `_nats` are reserved. This feature only supported on servers
   * 2.10.x and better.
   */
  metadata?: Record<string, string> | undefined;
};
/**
 * @deprecated {@link ObjectStoreStatus}
 */
export type ObjectStoreInfo = ObjectStoreStatus;
export type ObjectStoreOptions = {
  /**
   * A description for the object store
   */
  description?: string;
  /**
   * The time to live for entries in the object store specified
   * as nanoseconds. Use the `nanos()` function to convert millis to
   * nanos.
   */
  ttl?: Nanos;
  /**
   * The underlying stream storage type for the object store.
   */
  storage: StorageType;
  /**
   * The number of replicas to create.
   */
  replicas: number;
  /**
   * The maximum amount of data that the object store should store in bytes.
   */
  "max_bytes": number;
  /**
   * Placement hints for the underlying object store stream
   */
  placement: Placement; /**
   * Metadata field to store additional information about the stream. Note that
   * keys starting with `_nats` are reserved. This feature only supported on servers
   * 2.10.x and better.
   */
  metadata?: Record<string, string>;
};
/**
 * An object that allows reading the object stored under a specified name.
 */
export type ObjectResult = {
  /**
   * The info of the object that was retrieved.
   */
  info: ObjectInfo;
  /**
   * The readable stream where you can read the data.
   */
  data: ReadableStream<Uint8Array>;
  /**
   * A promise that will resolve to an error if the readable stream failed
   * to process the entire response. Should be checked when the readable stream
   * has finished yielding data.
   */
  error: Promise<Error | null>;
};
export type ObjectStorePutOpts = {
  /**
   * maximum number of millis for the put requests to succeed
   */
  timeout?: number;
  /**
   * If set the ObjectStore must be at the current sequence or the
   * put will fail. Note the sequence accounts where the metadata
   * for the entry is stored.
   */
  previousRevision?: number;
};

export interface ObjectStore {
  /**
   * Returns the ObjectInfo of the named entry. Or null if the
   * entry doesn't exist.
   * @param name
   */
  info(name: string): Promise<ObjectInfo | null>;
  /**
   * Returns a list of the entries in the ObjectStore
   */
  list(): Promise<ObjectInfo[]>;
  /**
   * Returns an object you can use for reading the data from the
   * named stored object or null if the entry doesn't exist.
   * @param name
   */
  get(name: string): Promise<ObjectResult | null>;
  /**
   * Returns the data stored for the named entry.
   * @param name
   */
  getBlob(name: string): Promise<Uint8Array | null>;
  /**
   * Adds an object to the store with the specified meta
   * and using the specified ReadableStream to stream the data.
   * @param meta
   * @param rs
   * @param opts
   */
  put(
    meta: ObjectStoreMeta,
    rs: ReadableStream<Uint8Array>,
    opts?: ObjectStorePutOpts,
  ): Promise<ObjectInfo>;
  /**
   * Puts the specified bytes into the store with the specified meta.
   * @param meta
   * @param data
   * @param opts
   */
  putBlob(
    meta: ObjectStoreMeta,
    data: Uint8Array | null,
    opts?: ObjectStorePutOpts,
  ): Promise<ObjectInfo>;
  /**
   * Deletes the specified entry from the object store.
   * @param name
   */
  delete(name: string): Promise<PurgeResponse>;

  /**
   * Adds a link to another object in the same store or a different one.
   * Note that links of links are rejected.
   * object.
   * @param name
   * @param meta
   */
  link(name: string, meta: ObjectInfo): Promise<ObjectInfo>;

  /**
   * Add a link to another object store
   * @param name
   * @param bucket
   */
  linkStore(name: string, bucket: ObjectStore): Promise<ObjectInfo>;
  /**
   * Watch an object store and receive updates of modifications via
   * an iterator.
   * @param opts
   */
  watch(
    opts?: Partial<
      {
        ignoreDeletes?: boolean;
        includeHistory?: boolean;
      }
    >,
  ): Promise<QueuedIterator<ObjectInfo | null>>;
  /**
   * Seals the object store preventing any further modifications.
   */
  seal(): Promise<ObjectStoreStatus>;
  /**
   * Returns the runtime status of the object store.
   * @param opts
   */
  status(opts?: Partial<StreamInfoRequestOptions>): Promise<ObjectStoreStatus>;

  /**
   * Update the metadata for an object. If the name is modified, the object
   * is effectively renamed and will only be accessible by its new name.
   * @param name
   * @param meta
   */
  update(name: string, meta: Partial<ObjectStoreMeta>): Promise<PubAck>;
  /**
   * Destroys the object store and all its entries.
   */
  destroy(): Promise<boolean>;
}

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
   * The size in bytes of the message's body - Only if {@link Republish#headers_only} is set.
   */
  Size = "Nats-Msg-Size",
}

export const kvPrefix = "KV_";

export interface JetStreamSubscriptionInfoable {
  info: JetStreamSubscriptionInfo | null;
}

export interface JetStreamSubscriptionInfo extends ConsumerOpts {
  api: BaseApiClient;
  last: ConsumerInfo;
  attached: boolean;
  deliver: string;
  bind: boolean;
  "ordered_consumer_sequence": { "delivery_seq": number; "stream_seq": number };
  "flow_control": {
    "heartbeat_count": number;
    "fc_count": number;
    "consumer_restarts": number;
  };
}

// FIXME: some items here that may need to be addressed
// 503s?
// maxRetries()
// retryBackoff()
// ackWait(time)
// replayOriginal()
// rateLimit(bytesPerSec)
export class ConsumerOptsBuilderImpl implements ConsumerOptsBuilder {
  config: Partial<ConsumerConfig>;
  ordered: boolean;
  mack: boolean;
  stream: string;
  callbackFn?: JsMsgCallback;
  max?: number;
  qname?: string;
  isBind?: boolean;
  filters?: string[];

  constructor(opts?: Partial<ConsumerConfig>) {
    this.stream = "";
    this.mack = false;
    this.ordered = false;
    this.config = defaultConsumer("", opts || {});
  }

  getOpts(): ConsumerOpts {
    const o = {} as ConsumerOpts;
    o.config = Object.assign({}, this.config);
    if (o.config.filter_subject) {
      this.filterSubject(o.config.filter_subject);
      o.config.filter_subject = undefined;
    }
    if (o.config.filter_subjects) {
      o.config.filter_subjects?.forEach((v) => {
        this.filterSubject(v);
      });
      o.config.filter_subjects = undefined;
    }

    o.mack = this.mack;
    o.stream = this.stream;
    o.callbackFn = this.callbackFn;
    o.max = this.max;
    o.queue = this.qname;
    o.ordered = this.ordered;
    o.config.ack_policy = o.ordered ? AckPolicy.None : o.config.ack_policy;
    o.isBind = o.isBind || false;

    if (this.filters) {
      switch (this.filters.length) {
        case 0:
          break;
        case 1:
          o.config.filter_subject = this.filters[0];
          break;
        default:
          o.config.filter_subjects = this.filters;
      }
    }
    return o;
  }

  description(description: string) {
    this.config.description = description;
    return this;
  }

  deliverTo(subject: string) {
    this.config.deliver_subject = subject;
    return this;
  }

  durable(name: string) {
    validateDurableName(name);
    this.config.durable_name = name;
    return this;
  }

  startSequence(seq: number) {
    if (seq <= 0) {
      throw new Error("sequence must be greater than 0");
    }
    this.config.deliver_policy = DeliverPolicy.StartSequence;
    this.config.opt_start_seq = seq;
    return this;
  }

  startTime(time: Date) {
    this.config.deliver_policy = DeliverPolicy.StartTime;
    this.config.opt_start_time = time.toISOString();
    return this;
  }

  deliverAll() {
    this.config.deliver_policy = DeliverPolicy.All;
    return this;
  }

  deliverLastPerSubject() {
    this.config.deliver_policy = DeliverPolicy.LastPerSubject;
    return this;
  }

  deliverLast() {
    this.config.deliver_policy = DeliverPolicy.Last;
    return this;
  }

  deliverNew() {
    this.config.deliver_policy = DeliverPolicy.New;
    return this;
  }

  startAtTimeDelta(millis: number) {
    this.startTime(new Date(Date.now() - millis));
    return this;
  }

  headersOnly() {
    this.config.headers_only = true;
    return this;
  }

  ackNone() {
    this.config.ack_policy = AckPolicy.None;
    return this;
  }

  ackAll() {
    this.config.ack_policy = AckPolicy.All;
    return this;
  }

  ackExplicit() {
    this.config.ack_policy = AckPolicy.Explicit;
    return this;
  }

  ackWait(millis: number) {
    this.config.ack_wait = nanos(millis);
    return this;
  }

  maxDeliver(max: number) {
    this.config.max_deliver = max;
    return this;
  }

  filterSubject(s: string) {
    this.filters = this.filters || [];
    this.filters.push(s);
    return this;
  }

  replayInstantly() {
    this.config.replay_policy = ReplayPolicy.Instant;
    return this;
  }

  replayOriginal() {
    this.config.replay_policy = ReplayPolicy.Original;
    return this;
  }

  sample(n: number) {
    n = Math.trunc(n);
    if (n < 0 || n > 100) {
      throw new Error(`value must be between 0-100`);
    }
    this.config.sample_freq = `${n}%`;
    return this;
  }

  limit(n: number) {
    this.config.rate_limit_bps = n;
    return this;
  }

  maxWaiting(max: number) {
    this.config.max_waiting = max;
    return this;
  }

  maxAckPending(max: number) {
    this.config.max_ack_pending = max;
    return this;
  }

  idleHeartbeat(millis: number) {
    this.config.idle_heartbeat = nanos(millis);
    return this;
  }

  flowControl() {
    this.config.flow_control = true;
    return this;
  }

  deliverGroup(name: string) {
    this.queue(name);
    return this;
  }

  manualAck() {
    this.mack = true;
    return this;
  }

  maxMessages(max: number) {
    this.max = max;
    return this;
  }

  callback(fn: JsMsgCallback) {
    this.callbackFn = fn;
    return this;
  }

  queue(n: string) {
    this.qname = n;
    this.config.deliver_group = n;
    return this;
  }

  orderedConsumer() {
    this.ordered = true;
    return this;
  }

  bind(stream: string, durable: string) {
    this.stream = stream;
    this.config.durable_name = durable;
    this.isBind = true;
    return this;
  }

  bindStream(stream: string) {
    this.stream = stream;
    return this;
  }

  inactiveEphemeralThreshold(millis: number) {
    this.config.inactive_threshold = nanos(millis);
    return this;
  }

  maxPullBatch(n: number) {
    this.config.max_batch = n;
    return this;
  }

  maxPullRequestExpires(millis: number) {
    this.config.max_expires = nanos(millis);
    return this;
  }

  memory() {
    this.config.mem_storage = true;
    return this;
  }

  numReplicas(n: number) {
    this.config.num_replicas = n;
    return this;
  }
}

export function consumerOpts(
  opts?: Partial<ConsumerConfig>,
): ConsumerOptsBuilder {
  return new ConsumerOptsBuilderImpl(opts);
}

export function isConsumerOptsBuilder(
  o: ConsumerOptsBuilder | Partial<ConsumerOpts>,
): o is ConsumerOptsBuilderImpl {
  return typeof (o as ConsumerOptsBuilderImpl).getOpts === "function";
}

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
export {
  checkJsError,
  isFlowControlMsg,
  isHeartbeatMsg,
  millis,
  nanos,
} from "./jsutil.ts";

export {
  AdvisoryKind,
  consumerOpts,
  DirectMsgHeaders,
  isConsumerOptsBuilder,
  JsHeaders,
  RepublishHeaders,
} from "./types.ts";

export type {
  Advisory,
  Closed,
  ConsumerInfoable,
  ConsumerOpts,
  ConsumerOptsBuilder,
  Consumers,
  Destroyable,
  JetStreamClient,
  JetStreamManager,
  JetStreamManagerOptions,
  JetStreamOptions,
  JetStreamPublishOptions,
  JetStreamPullSubscription,
  JetStreamSubscription,
  JetStreamSubscriptionInfoable,
  JetStreamSubscriptionOptions,
  JsMsgCallback,
  KV,
  KvCodec,
  KvCodecs,
  KvEntry,
  KvLimits,
  KvOptions,
  KvPutOptions,
  KvStatus,
  KvWatchInclude,
  KvWatchOptions,
  ObjectInfo,
  ObjectResult,
  ObjectStore,
  ObjectStoreLink,
  ObjectStoreMeta,
  ObjectStoreMetaOptions,
  ObjectStoreOptions,
  ObjectStorePutOpts,
  ObjectStoreStatus,
  PubAck,
  Pullable,
  RoKV,
  StoredMsg,
  Stream,
  StreamAPI,
  Streams,
  Views,
} from "./types.ts";

export type { StreamNames } from "./jsbaseclient_api.ts";
export type {
  AccountLimits,
  ApiPagedRequest,
  ClusterInfo,
  ConsumerConfig,
  ConsumerInfo,
  ConsumerUpdateConfig,
  ExternalStream,
  JetStreamAccountStats,
  JetStreamApiStats,
  JetStreamUsageAccountLimits,
  LastForMsgRequest,
  LostStreamData,
  MsgDeleteRequest,
  MsgRequest,
  PeerInfo,
  Placement,
  PullOptions,
  PurgeBySeq,
  PurgeBySubject,
  PurgeOpts,
  PurgeResponse,
  PurgeTrimOpts,
  Republish,
  SeqMsgRequest,
  SequenceInfo,
  StreamAlternate,
  StreamConfig,
  StreamConsumerLimits,
  StreamInfo,
  StreamSource,
  StreamSourceInfo,
  StreamState,
  StreamUpdateConfig,
  SubjectTransformConfig,
} from "./jsapi_types.ts";

export type { JsMsg } from "./jsmsg.ts";
export type { Lister } from "./jslister.ts";

export {
  AckPolicy,
  DeliverPolicy,
  DiscardPolicy,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
  StoreCompression,
} from "./jsapi_types.ts";

export type { ConsumerAPI } from "./jsmconsumer_api.ts";
export type { DeliveryInfo, StreamInfoRequestOptions } from "./jsapi_types.ts";

export type {
  ConsumeBytes,
  ConsumeCallback,
  ConsumeMessages,
  ConsumeOptions,
  Consumer,
  ConsumerCallbackFn,
  ConsumerMessages,
  ConsumerStatus,
  Expires,
  FetchBytes,
  FetchMessages,
  FetchOptions,
  IdleHeartbeat,
  MaxBytes,
  MaxMessages,
  OrderedConsumerOptions,
  ThresholdBytes,
  ThresholdMessages,
} from "./consumer.ts";
export { ConsumerDebugEvents, ConsumerEvents } from "./consumer.ts";

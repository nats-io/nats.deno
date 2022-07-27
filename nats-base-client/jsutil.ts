/*
 * Copyright 2021 The NATS Authors
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
import {
  AckPolicy,
  ConsumerConfig,
  DeliverPolicy,
  Msg,
  Nanos,
  ReplayPolicy,
} from "./types.ts";
import { ErrorCode, NatsError } from "./error.ts";

export function validateDurableName(name?: string) {
  return validateName("durable", name);
}

export function validateStreamName(name?: string) {
  return validateName("stream", name);
}

export function validateName(context: string, name = "") {
  if (name === "") {
    throw Error(`${context} name required`);
  }
  const bad = [".", "*", ">"];
  bad.forEach((v) => {
    if (name.indexOf(v) !== -1) {
      throw Error(
        `invalid ${context} name - ${context} name cannot contain '${v}'`,
      );
    }
  });
}

export function defaultConsumer(
  name: string,
  opts: Partial<ConsumerConfig> = {},
): ConsumerConfig {
  return Object.assign({
    name: name,
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
    ack_wait: nanos(30 * 1000),
    replay_policy: ReplayPolicy.Instant,
  }, opts);
}

/**
 * Converts the specified millis into Nanos
 * @param millis
 */
export function nanos(millis: number): Nanos {
  return millis * 1000000;
}

/**
 * Convert the specified Nanos into millis
 * @param ns
 */
export function millis(ns: Nanos) {
  return Math.floor(ns / 1000000);
}

/**
 * Returns true if the message is a flow control message
 * @param msg
 */
export function isFlowControlMsg(msg: Msg): boolean {
  if (msg.data.length > 0) {
    return false;
  }
  const h = msg.headers;
  if (!h) {
    return false;
  }
  return h.code >= 100 && h.code < 200;
}

/**
 * Returns true if the message is a heart beat message
 * @param msg
 */
export function isHeartbeatMsg(msg: Msg): boolean {
  return isFlowControlMsg(msg) && msg.headers?.description === "Idle Heartbeat";
}

export function checkJsError(msg: Msg): NatsError | null {
  // JS error only if no payload - otherwise assume it is application data
  if (msg.data.length !== 0) {
    return null;
  }
  const h = msg.headers;
  if (!h) {
    return null;
  }
  return checkJsErrorCode(h.code, h.description);
}

export enum Js409Errors {
  MaxBatchExceeded = "exceeded maxrequestbatch of",
  MaxExpiresExceeded = "exceeded maxrequestexpires of",
  MaxBytesExceeded = "exceeded maxrequestmaxbytes of",
  MaxMessageSizeExceeded = "message size exceeds maxbytes",
  PushConsumer = "consumer is push based",
  MaxWaitingExceeded = "exceeded maxwaiting", // not terminal
}

let MAX_WAITING_FAIL = false;
export function setMaxWaitingToFail(tf: boolean) {
  MAX_WAITING_FAIL = tf;
}

export function isTerminal409(err: NatsError): boolean {
  if (err.code !== ErrorCode.JetStream409) {
    return false;
  }
  const fatal = [
    Js409Errors.MaxBatchExceeded,
    Js409Errors.MaxExpiresExceeded,
    Js409Errors.MaxBytesExceeded,
    Js409Errors.MaxMessageSizeExceeded,
    Js409Errors.PushConsumer,
  ];
  if (MAX_WAITING_FAIL) {
    fatal.push(Js409Errors.MaxWaitingExceeded);
  }

  return fatal.find((s) => {
    return err.message.indexOf(s) !== -1;
  }) !== undefined;
}

export function checkJsErrorCode(
  code: number,
  description = "",
): NatsError | null {
  if (code < 300) {
    return null;
  }
  description = description.toLowerCase();
  switch (code) {
    case 404:
      // 404 for jetstream will provide different messages ensure we
      // keep whatever the server returned
      return new NatsError(description, ErrorCode.JetStream404NoMessages);
    case 408:
      return new NatsError(description, ErrorCode.JetStream408RequestTimeout);
    case 409:
      // the description can be exceeded max waiting or max ack pending, which are
      // recoverable, but can also be terminal errors where the request exceeds
      // some value in the consumer configuration
      return new NatsError(
        description,
        ErrorCode.JetStream409,
      );
    case 503:
      return NatsError.errorForCode(
        ErrorCode.JetStreamNotEnabled,
        new Error(description),
      );
    default:
      if (description === "") {
        description = ErrorCode.Unknown;
      }
      return new NatsError(description, `${code}`);
  }
}

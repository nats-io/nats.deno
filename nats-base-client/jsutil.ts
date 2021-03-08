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
  Nanos,
  ReplayPolicy,
} from "./types.ts";

export function validateDurableName(name?: string) {
  return validateName("durable", name);
}

export function validateStreamName(name?: string) {
  return validateName("stream", name);
}

function validateName(context: string, name = "") {
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
    ack_wait: ns(30 * 1000),
    replay_policy: ReplayPolicy.Instant,
  }, opts);
}

// export function defaultPushConsumer(
//   name: string,
//   deliverSubject: string,
//   opts: Partial<ConsumerConfig> = {},
// ): PushConsumerConfig {
//   return Object.assign(defaultConsumer(name), {
//     deliver_subject: deliverSubject,
//   }, opts);
// }

// export function ephemeralConsumer(
//   stream: string,
//   cfg: Partial<ConsumerConfig> = {},
// ): PushConsumer {
//   validateStreamName(stream);
//   if (cfg.durable_name) {
//     throw new Error("ephemeral subscribers cannot be durable");
//   }
//   cfg.name = cfg.name ? cfg.name : nuid.next();
//   const deliver = cfg.deliver_subject ? cfg.deliver_subject : createInbox();
//   const c = defaultPushConsumer(cfg.name, deliver, cfg);
//   return { stream_name: stream, config: c } as PushConsumer;
// }
//
// export function pushConsumer(
//   stream: string,
//   cfg: Partial<ConsumerConfig> = {},
// ): Consumer {
//   validateStreamName(stream);
//   if (!cfg.durable_name) {
//     throw new Error("durable_name is required");
//   }
//   if (!cfg.deliver_subject) {
//     throw new Error("deliver_subject is required");
//   }
//   cfg.name = cfg.name ? cfg.name : nuid.next();
//   const c = defaultPushConsumer(cfg.name, cfg.durable_name, cfg);
//   return { stream_name: stream, config: c };
// }

export function ns(millis: number): Nanos {
  return millis * 1000000;
}

export function ms(ns: Nanos) {
  return ns / 1000000;
}

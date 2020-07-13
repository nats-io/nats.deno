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

import { extend } from "./util.ts";
import { ErrorCode, NatsError } from "./error.ts";
import {
  ConnectionOptions,
  DEFAULT_JITTER,
  DEFAULT_JITTER_TLS,
  DEFAULT_MAX_PING_OUT,
  DEFAULT_MAX_RECONNECT_ATTEMPTS,
  DEFAULT_PING_INTERVAL,
  DEFAULT_PRE,
  DEFAULT_RECONNECT_TIME_WAIT,
  DEFAULT_URI,
  Payload,
} from "./types.ts";

export function defaultOptions(): ConnectionOptions {
  return {
    maxPingOut: DEFAULT_MAX_PING_OUT,
    maxReconnectAttempts: DEFAULT_MAX_RECONNECT_ATTEMPTS,
    noRandomize: false,
    payload: Payload.STRING,
    pedantic: false,
    pingInterval: DEFAULT_PING_INTERVAL,
    reconnect: true,
    reconnectJitter: DEFAULT_JITTER,
    reconnectJitterTLS: DEFAULT_JITTER_TLS,
    reconnectTimeWait: DEFAULT_RECONNECT_TIME_WAIT,
    tls: undefined,
    verbose: false,
    waitOnFirstConnect: false,
  } as ConnectionOptions;
}

export function parseOptions(opts?: ConnectionOptions): ConnectionOptions {
  opts = opts || { url: DEFAULT_URI };
  if (opts.port) {
    opts.url = DEFAULT_PRE + opts.port;
  }

  let options = extend(defaultOptions(), opts);
  if (options.user && options.token) {
    throw NatsError.errorForCode(ErrorCode.BAD_AUTHENTICATION);
  }

  let payloadTypes = [Payload.JSON, Payload.STRING, Payload.BINARY];
  if (opts.payload && !payloadTypes.includes(opts.payload)) {
    throw NatsError.errorForCode(ErrorCode.INVALID_PAYLOAD_TYPE);
  }

  ["nonceSigner", "reconnectDelayHandler"].forEach((n) => {
    if (options[n] && typeof options[n] !== "function") {
      throw new NatsError(
        `${n} option should be a function`,
        ErrorCode.NOT_FUNC,
      );
    }
  });

  if (!options.reconnectDelayHandler) {
    options.reconnectDelayHandler = () => {
      let extra = options.tls
        ? options.reconnectJitterTLS
        : options.reconnectJitter;
      if (extra) {
        extra++;
        extra = Math.floor(Math.random() * extra);
      }
      return options.reconnectTimeWait + extra;
    };
  }

  return options;
}

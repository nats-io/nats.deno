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

import { ErrorCode, NatsError } from "./error.ts";

export interface Codec<T> {
  encode(d: T): Uint8Array;
  decode(a: Uint8Array): T;
}

export function StringCodec(): Codec<string> {
  const te = new TextEncoder();
  const td = new TextDecoder();
  return {
    encode(d: any): Uint8Array {
      return te.encode(d);
    },
    decode(a: Uint8Array): any {
      return td.decode(a);
    },
  };
}

export function JSONCodec(): Codec<any> {
  const te = new TextEncoder();
  const td = new TextDecoder();
  return {
    encode(d: any): Uint8Array {
      try {
        if (d === undefined) {
          d = null;
        }
        return te.encode(JSON.stringify(d));
      } catch (err) {
        throw NatsError.errorForCode(ErrorCode.BAD_JSON, err);
      }
    },
    //@ts-ignore
    decode(a: Uint8Array): any {
      try {
        return JSON.parse(td.decode(a));
      } catch (err) {
        throw NatsError.errorForCode(ErrorCode.BAD_JSON, err);
      }
    },
  };
}

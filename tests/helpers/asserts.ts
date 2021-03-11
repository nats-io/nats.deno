/*
 * Copyright 2020-2021 The NATS Authors
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
  assert,
  assertThrows,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import { isNatsError, NatsError } from "../../nats-base-client/error.ts";

export function assertErrorCode(err: Error, ...codes: string[]) {
  if (isNatsError(err)) {
    const { code } = err as NatsError;
    assert(code);
    const ok = codes.find((c) => {
      return code.indexOf(c) !== -1;
    });
    if (ok === "") {
      fail(`got ${code} - expected any of [${codes.join(", ")}]`);
    }
  } else {
    fail(`didn't get a nats error - got: ${err.message}`);
  }
}

export function assertThrowsErrorCode<T = void>(
  fn: () => T,
  ...codes: string[]
) {
  const err = assertThrows(fn);
  assertErrorCode(err, ...codes);
}

export async function assertThrowsAsyncErrorCode<T = void>(
  fn: () => Promise<T>,
  ...codes: string[]
) {
  const err = await assertThrowsAsync(fn);
  assertErrorCode(err, ...codes);
}

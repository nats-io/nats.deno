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
  fail,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { isNatsError, NatsError } from "../../nats-base-client/error.ts";

export function assertErrorCode(err?: Error, ...codes: string[]) {
  if (!err) {
    fail(`expected an error to be thrown`);
  }
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
  assertThrows(fn, (err: Error) => {
    assertErrorCode(err, ...codes);
  });
}

export async function assertThrowsAsyncErrorCode<T = void>(
  fn: () => Promise<T>,
  ...codes: string[]
) {
  try {
    await fn();
    fail("expected to throw");
  } catch (err) {
    assertErrorCode(err, ...codes);
  }
}

export function assertBetween(n: number, low: number, high: number) {
  console.assert(n >= low, `${n} >= ${low}`);
  console.assert(n <= high, `${n} <= ${low}`);
}

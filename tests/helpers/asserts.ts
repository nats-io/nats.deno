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

import { assert, fail } from "https://deno.land/std@0.83.0/testing/asserts.ts";

export function assertErrorCode(err: Error, ...codes: string[]) {
  assert(err, "expected an error");
  const { code } = err as { code?: string };
  if (code === undefined) {
    fail(`didn't get a nats error - got: ${err.message}`);
  }
  assert(code);
  const ok = codes.find((c) => {
    return code.indexOf(c) !== -1;
  });
  if (ok === "") {
    fail(`got ${code} - expected any of [${codes.join(", ")}]`);
  }
}

export function assertThrowsErrorCode(fn: () => unknown, ...codes: string[]) {
  try {
    fn();
    fail("failed to throw error");
  } catch (err) {
    assertErrorCode(err, ...codes);
  }
}

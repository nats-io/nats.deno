/*
 * Copyright 2020-2023 The NATS Authors
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

import { NatsServer } from "./launcher.ts";
import { NatsConnection } from "../../nats-base-client/types.ts";
import { cleanup } from "../jstest_util.ts";
import { compare, parseSemVer } from "../../nats-base-client/semver.ts";
export { check } from "./check.ts";
export { Lock } from "./lock.ts";
import { red, yellow } from "https://deno.land/std@0.177.0/fmt/colors.ts";
export { Connection, TestServer } from "./test_server.ts";
export {
  assertBetween,
  assertErrorCode,
  assertThrowsAsyncErrorCode,
  assertThrowsErrorCode,
} from "./asserts.ts";
export { NatsServer, ServerSignals } from "./launcher.ts";

export function disabled(reason: string): void {
  const m = new TextEncoder().encode(red(`skipping: ${reason} `));
  Deno.stdout.writeSync(m);
}

export async function notCompatible(
  ns: NatsServer,
  nc: NatsConnection,
  version?: string,
): Promise<boolean> {
  version = version ?? "2.3.3";
  const varz = await ns.varz() as unknown as Record<string, string>;
  const sv = parseSemVer(varz.version);
  if (compare(sv, parseSemVer(version)) < 0) {
    const m = new TextEncoder().encode(yellow(
      `skipping test as server (${varz.version}) doesn't implement required feature from ${version} `,
    ));
    await Deno.stdout.write(m);
    await cleanup(ns, nc);
    return true;
  }
  return false;
}

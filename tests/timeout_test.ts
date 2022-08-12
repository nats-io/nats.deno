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
 *
 */
import {
  assertStringIncludes,
  fail,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { connect } from "../src/connect.ts";
import { createInbox, Empty } from "../nats-base-client/mod.ts";

Deno.test("timeout - request noMux stack is useful", async () => {
  const nc = await connect({ servers: "demo.nats.io" });
  const subj = createInbox();
  try {
    await nc.request(subj, Empty, { noMux: true, timeout: 250 });
    fail("request should have failed!");
  } catch (err) {
    assertStringIncludes(err.stack, "timeout_test");
  }
  await nc.close();
});

Deno.test("timeout - request stack is useful", async () => {
  const nc = await connect({ servers: "demo.nats.io" });
  const subj = createInbox();
  try {
    await nc.request(subj, Empty, { timeout: 250 });
    fail("request should have failed!");
  } catch (err) {
    assertStringIncludes(err.stack, "timeout_test");
  }
  await nc.close();
});

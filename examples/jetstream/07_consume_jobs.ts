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

import { connect, JsMsg } from "../../src/mod.ts";
import { setupStreamAndConsumer } from "./util.ts";
import { delay, SimpleMutex } from "../../nats-base-client/util.ts";

// create a connection
const nc = await connect();

// make a stream and fill with messages, and create a consumer
// create a stream with a random name with some messages and a consumer
const { stream, consumer } = await setupStreamAndConsumer(nc, 100);

// retrieve an existing consumer
const js = nc.jetstream();
const c = await js.consumers.get(stream, consumer);

// this example uses a consume that is monitors a rate limiter
// this effectively allows it to process 5 messages as fast
// as it can but stalls the consume if no resources are available
const messages = await c.consume({ max_messages: 10 });

// this rate limiter is just example code, do not use in production
const rl = new SimpleMutex(5);

async function schedule(m: JsMsg): Promise<void> {
  // pretend to do work
  await delay(1000);
  m.ack();
  console.log(`${m.seq}`);
}

for await (const m of messages) {
  await rl.lock();
  schedule(m)
    .catch((err) => {
      console.log(`failed processing: ${err.message}`);
      m.nak();
    })
    .finally(() => {
      rl.unlock();
    });
}

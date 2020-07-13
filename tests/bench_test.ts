/*
 * Copyright 2018 The NATS Authors
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
  connect,
  Nuid,
} from "../src/mod.ts";
import { Lock, NatsServer } from "./helpers/mod.ts";

const u = "nats://demo.nats.io:4222";
const nuid = new Nuid();

let max = 1000;
Deno.test(`bench - pubsub`, async () => {
  const lock = Lock(max, 30000);
  const nc = await connect({ url: u });
  const subj = nuid.next();
  nc.subscribe(subj, {
    callback: () => {
      lock.unlock();
    },
    max: max,
  });
  await nc.flush();
  for (let i = 0; i < max; i++) {
    nc.publish(subj);
  }
  await nc.drain();
  try {
    await nc.close();
  } catch (err) {
    console.log(err);
  }
  await lock;
});

Deno.test(`bench - pubonly`, async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();

  for (let i = 0; i < max; i++) {
    nc.publish(subj);
  }
  await nc.drain();
  await nc.closed();
});

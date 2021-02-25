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

// import the connect function
import { connect, JSONCodec } from "../../src/mod.ts";

// to create a connection to a nats-server:
const nc = await connect({ servers: "demo.nats.io:4222" });

interface Person {
  name: string;
}

// create a codec
const sc = JSONCodec<Person>();

// create a simple subscriber and iterate over messages
// matching the subscription
const sub = nc.subscribe("people");
(async () => {
  for await (const m of sub) {
    // typescript will see this as a Person
    const p = sc.decode(m.data);
    console.log(`[${sub.getProcessed()}]: ${p.name}`);
  }
})();

// if you made a typo or added other properties
// the compiler will get angry
const p = { name: "Memo" } as Person;
nc.publish("people", sc.encode(p));

// finish
await nc.drain();

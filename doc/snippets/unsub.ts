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
import { connect, StringCodec } from "../../src/mod.ts";

// to create a connection to a nats-server:
const nc = await connect({ servers: "demo.nats.io:4222" });
const sc = StringCodec();

// create a couple of subscriptions
// this subscription also illustrates the possible use of callbacks.
const auto = nc.subscribe("hello", {
  callback: (err, msg) => {
    if (err) {
      console.error(err);
      return;
    }
    console.log("auto", auto.getProcessed(), sc.decode(msg.data));
  },
});
// unsubscribe after the first message,
auto.unsubscribe(1);

const manual = nc.subscribe("hello");
// wait for a message that says to stop
const done = (async () => {
  console.log("waiting for a message on `hello` with a payload of `stop`");
  for await (const m of manual) {
    const d = sc.decode(m.data);
    console.log("manual", manual.getProcessed(), d);
    if (d === "stop") {
      manual.unsubscribe();
    }
  }
  console.log("subscription iterator is done");
})();
await done;
await nc.close();

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

// import the connect function
import { connect, StringCodec, Subscription } from "../../src/mod.ts";

// create a connection
const nc = await connect({ url: "demo.nats.io" });

// create a codec
const sc = StringCodec();

// A service is a subscriber that listens for messages, and responds
const started = Date.now();
const sub = nc.subscribe("time");
requestHandler(sub);

// If you wanted to manage a service - well NATS is awesome
// for just that - setup another subscription where admin
// messages can be sent
const msub = nc.subscribe("admin.*");
adminHandler(msub);

// wait for the client to close here.
await nc.closed().then((err?: void | Error) => {
  let m = `connection to ${nc.getServer()} closed`;
  if (err) {
    m = `${m} with an error: ${err.message}`;
  }
  console.log(m);
});

// this implements the public service, and just prints
async function requestHandler(sub: Subscription) {
  console.log(`listening for ${sub.getSubject()} requests...`);
  let serviced = 0;
  for await (const m of sub) {
    serviced++;
    if (m.respond(new Date().toISOString())) {
      console.info(
        `[${serviced}] handled ${m.data ? "- " + sc.decode(m.data) : ""}`,
      );
    } else {
      console.log(`[${serviced}] ignored - no reply subject`);
    }
  }
}

// this implements the admin service
async function adminHandler(sub: Subscription) {
  console.log(`listening for ${sub.getSubject()} requests [uptime | stop]`);

  // it would be very good to verify the origin of the request
  // before implementing something that allows your service to be managed.
  // NATS can limit which client can send or receive on what subjects.
  for await (const m of sub) {
    const chunks = m.subject.split(".");
    console.info(`[admin] handling ${chunks[1]}`);
    switch (chunks[1]) {
      case "uptime":
        // send the number of millis since up
        m.respond(sc.encode(`${Date.now() - started}`));
        break;
      case "stop":
        m.respond("stopping....");
        // finish requests by draining the subscription
        await sub.drain();
        // close the connection
        const _ = nc.close();
        break;
      default:
        console.log(`ignoring request`);
    }
  }
}

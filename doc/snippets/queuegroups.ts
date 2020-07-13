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
import { connect, NatsConnection, Subscription } from "../../src/mod.ts";

async function createService(
  name: string,
  count: number = 1,
  queue: string = "",
): Promise<NatsConnection[]> {
  const conns: NatsConnection[] = [];
  for (let i = 1; i <= count; i++) {
    const n = queue ? `${name}-${i}` : name;
    const nc = await connect(
      { url: "demo.nats.io:4222", name: `${n}` },
    );
    nc.closed()
      .then((err) => {
        if (err) {
          console.error(
            `service ${n} exited because of error: ${err.message}`,
          );
        }
      });
    // create a subscription - note the option for a queue, if set
    // any client with the same queue will be the queue group.
    const sub = nc.subscribe("echo", { queue: queue });
    const _ = handleRequest(n, sub);
    console.log(`${nc.options.name} is listening for 'echo' requests...`);
    conns.push(nc);
  }
  return conns;
}

// simple handler for service requests
async function handleRequest(name: string, s: Subscription) {
  for await (const m of s) {
    // respond returns true if the message had a reply subject, thus it could respond
    if (m.respond(m.data)) {
      console.log(`[${name} - ${s.getReceived()}]: echoed ${m.data}`);
    } else {
      console.log(
        `[${name} - ${s.getReceived()}]: ignoring request - no reply subject`,
      );
    }
  }
}

// let's create two queue groups and a standalone subscriber
const conns: NatsConnection[] = [];
conns.push(...await createService("echo", 3, "echo"));
conns.push(...await createService("other-echo", 2, "other-echo"));
conns.push(...await createService("standalone"));

const a: Promise<void | Error>[] = [];
conns.forEach((c) => {
  a.push(c.closed());
});
await Promise.all(a);

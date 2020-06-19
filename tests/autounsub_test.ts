/*
 * Copyright 2018-2020 The NATS Authors
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
  assertEquals,
} from "https://deno.land/std/testing/asserts.ts";

import {
  ErrorCode,
  Nuid,
  connect,
} from "../src/mod.ts";
import { Lock } from "./helpers/mod.ts";

const u = "https://demo.nats.io:4222";

const nuid = new Nuid();

Deno.test("auto unsub from max from options", async () => {
  let nc = await connect({ url: u });

  let count = 0;
  let subj = nuid.next();
  nc.subscribe(subj, () => {
    count++;
  }, { max: 10 });
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(count, 10);
  await nc.close();
});

Deno.test("auto unsub from unsubscribe", async () => {
  let nc = await connect({ url: u });

  let count = 0;
  let subj = nuid.next();
  let sub = nc.subscribe(subj, () => {
    count++;
  }, { max: 10 });
  sub.unsubscribe(11);
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }

  await nc.flush();
  assertEquals(count, 11);
  await nc.close();
});

Deno.test("can unsub from auto-unsubscribed", async () => {
  let nc = await connect({ url: u });

  let count = 0;
  let subj = nuid.next();
  let sub = nc.subscribe(subj, () => {
    count++;
  }, { max: 1 });
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(count, 1);
  sub.unsubscribe();
  await nc.close();
});

Deno.test("can change auto-unsub to a lesser value", async () => {
  let nc = await connect({ url: u });

  let count = 0;
  let subj = nuid.next();
  let sub = nc.subscribe(subj, () => {
    count++;
    sub.unsubscribe(1);
  });
  sub.unsubscribe(20);
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(count, 1);
  await nc.close();
});

Deno.test("can change auto-unsub to a higher value", async () => {
  let nc = await connect({ url: u });

  let count = 0;
  let subj = nuid.next();
  let sub = nc.subscribe(subj, () => {
    count++;
  });
  sub.unsubscribe(1);
  sub.unsubscribe(10);
  for (let i = 0; i < 20; i++) {
    nc.publish(subj);
  }
  await nc.flush();
  assertEquals(count, 10);
  await nc.close();
});

Deno.test("request receives expected count with multiple helpers", async () => {
  let nc = await connect({ url: u });
  let subj = nuid.next();

  let answers = 0;
  for (let i = 0; i < 5; i++) {
    nc.subscribe(subj, (_, msg) => {
      if (msg.reply) {
        msg.respond();
        answers++;
      }
    });
  }
  await nc.flush();

  let answer = await nc.request(subj);
  await nc.flush();
  assertEquals(answers, 5);
  assert(answer);
  await nc.close();
});

Deno.test("manual request receives expected count with multiple helpers", async () => {
  let nc = await connect({ url: u });
  let requestSubject = nuid.next();

  const lock = Lock(1000, 6);
  for (let i = 0; i < 5; i++) {
    nc.subscribe(requestSubject, (_, msg) => {
      if (msg.reply) {
        msg.respond();
        lock.unlock();
      }
    });
  }

  let replySubj = nuid.next();
  nc.subscribe(replySubj, () => {
    lock.unlock();
  }, { max: 1 });

  // publish the request
  nc.publish(requestSubject, "", replySubj);
  await nc.flush();
  await lock;
  await nc.close();
});

Deno.test("check subscription leaks", async () => {
  let nc = await connect({ url: u });
  let subj = nuid.next();
  let sub = nc.subscribe(subj, () => {
  });
  sub.unsubscribe();
  assertEquals(nc.protocol.subscriptions.length, 0);
  await nc.close();
});

Deno.test("check request leaks", async () => {
  let nc = await connect({ url: u });
  let subj = nuid.next();

  // should have no subscriptions
  assertEquals(nc.protocol.subscriptions.length, 0);

  let sub = nc.subscribe(subj, (_, msg) => {
    if (msg.reply) {
      msg.respond();
    }
  });

  // should have one subscription
  assertEquals(nc.protocol.subscriptions.length, 1);

  let msgs = [];
  msgs.push(nc.request(subj));
  msgs.push(nc.request(subj));

  // should have 2 mux subscriptions, and 2 subscriptions
  assertEquals(nc.protocol.subscriptions.length, 2);
  assertEquals(nc.protocol.muxSubscriptions.length, 2);

  await Promise.all(msgs);

  // mux subs should have pruned
  assertEquals(nc.protocol.muxSubscriptions.length, 0);

  sub.unsubscribe();
  assertEquals(nc.protocol.subscriptions.length, 1);
  await nc.close();
});

Deno.test("check cancelled request leaks", async () => {
  let nc = await connect({ url: u });
  let subj = nuid.next();

  // should have no subscriptions
  assertEquals(nc.protocol.subscriptions.length, 0);

  let rp = nc.request(subj, 100);

  assertEquals(nc.protocol.subscriptions.length, 1);
  assertEquals(nc.protocol.muxSubscriptions.length, 1);

  // the rejection should be timeout
  const lock = Lock();
  rp.catch((rej) => {
    assertEquals(rej?.code, ErrorCode.CONNECTION_TIMEOUT);
    lock.unlock();
  });

  await lock;
  // mux subs should have pruned
  assertEquals(nc.protocol.muxSubscriptions.length, 0);
  await nc.close();
});

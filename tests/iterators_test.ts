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
import { connect, createInbox, ErrorCode, NatsError } from "../src/mod.ts";
import {
  assertEquals,
} from "https://deno.land/std@0.61.0/testing/asserts.ts";
import { assertErrorCode, Lock, NatsServer } from "./helpers/mod.ts";

const u = "demo.nats.io:4222";

Deno.test("iterators - unsubscribe breaks and closes", async () => {
  const nc = await connect({ url: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  const done = (async () => {
    for await (const m of sub) {
      if (sub.getReceived() > 1) {
        sub.unsubscribe();
      }
    }
  })();
  nc.publish(subj);
  nc.publish(subj);
  await done;
  assertEquals(sub.getReceived(), 2);
  await nc.close();
});

Deno.test("iterators - autounsub breaks and closes", async () => {
  const nc = await connect({ url: u });
  const subj = createInbox();
  const sub = nc.subscribe(subj, { max: 2 });
  const lock = Lock(2);
  const done = (async () => {
    for await (const m of sub) {
      lock.unlock();
    }
  })();
  nc.publish(subj);
  nc.publish(subj);
  await done;
  await lock;
  assertEquals(sub.getReceived(), 2);
  await nc.close();
});

Deno.test("iterators - permission error breaks and closes", async () => {
  const conf = {
    authorization: {
      PERM: {
        subscribe: "bar",
        publish: "foo",
      },
      users: [{
        user: "derek",
        password: "foobar",
        permission: "$PERM",
      }],
    },
  };
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    { port: ns.port, user: "derek", pass: "foobar" },
  );
  const sub = nc.subscribe("foo");

  const lock = Lock();
  await (async () => {
    for await (const m of sub) {}
  })().catch(() => {
    lock.unlock();
  });

  await lock;
  await nc.closed().then((err) => {
    assertErrorCode(err as NatsError, ErrorCode.PERMISSIONS_VIOLATION);
  });
  await nc.close();
  await ns.stop();
});

Deno.test("iterators - unsubscribing closes", async () => {
  const nc = await connect(
    { url: u },
  );
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  const lock = Lock();
  const done = (async () => {
    for await (const m of sub) {
      lock.unlock();
    }
  })();
  nc.publish(subj);
  await lock;
  sub.unsubscribe();
  await done;
  await nc.close();
});

Deno.test("iterators - connection close closes", async () => {
  const nc = await connect(
    { url: u },
  );
  const subj = createInbox();
  const sub = nc.subscribe(subj);
  const lock = Lock();
  const done = (async () => {
    for await (const m of sub) {
      lock.unlock();
    }
  })();
  nc.publish(subj);
  await nc.flush();
  await nc.close();
  await lock;
  await done;
});

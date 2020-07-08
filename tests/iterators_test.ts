import { connect } from "../src/connect.ts";
import { delay, ErrorCode, NatsError, Nuid } from "../nats-base-client/mod.ts";
import {
  assert,
  assertEquals,
  assertThrowsAsync,
  fail,
} from "https://deno.land/std/testing/asserts.ts";
import { assertErrorCode, Lock, NatsServer } from "./helpers/mod.ts";
import { timeout } from "../nats-base-client/util.ts";

const u = "demo.nats.io:4222";
const nuid = new Nuid();

Deno.test("iterators - return breaks and closes", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
  const sub = nc.subscribe(subj);
  const done = (async () => {
    for await (const m of sub) {
      if (sub.received > 1) {
        sub.return();
      }
    }
  })();
  nc.publish(subj);
  nc.publish(subj);
  await done;
  assertEquals(sub.received, 2);
  await nc.close();
});

Deno.test("iterators - autounsub breaks and closes", async () => {
  const nc = await connect({ url: u });
  const subj = nuid.next();
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
  assertEquals(sub.received, 2);
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
  await nc.status().then((err) => {
    assertErrorCode(err as NatsError, ErrorCode.PERMISSIONS_VIOLATION);
  });
  await nc.close();
  await ns.stop();
});

Deno.test("iterators - closing closes", async () => {
  const nc = await connect(
    { url: u },
  );
  const subj = nuid.next();
  const sub = nc.subscribe(subj);
  const lock = Lock();
  const done = (async () => {
    for await (const m of sub) {
      lock.unlock();
    }
  })();
  nc.publish(subj);
  await lock;
  sub.close();
  await done;
  await nc.close();
});

Deno.test("iterators - connection close closes", async () => {
  const nc = await connect(
    { url: u },
  );
  const subj = nuid.next();
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

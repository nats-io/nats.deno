import { NatsServer, Lock, ServerSignals } from "../tests/helpers/mod.ts";
import { connect, Events, ServersChanged } from "../src/mod.ts";
import {
  assertEquals,
} from "https://deno.land/std@0.69.0/testing/asserts.ts";
import { delay } from "../nats-base-client/internal_mod.ts";

Deno.test("events - close on close", async () => {
  const ns = await NatsServer.start();
  const nc = await connect(
    { port: ns.port },
  );
  nc.close();
  const status = await nc.closed();
  await ns.stop();
  assertEquals(status, undefined);
});

Deno.test("events - disconnect and close", async () => {
  const lock = Lock(2);
  const ns = await NatsServer.start();
  const nc = await connect(
    { port: ns.port, reconnect: false },
  );
  (async () => {
    for await (const s of nc.status()) {
      switch (s.type) {
        case Events.DISCONNECT:
          lock.unlock();
          break;
      }
    }
  })().then();
  nc.closed().then(() => {
    lock.unlock();
  });
  await ns.stop();
  await lock;

  const v = await nc.closed();

  assertEquals(v, undefined);
});

Deno.test("events - disconnect, reconnect", async () => {
  const cluster = await NatsServer.cluster();
  const nc = await connect(
    {
      servers: `${cluster[0].hostname}:${cluster[0].port}`,
      maxReconnectAttempts: 1,
      reconnectTimeWait: 0,
    },
  );
  const disconnect = Lock();
  const reconnect = Lock();

  (async () => {
    for await (const s of nc.status()) {
      switch (s.type) {
        case Events.DISCONNECT:
          disconnect.unlock();
          break;
        case Events.RECONNECT:
          reconnect.unlock();
          break;
      }
    }
  })().then();

  await cluster[0].stop();
  await Promise.all([disconnect, reconnect]);
  await nc.close();
  await NatsServer.stopAll(cluster);
});

Deno.test("events - update", async () => {
  const cluster = await NatsServer.cluster(1);
  const nc = await connect(
    {
      servers: `127.0.0.1:${cluster[0].port}`,
    },
  );
  const lock = Lock(1, 5000);
  (async () => {
    for await (const s of nc.status()) {
      switch (s.type) {
        case Events.UPDATE:
          const u = s.data as ServersChanged;
          assertEquals(u.added.length, 1);
          lock.unlock();
          break;
      }
    }
  })().then();
  await delay(250);
  const s = await NatsServer.addClusterMember(cluster[0]);
  cluster.push(s);
  await lock;
  await nc.close();
  await NatsServer.stopAll(cluster);
});

Deno.test("events - ldm", async () => {
  const cluster = await NatsServer.cluster(2);
  const nc = await connect(
    {
      servers: `127.0.0.1:${cluster[0].port}`,
    },
  );
  const lock = Lock(1, 5000);
  (async () => {
    for await (const s of nc.status()) {
      switch (s.type) {
        case Events.LDM:
          lock.unlock();
          break;
      }
    }
  })().then();

  await cluster[0].signal(ServerSignals.LDM);
  await lock;
  await nc.close();
  await NatsServer.stopAll(cluster);
});

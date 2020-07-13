import { NatsServer, Lock } from "../tests/helpers/mod.ts";
import { connect, Events } from "../src/mod.ts";
import {
  assertEquals,
} from "https://deno.land/std/testing/asserts.ts";
import { delay } from "../nats-base-client/mod.ts";

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
  nc.addEventListener(Events.DISCONNECT, () => {
    lock.unlock();
  });
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
      url: `${cluster[0].hostname}:${cluster[0].port}`,
      maxReconnectAttempts: 1,
      reconnectTimeWait: 0,
    },
  );
  const disconnect = Lock();
  const reconnect = Lock();
  nc.addEventListener(Events.RECONNECT, () => {
    reconnect.unlock();
  });
  nc.addEventListener(Events.DISCONNECT, () => {
    disconnect.unlock();
  });

  await cluster[0].stop();
  await Promise.all([disconnect, reconnect]);
  await nc.close();
  await NatsServer.stopAll(cluster);
});

Deno.test("events - update", async () => {
  const cluster = await NatsServer.cluster(1);
  const nc = await connect(
    {
      url: `nats://127.0.0.1:${cluster[0].port}`,
    },
  );
  const lock = Lock(1);
  nc.addEventListener(
    Events.UPDATE,
    ((evt: CustomEvent) => {
      assertEquals(evt.detail.added.length, 1);
      lock.unlock();
    }) as EventListener,
  );

  const s = await NatsServer.addClusterMember(cluster[0]);
  cluster.push(s);
  await nc.close();
  await lock;
  await NatsServer.stopAll(cluster);
});

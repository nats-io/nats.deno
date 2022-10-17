import { cleanup, setup } from "./jstest_util.ts";
import {
  addService,
  EndpointStatus,
  ServiceImpl,
  ServiceInfo,
  ServiceStatus,
  SrvVerb,
} from "../nats-base-client/service.ts";
import {
  assertEquals,
  assertExists,
  assertRejects,
} from "https://deno.land/std@0.125.0/testing/asserts.ts";
import {
  Empty,
  JSONCodec,
  Msg,
  QueuedIterator,
} from "../nats-base-client/mod.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";

Deno.test("service - basics", async () => {
  const { ns, nc } = await setup({}, {});
  const jc = JSONCodec();

  const srvA = await addService(nc, {
    name: "test",
    endpoints: {
      subject: "foo",
      handler: (_err: Error | null, msg: Msg) => {
        msg?.respond();
      },
    },
  });

  const srvB = await addService(nc, {
    name: "test",
    endpoints: {
      subject: "foo",
      handler: (_err: Error | null, msg: Msg) => {
        msg?.respond();
      },
    },
  });

  const nci = nc as NatsConnectionImpl;

  async function collect(
    p: Promise<QueuedIterator<Msg | Error>>,
  ): Promise<Msg[]> {
    const msgs: Msg[] = [];
    const iter = await p;
    for await (const e of iter) {
      if (e instanceof Error) {
        return Promise.reject(e);
      }
      msgs.push(e);
    }
    return Promise.resolve(msgs);
  }

  let msgs = await collect(
    nci.requestMany(ServiceImpl.controlSubject(SrvVerb.PING), Empty, {
      maxMessages: 2,
    }),
  );
  assertEquals(msgs.length, 2);

  msgs = await collect(
    nci.requestMany(ServiceImpl.controlSubject(SrvVerb.PING, "test"), Empty, {
      maxMessages: 2,
    }),
  );
  assertEquals(msgs.length, 2);

  const pingr = await nc.request(
    ServiceImpl.controlSubject(SrvVerb.PING, "test", srvA.id),
  );
  const info = jc.decode(pingr.data) as ServiceInfo;
  assertEquals(info.name, "test");
  assertEquals(info.id, srvA.id);

  await assertRejects(async () => {
    await nc.request(ServiceImpl.controlSubject(SrvVerb.PING, "test", "c"));
  });
  await nc.request("foo");

  let r = await nc.request(
    ServiceImpl.controlSubject(SrvVerb.STATUS, "test", srvA.id),
  );
  let status = jc.decode(r.data) as ServiceStatus;

  function findStatus(n: string): EndpointStatus | null {
    const found = status.endpoints.find((se) => {
      return se.name === n;
    });
    return found as EndpointStatus || null;
  }

  const paEntry = findStatus("ping-all");
  assertExists(paEntry);
  assertEquals(paEntry?.requests, 1);
  assertEquals(paEntry?.errors, 0);

  const pkEntry = findStatus("ping-kind");
  assertExists(pkEntry);
  assertEquals(pkEntry?.requests, 1);
  assertEquals(pkEntry?.errors, 0);

  const pEntry = findStatus("ping");
  assertExists(pEntry);
  assertEquals(pEntry?.requests, 1);
  assertEquals(pEntry?.errors, 0);

  const saEntry = findStatus("status-all");
  assertExists(saEntry);
  assertEquals(saEntry?.requests, 0);
  assertEquals(saEntry?.errors, 0);

  const skEntry = findStatus("status-kind");
  assertExists(skEntry);
  assertEquals(skEntry?.requests, 0);
  assertEquals(skEntry?.errors, 0);

  const sEntry = findStatus("status");
  assertExists(sEntry);
  assertEquals(pEntry?.requests, 1);
  assertEquals(pEntry?.errors, 0);

  msgs = await collect(nci.requestMany(
    ServiceImpl.controlSubject(SrvVerb.STATUS),
    Empty,
    { maxMessages: 2 },
  ));
  assertEquals(msgs.length, 2);
  const statuses = msgs.map((m) => {
    return jc.decode(m.data) as ServiceStatus;
  });

  const statusA = statuses.find((a) => {
    return a.id === srvA.id && a.name === "test";
  });
  assertExists(statusA);
  const statusB = statuses.find((a) => {
    return a.id === srvB.id && a.name === "test";
  });
  assertExists(statusB);

  r = await nc.request(
    ServiceImpl.controlSubject(SrvVerb.STATUS, "test", srvA.id),
    jc.encode({ internal: false }),
  );
  status = jc.decode(r.data) as ServiceStatus;
  assertEquals(status.name, "test");
  assertEquals(status.id, srvA.id);
  assertEquals(status.endpoints.length, 1);
  assertEquals(status.endpoints[0].name, "test");

  await srvA.stop();
  await srvB.stop();

  await cleanup(ns, nc);
});

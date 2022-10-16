import { cleanup, setup } from "./jstest_util.ts";
import {
  addService,
  EndpointStatus,
  ServiceImpl,
  ServiceStatus,
  SrvVerb,
} from "../nats-base-client/service.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";
import {
  deferred,
  Empty,
  JSONCodec,
  Msg,
  QueuedIterator,
} from "../nats-base-client/mod.ts";
import {
  assertEquals,
  assertExists,
} from "https://deno.land/std@0.75.0/testing/asserts.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";

Deno.test("service - basics", async () => {
  const { ns, nc } = await setup({}, {});
  const jc = JSONCodec();

  const hb = deferred<Msg>();
  nc.subscribe(
    ServiceImpl.controlSubject(SrvVerb.HEARTBEAT, "test", "a"),
    {
      callback: (_err, msg) => {
        hb.resolve(msg);
      },
      max: 1,
    },
  );

  const srvA = await addService(nc, {
    kind: "test",
    id: "a",
    endpoints: {
      name: "x",
      subject: "foo",
      queueGroup: "x",
      handler: (_err: Error | null, msg: Msg) => {
        msg?.respond();
      },
    },
  });
  srvA.heartbeatInterval = 3000;

  const srvB = await addService(nc, {
    kind: "test",
    id: "b",
    endpoints: {
      name: "x",
      subject: "foo",
      queueGroup: "x",
      handler: (_err: Error | null, msg: Msg) => {
        msg?.respond();
      },
    },
  });
  srvB.heartbeatInterval = 3000;

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

  await nc.request(ServiceImpl.controlSubject(SrvVerb.PING, "test", "a"));
  await assertRejects(async () => {
    await nc.request(ServiceImpl.controlSubject(SrvVerb.PING, "test", "c"));
  });
  await nc.request("foo");

  let r = await nc.request(
    ServiceImpl.controlSubject(SrvVerb.STATUS, "test", "a"),
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

  const m = await hb;
  status = jc.decode(m.data) as ServiceStatus;

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
    return a.id === "A" && a.kind === "TEST";
  });
  assertExists(statusA);
  const statusB = statuses.find((a) => {
    return a.id === "B" && a.kind === "TEST";
  });
  assertExists(statusB);

  r = await nc.request(
    ServiceImpl.controlSubject(SrvVerb.STATUS, "test", "a"),
    jc.encode({ internal: false }),
  );
  status = jc.decode(r.data) as ServiceStatus;
  assertEquals(status.kind, "TEST");
  assertEquals(status.id, "A");
  assertEquals(status.endpoints.length, 1);
  assertEquals(status.endpoints[0].name, "x");

  await cleanup(ns, nc);
});

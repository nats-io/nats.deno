import { cleanup, setup } from "./jstest_util.ts";
import {
  addService,
  EndpointStats,
  ServiceErrorHeader,
  ServiceImpl,
  ServiceInfo,
  ServiceStats,
  ServiceVerb,
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
  const nci = nc as NatsConnectionImpl;
  const jc = JSONCodec();

  const srvA = await addService(nc, {
    name: "test",
    endpoint: {
      subject: "foo",
      handler: (_err: Error | null, msg: Msg): Promise<void> => {
        msg?.respond();
        return Promise.resolve();
      },
    },
  });

  const srvB = await addService(nc, {
    name: "test",
    endpoint: {
      subject: "foo",
      handler: (_err: Error | null, msg: Msg): Promise<void> => {
        msg?.respond();
        return Promise.resolve();
      },
    },
  });

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
    nci.requestMany(ServiceImpl.controlSubject(ServiceVerb.PING), Empty, {
      maxMessages: 2,
    }),
  );
  assertEquals(msgs.length, 2);

  msgs = await collect(
    nci.requestMany(
      ServiceImpl.controlSubject(ServiceVerb.PING, "test"),
      Empty,
      {
        maxMessages: 2,
      },
    ),
  );
  assertEquals(msgs.length, 2);

  const pingr = await nc.request(
    ServiceImpl.controlSubject(ServiceVerb.PING, "test", srvA.id),
  );
  const info = jc.decode(pingr.data) as ServiceInfo;
  assertEquals(info.name, "test");
  assertEquals(info.id, srvA.id);

  await assertRejects(async () => {
    await nc.request(ServiceImpl.controlSubject(ServiceVerb.PING, "test", "c"));
  });
  await nc.request("foo");

  let r = await nc.request(
    ServiceImpl.controlSubject(ServiceVerb.STATUS, "test", srvA.id),
  );
  let status = jc.decode(r.data) as ServiceStats;

  function findStatus(n: string): EndpointStats | null {
    const found = status.stats.find((se) => {
      return se.name === n;
    });
    return found as EndpointStats || null;
  }

  const paEntry = findStatus("PING-all");
  assertExists(paEntry);
  assertEquals(paEntry?.num_requests, 1);
  assertEquals(paEntry?.num_errors, 0);

  const pkEntry = findStatus("PING-kind");
  assertExists(pkEntry);
  assertEquals(pkEntry?.num_requests, 1);
  assertEquals(pkEntry?.num_errors, 0);

  const pEntry = findStatus("PING");
  assertExists(pEntry);
  assertEquals(pEntry?.num_requests, 1);
  assertEquals(pEntry?.num_errors, 0);

  const saEntry = findStatus("STATUS-all");
  assertExists(saEntry);
  assertEquals(saEntry?.num_requests, 0);
  assertEquals(saEntry?.num_errors, 0);

  const skEntry = findStatus("STATUS-kind");
  assertExists(skEntry);
  assertEquals(skEntry?.num_requests, 0);
  assertEquals(skEntry?.num_errors, 0);

  const sEntry = findStatus("STATUS");
  assertExists(sEntry);
  assertEquals(pEntry?.num_requests, 1);
  assertEquals(pEntry?.num_errors, 0);

  msgs = await collect(nci.requestMany(
    ServiceImpl.controlSubject(ServiceVerb.STATUS),
    Empty,
    { maxMessages: 2 },
  ));
  assertEquals(msgs.length, 2);
  const statuses = msgs.map((m) => {
    return jc.decode(m.data) as ServiceStats;
  });

  const statusA = statuses.find((a) => {
    return a.id === srvA.id && a.name === "test";
  });
  assertExists(statusA);
  const statusB = statuses.find((a) => {
    return a.id === srvB.id && a.name === "test";
  });
  assertExists(statusB);

  msgs = await collect(
    nci.requestMany(ServiceImpl.controlSubject(ServiceVerb.INFO), Empty, {
      maxMessages: 2,
    }),
  );
  const infos = msgs.map((m) => {
    return jc.decode(m.data) as ServiceInfo;
  });
  assertEquals(infos.length, 2);
  assertEquals(infos[0].subject, "foo");
  assertEquals(infos[1].subject, "foo");

  r = await nc.request(
    ServiceImpl.controlSubject(ServiceVerb.STATUS, "test", srvA.id),
    jc.encode({ internal: false }),
  );
  status = jc.decode(r.data) as ServiceStats;
  assertEquals(status.name, "test");
  assertEquals(status.id, srvA.id);
  assertEquals(status.stats.length, 1);
  assertEquals(status.stats[0].name, "test");

  await srvA.stop();
  await srvB.stop();

  await cleanup(ns, nc);
});

Deno.test("service - handler error", async () => {
  const { ns, nc } = await setup();

  await addService(nc, {
    name: "test",
    endpoint: {
      subject: "fail",
      handler: () => {
        throw new Error("cb error");
      },
    },
  });

  const r = await nc.request("fail");
  assertEquals(r.headers?.get(ServiceErrorHeader), "400 cb error");

  await cleanup(ns, nc);
});

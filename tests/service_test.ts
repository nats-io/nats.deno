import { cleanup, setup } from "./jstest_util.ts";
import {
  addService,
  ServiceImpl,
  ServiceStatus,
  SrvVerb,
} from "../nats-base-client/service.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";
import { deferred, JSONCodec, Msg } from "../nats-base-client/mod.ts";
import { assertEquals } from "https://deno.land/std@0.75.0/testing/asserts.ts";

Deno.test("service - basics", async () => {
  const { ns, nc } = await setup({}, { debug: true });
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

  const srv = await addService(nc, {
    kind: "test",
    id: "a",
    endpoints: {
      name: "x",
      subject: "foo",
      handler: (_err: Error | null, msg: Msg) => {
        msg?.respond();
      },
    },
  });
  srv.heartbeatInterval = 3000;

  await nc.request(ServiceImpl.controlSubject(SrvVerb.PING, "test"));
  await nc.request(ServiceImpl.controlSubject(SrvVerb.PING, "test", "a"));
  await assertRejects(async () => {
    await nc.request(ServiceImpl.controlSubject(SrvVerb.PING, "test", "b"));
  });
  await nc.request("foo");

  const r = await nc.request(
    ServiceImpl.controlSubject(SrvVerb.STATUS, "test", "a"),
  );
  let status = jc.decode(r.data) as ServiceStatus[];
  assertEquals(status.length, 7);

  const m = await hb;
  status = jc.decode(m.data) as ServiceStatus[];
  assertEquals(status.length, 7);

  await cleanup(ns, nc);
});

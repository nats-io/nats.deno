import { cleanup, setup } from "./jstest_util.ts";
import {
  addService,
  MsgSrvHandler,
  SrvImpl,
  SrvStatus,
  SrvVerb,
} from "../nats-base-client/service.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";
import { deferred, JSONCodec, Msg } from "../nats-base-client/mod.ts";
import { assertEquals } from "https://deno.land/std@0.75.0/testing/asserts.ts";

Deno.test("svc - basics", async () => {
  const { ns, nc } = await setup();
  const jc = JSONCodec();

  const hb = deferred<Msg>();
  nc.subscribe(
    SrvImpl.controlSubject(SrvVerb.HEARTBEAT, "test", "a"),
    {
      callback: (_err, msg) => {
        hb.resolve(msg);
      },
      max: 1,
    },
  );

  const srv = await addService(nc, "test", "a", {
    name: "x",
    subject: "foo",
    handler: (_err, msg) => {
      msg?.respond();
    },
  } as MsgSrvHandler);
  srv.heartbeatInterval = 3000;

  await nc.request(SrvImpl.controlSubject(SrvVerb.PING, "test"));
  await nc.request(SrvImpl.controlSubject(SrvVerb.PING, "test", "a"));
  await assertRejects(async () => {
    await nc.request(SrvImpl.controlSubject(SrvVerb.PING, "test", "b"));
  });
  await nc.request("foo");

  const r = await nc.request(
    SrvImpl.controlSubject(SrvVerb.STATUS, "test", "a"),
  );
  let status = jc.decode(r.data) as SrvStatus[];
  assertEquals(status.length, 7);

  const m = await hb;
  status = jc.decode(m.data) as SrvStatus[];
  assertEquals(status.length, 7);

  await cleanup(ns, nc);
});

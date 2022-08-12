import { cleanup, setup } from "./jstest_util.ts";
import { createInbox } from "../nats-base-client/protocol.ts";
import { Msg } from "../nats-base-client/types.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";
import { assertEquals } from "https://deno.land/std@0.152.0/testing/asserts.ts";

Deno.test("resub - iter", async () => {
  const { ns, nc } = await setup();
  const nci = nc as NatsConnectionImpl;
  const subja = createInbox();

  const sub = nc.subscribe(subja, { max: 2 });
  const buf: Msg[] = [];
  (async () => {
    for await (const m of sub) {
      buf.push(m);
      m.respond();
    }
  })().then();

  await nc.request(subja);

  const subjb = createInbox();
  nci._resub(sub, subjb);
  assertEquals(sub.getSubject(), subjb);

  await nc.request(subjb);

  assertEquals(sub.getProcessed(), 2);
  assertEquals(buf.length, 2);
  assertEquals(buf[0].subject, subja);
  assertEquals(buf[1].subject, subjb);
  await cleanup(ns, nc);
});

Deno.test("resub - callback", async () => {
  const { ns, nc } = await setup();
  const nci = nc as NatsConnectionImpl;
  const subja = createInbox();
  const buf: Msg[] = [];

  const sub = nc.subscribe(subja, {
    max: 2,
    callback: (_err, msg) => {
      buf.push(msg);
      msg.respond();
    },
  });

  await nc.request(subja);

  const subjb = createInbox();
  nci._resub(sub, subjb);
  assertEquals(sub.getSubject(), subjb);

  await nc.request(subjb);

  assertEquals(sub.getProcessed(), 2);
  assertEquals(buf.length, 2);
  assertEquals(buf[0].subject, subja);
  assertEquals(buf[1].subject, subjb);
  await cleanup(ns, nc);
});

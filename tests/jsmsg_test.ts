/*
 * Copyright 2021 The NATS Authors
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
import {
  assertEquals,
  fail,
} from "https://deno.land/std@0.95.0/testing/asserts.ts";
import {
  connect,
  createInbox,
  Empty,
  headers,
  Msg,
  StringCodec,
} from "../src/mod.ts";
import { deferred } from "../nats-base-client/util.ts";
import { nanos } from "../nats-base-client/jsutil.ts";
import { parseInfo, toJsMsg } from "../nats-base-client/jsmsg.ts";

Deno.test("jsmsg - parse", async () => {
  const nc = await connect({ servers: "demo.nats.io" });
  const subj = createInbox();
  const m = deferred<Msg>();
  const sub = nc.subscribe(subj, {
    max: 1,
    callback: (_err, msg) => {
      m.resolve(msg);
    },
  });

  // "$JS.ACK.<stream>.<consumer>.<redeliveryCount><streamSeq><deliverySequence>.<timestamp>.<pending>"
  const rs = `MY.TEST.streamname.consumername.2.3.4.${nanos(Date.now())}.100`;
  const h = headers();
  h.set("hello", "world");
  nc.publish(subj, Empty, { reply: rs, headers: h });
  const msg = await m;
  const jm = toJsMsg(msg);

  assertEquals(jm.info.stream, "streamname");
  assertEquals(jm.info.consumer, "consumername");
  assertEquals(jm.info.redeliveryCount, 2);
  assertEquals(jm.redelivered, true);
  assertEquals(jm.seq, 3);
  assertEquals(jm.info.pending, 100);
  assertEquals(jm.sid, sub.getID());

  const h2 = jm.headers;
  assertEquals(h2!.get("hello"), "world");

  await nc.close();
});

Deno.test("jsmsg - parse rejects subject is not 9 tokens", () => {
  const fn = (s: string, ok: boolean) => {
    try {
      parseInfo(s);
      if (!ok) {
        fail(`${s} should have failed to parse`);
      }
    } catch (err) {
      if (ok) {
        fail(`${s} shouldn't have failed to parse: ${err.message}`);
      }
    }
  };

  const chunks = `$JS.ACK.stream.consumer.1.2.3.4.5`.split(".");
  for (let i = 1; i <= chunks.length; i++) {
    fn(chunks.slice(0, i).join("."), i === 9);
  }
});

Deno.test("jsmsg - acks", async () => {
  const nc = await connect({ servers: "demo.nats.io" });
  const subj = createInbox();

  // something that puts a reply that we can test
  let counter = 1;
  nc.subscribe(subj, {
    callback: (err, msg) => {
      if (err) {
        fail(err.message);
      }
      msg.respond(Empty, {
        // "$JS.ACK.<stream>.<consumer>.<redeliveryCount><streamSeq><deliverySequence>.<timestamp>.<pending>"
        reply:
          `MY.TEST.streamname.consumername.1.${counter}.${counter}.${Date.now()}.0`,
      });
      counter++;
    },
  });

  // something to collect the replies
  const replies: Msg[] = [];
  nc.subscribe("MY.TEST.*.*.*.*.*.*.*", {
    callback: (err, msg) => {
      if (err) {
        fail(err.message);
      }
      replies.push(msg);
    },
  });

  // nak
  let msg = await nc.request(subj);
  let js = toJsMsg(msg);
  js.nak();

  // working
  msg = await nc.request(subj);
  js = toJsMsg(msg);
  js.working();

  // working
  msg = await nc.request(subj);
  js = toJsMsg(msg);
  js.term();

  msg = await nc.request(subj);
  js = toJsMsg(msg);
  js.ack();
  await nc.flush();

  assertEquals(replies.length, 4);
  const sc = StringCodec();
  assertEquals(sc.decode(replies[0].data), "-NAK");
  assertEquals(sc.decode(replies[1].data), "+WPI");
  assertEquals(sc.decode(replies[2].data), "+TERM");
  assertEquals(sc.decode(replies[3].data), "+ACK");

  await nc.close();
});

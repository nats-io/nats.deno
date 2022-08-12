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
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { connect, createInbox, Empty, Msg, StringCodec } from "../src/mod.ts";
import { nanos } from "../nats-base-client/jsutil.ts";
import { parseInfo, toJsMsg } from "../nats-base-client/jsmsg.ts";

Deno.test("jsmsg - parse", () => {
  // "$JS.ACK.<stream>.<consumer>.<redeliveryCount><streamSeq><deliverySequence>.<timestamp>.<pending>"
  const rs = `$JS.ACK.streamname.consumername.2.3.4.${nanos(Date.now())}.100`;
  const info = parseInfo(rs);
  assertEquals(info.stream, "streamname");
  assertEquals(info.consumer, "consumername");
  assertEquals(info.redeliveryCount, 2);
  assertEquals(info.streamSequence, 3);
  assertEquals(info.pending, 100);
});

Deno.test("jsmsg - parse long", () => {
  // $JS.ACK.<domain>.<accounthash>.<stream>.<consumer>.<redeliveryCount>.<streamSeq>.<deliverySequence>.<timestamp>.<pending>.<random>
  const rs = `$JS.ACK.domain.account.streamname.consumername.2.3.4.${
    nanos(Date.now())
  }.100.rand`;
  const info = parseInfo(rs);
  assertEquals(info.domain, "domain");
  assertEquals(info.account_hash, "account");
  assertEquals(info.stream, "streamname");
  assertEquals(info.consumer, "consumername");
  assertEquals(info.redeliveryCount, 2);
  assertEquals(info.streamSequence, 3);
  assertEquals(info.pending, 100);
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

  const chunks = `$JS.ACK.stream.consumer.1.2.3.4.5.6.7.8.9.10`.split(".");
  for (let i = 1; i <= chunks.length; i++) {
    fn(chunks.slice(0, i).join("."), i === 9 || i >= 12);
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

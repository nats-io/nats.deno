/*
 * Copyright 2020 The NATS Authors
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

import { Err, Info, Msg, Parser, ParserEvents, State } from "./parser.ts";
import {
  assertEquals,
  assertThrows,
  assert,
} from "https://deno.land/std@0.63.0/testing/asserts.ts";
import { Dispatcher } from "./queued_iterator.ts";

let te = new TextEncoder();
const td = new TextDecoder();

class NoopDispatcher implements Dispatcher<ParserEvents> {
  push(a: ParserEvents): void {}
}

class TestDispatcher implements Dispatcher<ParserEvents> {
  count = 0;
  pings = 0;
  pongs = 0;
  ok = 0;
  errs: Err[] = [];
  infos: Info[] = [];
  msgs: Msg[] = [];

  push(a: ParserEvents): void {
    this.count++;
    if (typeof a === "object" && "msg" in a) {
      this.msgs.push(a);
    } else if (typeof a === "object" && "info" in a) {
      this.infos.push(a);
    } else if (typeof a === "object" && "message" in a) {
      this.errs.push(a);
    } else if (a === "ping") {
      this.pings++;
    } else if (a === "pong") {
      this.pongs++;
    } else if (a === "ok") {
      this.ok++;
    } else {
      throw new Error(`unknown parser evert ${a}`);
    }
  }
}

// These are almost verbatim ports of the NATS parser tests

function byByteTest(
  data: Uint8Array,
): { states: State[]; dispatcher: TestDispatcher } {
  const e = new TestDispatcher();
  const p = new Parser(e);
  const states: State[] = [];
  assertEquals(p.state, State.OP_START);

  for (let i = 0; i < data.length; i++) {
    states.push(p.state);
    p.parse(Uint8Array.of(data[i]));
  }
  states.push(p.state);
  return { states, dispatcher: e };
}

Deno.test("parser - ping", () => {
  const states = [
    State.OP_START,
    State.OP_P,
    State.OP_PI,
    State.OP_PIN,
    State.OP_PING,
    State.OP_PING,
    State.OP_START,
  ];
  const results = byByteTest(te.encode("PING\r\n"));
  assertEquals(results.states, states);
  assertEquals(results.dispatcher.pings, 1);
  assertEquals(results.dispatcher.count, 1);

  const events = new TestDispatcher();
  const p = new Parser(events);
  p.parse(te.encode("PING\r\n"));
  assertEquals(p.state, State.OP_START);

  p.parse(te.encode("PING \r"));
  assertEquals(p.state, State.OP_PING);

  p.parse(te.encode("PING \r \n"));
  assertEquals(p.state, State.OP_START);

  assertEquals(events.pings, 2);
  assertEquals(events.count, 2);
});

Deno.test("parser - err", () => {
  const states = [
    State.OP_START,
    State.OP_MINUS,
    State.OP_MINUS_E,
    State.OP_MINUS_ER,
    State.OP_MINUS_ERR,
    State.OP_MINUS_ERR_SPC,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.MINUS_ERR_ARG,
    State.OP_START,
  ];
  const results = byByteTest(te.encode(`-ERR '234 6789'\r\n`));
  assertEquals(results.states, states);
  assertEquals(results.dispatcher.errs.length, 1);
  assertEquals(results.dispatcher.count, 1);
  assertEquals(results.dispatcher.errs[0].message, `'234 6789'`);

  const events = new TestDispatcher();
  const p = new Parser(events);
  p.parse(te.encode("-ERR 'Any error'\r\n"));
  assertEquals(p.state, State.OP_START);
  assertEquals(events.errs.length, 1);
  assertEquals(events.count, 1);
  assertEquals(events.errs[0].message, `'Any error'`);
});

Deno.test("parser - ok", () => {
  let states = [
    State.OP_START,
    State.OP_PLUS,
    State.OP_PLUS_O,
    State.OP_PLUS_OK,
    State.OP_PLUS_OK,
    State.OP_START,
  ];
  let result = byByteTest(te.encode("+OK\r\n"));
  assertEquals(result.states, states);

  states = [
    State.OP_START,
    State.OP_PLUS,
    State.OP_PLUS_O,
    State.OP_PLUS_OK,
    State.OP_PLUS_OK,
    State.OP_PLUS_OK,
    State.OP_PLUS_OK,
    State.OP_START,
  ];
  result = byByteTest(te.encode("+OKay\r\n"));

  assertEquals(result.states, states);
});

Deno.test("parser - info", () => {
  let states = [
    State.OP_START,
    State.OP_I,
    State.OP_IN,
    State.OP_INF,
    State.OP_INFO,
    State.OP_INFO_SPC,
    State.INFO_ARG,
    State.INFO_ARG,
    State.INFO_ARG,
    State.OP_START,
  ];

  const results = byByteTest(te.encode(`INFO {}\r\n`));
  assertEquals(results.states, states);
  assertEquals(results.dispatcher.infos.length, 1);
  assertEquals(results.dispatcher.count, 1);
});

Deno.test("parser - errors", () => {
  const bad = [
    " PING",
    "POO",
    "Px",
    "PIx",
    "PINx",
    "PONx",
    "ZOO",
    "Mx\r\n",
    "MSx\r\n",
    "MSGx\r\n",
    "MSG foo\r\n",
    "MSG \r\n",
    "MSG foo 1\r\n",
    "MSG foo bar 1\r\n",
    "MSG foo bar 1 baz\r\n",
    "MSG foo 1 bar baz\r\n",
    "+x\r\n",
    "+0x\r\n",
    "-x\r\n",
    "-Ex\r\n",
    "-ERx\r\n",
    "-ERRx\r\n",
  ];

  let count = 0;
  bad.forEach((s) => {
    assertThrows(() => {
      count++;
      const p = new Parser(new NoopDispatcher());
      p.parse(te.encode(s));
    });
  });

  assertEquals(count, bad.length);
});

Deno.test("parser - split msg", () => {
  assertThrows(() => {
    const p = new Parser(new NoopDispatcher());
    p.parse(te.encode("MSG a\r\n"));
  });

  assertThrows(() => {
    const p = new Parser(new NoopDispatcher());
    p.parse(te.encode("MSG a b c\r\n"));
  });

  let d = new TestDispatcher();
  let p = new Parser(d);
  p.parse(te.encode("MSG a"));
  assert(p.argBuf);
  p.parse(te.encode(" 1 3\r\nf"));
  assertEquals(p.ma.size, 3, "size");
  assertEquals(p.ma.sid, 1, "sid");
  assertEquals(p.ma.subject, te.encode("a"), "subject");
  assert(p.msgBuf, "should message buffer");
  p.parse(te.encode("oo\r\n"));
  assertEquals(d.count, 1);
  assertEquals(d.msgs.length, 1);
  assertEquals(td.decode(d.msgs[0].msg.subject), "a");
  assertEquals(td.decode(d.msgs[0].data), "foo");
  assertEquals(p.msgBuf, undefined);

  p.parse(te.encode("MSG a 1 3\r\nba"));
  assertEquals(p.ma.size, 3, "size");
  assertEquals(p.ma.sid, 1, "sid");
  assertEquals(p.ma.subject, te.encode("a"), "subject");
  assert(p.msgBuf, "should message buffer");
  p.parse(te.encode("r\r\n"));
  assertEquals(d.msgs.length, 2);
  assertEquals(td.decode(d.msgs[1].data), "bar");
  assertEquals(p.msgBuf, undefined);

  p.parse(te.encode("MSG a 1 6\r\nfo"));
  assertEquals(p.ma.size, 6, "size");
  assertEquals(p.ma.sid, 1, "sid");
  assertEquals(p.ma.subject, te.encode("a"), "subject");
  assert(p.msgBuf, "should message buffer");
  p.parse(te.encode("ob"));
  p.parse(te.encode("ar\r\n"));

  assertEquals(d.msgs.length, 3);
  assertEquals(td.decode(d.msgs[2].data), "foobar");
  assertEquals(p.msgBuf, undefined);

  const payload = new Uint8Array(100);
  const buf = te.encode("MSG a 1 b 103\r\nfoo");
  p.parse(buf);
  assertEquals(p.ma.size, 103);
  assertEquals(p.ma.sid, 1);
  assertEquals(td.decode(p.ma.subject), "a");
  assertEquals(td.decode(p.ma.reply), "b");
  assert(p.argBuf);
  const a = "a".charCodeAt(0); //97
  for (let i = 0; i < payload.length; i++) {
    payload[i] = a + (i % 26);
  }
  p.parse(payload);
  assertEquals(p.state, State.MSG_PAYLOAD);
  assertEquals(p.ma.size, 103);
  assertEquals(p.msgBuf.length, 103);
  p.parse(te.encode("\r\n"));
  assertEquals(p.state, State.OP_START);
  assertEquals(p.msgBuf, undefined);
  assertEquals(d.msgs.length, 4);
  const db = d.msgs[3].data;
  assertEquals(td.decode(db.subarray(0, 3)), "foo");
  const gen = db.subarray(3);
  for (let k = 0; k < 100; k++) {
    assertEquals(gen[k], a + (k % 26));
  }
});

Deno.test("parser - info arg", () => {
  const arg = {
    server_id: "test",
    host: "localhost",
    port: 4222,
    version: "1.2.3",
    auth_required: true,
    tls_required: true,
    max_payload: 2 * 1024 * 1024,
    connect_urls: [
      "localhost:5222",
      "localhost:6222",
    ],
  };

  const d = new TestDispatcher();
  const p = new Parser(d);
  const info = te.encode(`INFO ${JSON.stringify(arg)}\r\n`);

  p.parse(info.subarray(0, 9));
  assertEquals(p.state, State.INFO_ARG);
  assert(p.argBuf);

  p.parse(info.subarray(9, 11));
  assertEquals(p.state, State.INFO_ARG);
  assert(p.argBuf);

  p.parse(info.subarray(11));
  assertEquals(p.state, State.OP_START);
  assertEquals(p.argBuf, undefined);

  assertEquals(d.infos.length, 1);
  assertEquals(d.count, 1);

  const arg2 = JSON.parse(td.decode(d.infos[0].info));
  assertEquals(arg2, arg);

  const good = [
    "INFO {}\r\n",
    "INFO  {}\r\n",
    "INFO {} \r\n",
    'INFO { "server_id": "test"  }   \r\n',
    'INFO {"connect_urls":[]}\r\n',
  ];
  good.forEach((info) => {
    p.parse(te.encode(info));
  });
  assertEquals(d.infos.length, good.length + 1);

  const bad = [
    "IxNFO {}\r\n",
    "INxFO {}\r\n",
    "INFxO {}\r\n",
    "INFOx {}\r\n",
    "INFO{}\r\n",
    "INFO {}",
  ];

  let count = 0;
  bad.forEach((info) => {
    assertThrows(() => {
      count++;
      p.parse(te.encode(info));
    });
  });
  assertEquals(count, bad.length);
});

#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.152.0/flags/mod.ts";
import {
  connect,
  ConnectionOptions,
  credsAuthenticator,
  StringCodec,
} from "../src/mod.ts";
import { headers, MsgHdrs } from "../nats-base-client/mod.ts";
import { delay } from "../nats-base-client/internal_mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "c": ["count"],
      "i": ["interval"],
      "f": ["creds"],
    },
    default: {
      s: "127.0.0.1:4222",
      c: 1,
      i: 0,
    },
    boolean: ["debug"],
    string: ["server", "count", "interval", "headers", "creds"],
  },
);

const copts = { servers: argv.s } as ConnectionOptions;
const subject = String(argv._[0]);
const payload = argv._[1] || "";
const count = (argv.c == -1 ? Number.MAX_SAFE_INTEGER : argv.c) || 1;
const interval = argv.i || 0;

if (argv.debug) {
  copts.debug = true;
}

if (argv.h || argv.help || !subject) {
  console.log(
    "Usage: nats-pub [-s server] [--creds=/path/file.creds] [-c <count>=1] [-i <interval>=0] [--headers='k=v;k2=v2'] subject [msg]",
  );
  console.log("to publish forever, specify -c=-1 or --count=-1");
  Deno.exit(1);
}

if (argv.creds) {
  const f = await Deno.open(argv.creds, { read: true });
  // FIXME: this needs to be changed when deno releases 2.0
  // deno-lint-ignore no-deprecated-deno-api
  const data = await Deno.readAll(f);
  Deno.close(f.rid);
  copts.authenticator = credsAuthenticator(data);
}

const nc = await connect(copts);
nc.closed()
  .then((err) => {
    if (err) {
      console.error(`closed with an error: ${err.message}`);
    }
  });

const pubopts = {} as { reply?: string; headers?: MsgHdrs };
if (argv.headers) {
  const hdrs = headers();
  argv.headers.split(";").map((l: string) => {
    const [k, v] = l.split("=");
    hdrs.append(k, v);
  });
  pubopts.headers = hdrs;
}

const sc = StringCodec();

for (let i = 1; i <= count; i++) {
  nc.publish(subject, sc.encode(String(payload)), pubopts);
  console.log(`[${i}] ${subject}: ${payload}`);
  if (interval) {
    await delay(interval);
  }
}
await nc.flush();
await nc.close();

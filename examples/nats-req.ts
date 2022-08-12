#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.152.0/flags/mod.ts";
import {
  connect,
  ConnectionOptions,
  credsAuthenticator,
  StringCodec,
} from "../src/mod.ts";
import { delay } from "../nats-base-client/internal_mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "c": ["count"],
      "i": ["interval"],
      "t": ["timeout"],
      "f": ["creds"],
    },
    default: {
      s: "127.0.0.1:4222",
      c: 1,
      i: 0,
      t: 1000,
    },
    boolean: ["debug"],
    string: ["server", "count", "interval", "headers", "creds"],
  },
);

const opts = { servers: argv.s } as ConnectionOptions;
const subject = String(argv._[0]);
const payload = String(argv._[1]) || "";
const count = (argv.c == -1 ? Number.MAX_SAFE_INTEGER : argv.c) || 1;
const interval = argv.i;

if (argv.debug) {
  opts.debug = true;
}

if (argv.h || argv.help || !subject) {
  console.log(
    "Usage: nats-pub [-s server] [--creds=/path/file.creds] [-c <count>=1] [-t <timeout>=1000] [-i <interval>=0] [--headers] subject [msg]",
  );
  console.log("to request forever, specify -c=-1 or --count=-1");
  Deno.exit(1);
}

if (argv.creds) {
  const f = await Deno.open(argv.creds, { read: true });
  // FIXME: this needs to be changed when deno releases 2.0
  // deno-lint-ignore no-deprecated-deno-api
  const data = await Deno.readAll(f);
  Deno.close(f.rid);
  opts.authenticator = credsAuthenticator(data);
}

const sc = StringCodec();
const nc = await connect(opts);
nc.closed()
  .then((err) => {
    if (err) {
      console.error(`closed with an error: ${err.message}`);
    }
  });

for (let i = 1; i <= count; i++) {
  await nc.request(subject, sc.encode(payload), { timeout: argv.t })
    .then((m) => {
      console.log(`[${i}]: ${sc.decode(m.data)}`);
      if (argv.headers && m.headers) {
        const h = [];
        for (const [key, value] of m.headers) {
          h.push(`${key}=${value}`);
        }
        console.log(`\t${h.join(";")}`);
      }
    })
    .catch((err) => {
      console.log(`[${i}]: request failed: ${err.message}`);
    });
  if (interval) {
    await delay(interval);
  }
}
await nc.flush();
await nc.close();

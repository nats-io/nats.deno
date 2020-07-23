#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.61.0/flags/mod.ts";
import { ConnectionOptions, connect } from "../src/mod.ts";
import { delay } from "../nats-base-client/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "c": ["count"],
      "i": ["interval"],
    },
    default: {
      s: "nats://127.0.0.1:4222",
      c: 1,
      i: 0,
    },
    boolean: true,
    string: ["server", "count", "interval", "headers"],
  },
);

const copts = { url: argv.s } as ConnectionOptions;
const subject = String(argv._[0]);
const payload = argv._[1] || "";
const count = (argv.c == -1 ? Number.MAX_SAFE_INTEGER : argv.c) || 1;
const interval = argv.i || 0;

if (argv.headers) {
  copts.headers = true;
}

if (argv.debug) {
  copts.debug = true;
}

if (argv.h || argv.help || !subject) {
  console.log(
    "Usage: nats-pub [-s server] [-c <count>=1] [-i <interval>=0] [--headers='k=v;k2=v2'] subject [msg]",
  );
  console.log("to publish forever, specify -c=-1 or --count=-1");
  Deno.exit(1);
}

const nc = await connect(copts);
nc.closed()
  .then((err) => {
    if (err) {
      console.error(`closed with an error: ${err.message}`);
    }
  });

const pubopts = {} as { reply?: string; headers?: Headers };
if (argv.headers) {
  const headers = new Headers();
  argv.headers.split(";").map((l: string) => {
    const [k, v] = l.split("=");
    headers.append(k, v);
  });
  pubopts.headers = headers;
}

for (let i = 1; i <= count; i++) {
  nc.publish(subject, payload, pubopts);
  console.log(`[${i}] ${subject}: ${payload}`);
  if (interval) {
    await delay(interval);
  }
}
await nc.flush();
await nc.close();

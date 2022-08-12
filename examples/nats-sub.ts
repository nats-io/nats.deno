#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.152.0/flags/mod.ts";
import {
  connect,
  ConnectionOptions,
  credsAuthenticator,
  StringCodec,
} from "../src/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "q": ["queue"],
      "f": ["creds"],
    },
    default: {
      s: "127.0.0.1:4222",
      q: "",
    },
    boolean: ["headers", "debug"],
    string: ["server", "queue", "creds"],
  },
);

const opts = { servers: argv.s } as ConnectionOptions;
const subject = argv._[0] ? String(argv._[0]) : ">";

if (argv.debug) {
  opts.debug = true;
}

if (argv.creds) {
  const f = await Deno.open(argv.creds, { read: true });
  // FIXME: this needs to be changed when deno releases 2.0
  // deno-lint-ignore no-deprecated-deno-api
  const data = await Deno.readAll(f);
  Deno.close(f.rid);
  opts.authenticator = credsAuthenticator(data);
}

if (argv.h || argv.help || !subject) {
  console.log(
    "Usage: nats-sub [-s server]  [--creds=/path/file.creds] [-q queue] [--headers] subject",
  );
  Deno.exit(1);
}

const nc = await connect(opts);
console.info(`connected ${nc.getServer()}`);
nc.closed()
  .then((err) => {
    if (err) {
      console.error(`closed with an error: ${err.message}`);
    }
  });

const sc = StringCodec();
const sub = nc.subscribe(subject, { queue: argv.q });
console.info(`${argv.q !== "" ? "queue " : ""}listening to ${subject}`);
for await (const m of sub) {
  console.log(`[${sub.getProcessed()}]: ${m.subject}: ${sc.decode(m.data)}`);
  if (argv.headers && m.headers) {
    const h = [];
    for (const [key, value] of m.headers) {
      h.push(`${key}=${value}`);
    }
    console.log(`\t${h.join(";")}`);
  }
}

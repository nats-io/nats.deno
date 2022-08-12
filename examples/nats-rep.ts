#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.152.0/flags/mod.ts";
import {
  connect,
  ConnectionOptions,
  credsAuthenticator,
  StringCodec,
} from "../src/mod.ts";
import { headers } from "../nats-base-client/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "q": ["queue"],
      "e": ["echo"],
      "f": ["creds"],
    },
    default: {
      s: "127.0.0.1:4222",
      q: "",
    },
    boolean: ["echo", "headers", "debug"],
    string: ["server", "queue", "creds"],
  },
);

const opts = { servers: argv.s } as ConnectionOptions;
const subject = argv._[0] ? String(argv._[0]) : "";
const sc = StringCodec();
const ps = argv._[1] || "";
const payload = sc.encode(String(ps));

if (argv.debug) {
  opts.debug = true;
}

if (argv.h || argv.help || !subject || (argv._[1] && argv.q)) {
  console.log(
    "Usage: nats-rep [-s server] [--creds=/path/file.creds] [-q queue] [--headers] [-e echo_payload] subject [payload]",
  );
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

const nc = await connect(opts);
console.info(`connected ${nc.getServer()}`);
nc.closed()
  .then((err) => {
    if (err) {
      console.error(`closed with an error: ${err.message}`);
    }
  });

const hdrs = argv.headers ? headers() : undefined;
const sub = nc.subscribe(subject, { queue: argv.q });
console.info(`${argv.q !== "" ? "queue " : ""}listening to ${subject}`);
for await (const m of sub) {
  if (hdrs) {
    hdrs.set("sequence", sub.getProcessed().toString());
    hdrs.set("time", Date.now().toString());
  }
  if (m.respond(argv.e ? m.data : payload, { headers: hdrs })) {
    console.log(`[${sub.getProcessed()}]: ${m.reply}: ${m.data}`);
  } else {
    console.log(`[${sub.getProcessed()}]: ignored - no reply subject`);
  }
}

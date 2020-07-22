#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.61.0/flags/mod.ts";
import { ConnectionOptions, connect } from "../src/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "q": ["queue"],
      "e": ["echo"],
    },
    default: {
      s: "nats://127.0.0.1:4222",
      q: "",
    },
    boolean: ["echo", "headers", "debug"],
    string: ["server", "queue"],
  },
);

const opts = { url: argv.s } as ConnectionOptions;
const subject = argv._[0] ? String(argv._[0]) : "";
const payload = argv._[1] || "";

if (argv.headers) {
  opts.headers = true;
}

if (argv.debug) {
  opts.debug = true;
}

if (argv.h || argv.help || !subject || (argv._[1] && argv.q)) {
  console.log(
    "Usage: nats-rep [-s server] [-q queue] [--headers] [-e echo_payload] subject [payload]",
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

const hdrs = argv.headers ? new Headers() : undefined;
const sub = nc.subscribe(subject, { queue: argv.q });
console.info(`${argv.q !== "" ? "queue " : ""}listening to ${subject}`);
for await (const m of sub) {
  if (hdrs) {
    hdrs.set("sequence", sub.getProcessed().toString());
    hdrs.set("time", Date.now().toString());
  }
  debugger;
  if (m.respond(argv.e ? m.data : payload, hdrs)) {
    console.log(`[${sub.getProcessed()}]: ${m.reply}: ${m.data}`);
  } else {
    console.log(`[${sub.getProcessed()}]: ignored - no reply subject`);
  }
}

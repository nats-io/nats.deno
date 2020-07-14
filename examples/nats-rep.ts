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
    boolean: ["echo"],
    default: {
      s: "nats://127.0.0.1:4222",
      q: "",
    },
  },
);

const opts = { url: argv.s } as ConnectionOptions;
const subject = argv._[0] ? String(argv._[0]) : "";
const payload = argv._[1] || "";

if (argv.h || argv.help || !subject || (argv._[1] && argv.q)) {
  console.log(
    "Usage: nats-rep [-s server] [-q queue] [-e echo_payload] subject [payload]",
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

const sub = nc.subscribe(subject, { queue: argv.q });
console.info(`${argv.q !== "" ? "queue " : ""}listening to ${subject}`);
for await (const m of sub) {
  if (m.respond(argv.e ? m.data : payload)) {
    console.log(`[${sub.getProcessed()}]: ${m.reply}: ${m.data}`);
  } else {
    console.log(`[${sub.getProcessed()}]: ignored - no reply subject`);
  }
}

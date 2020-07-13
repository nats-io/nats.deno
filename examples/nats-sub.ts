#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std/flags/mod.ts";
import { ConnectionOptions, connect } from "../src/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "q": ["queue"],
    },
    default: {
      s: "nats://127.0.0.1:4222",
      q: "",
    },
  },
);

const opts = { url: argv.s } as ConnectionOptions;
const subject = argv._[0] ? String(argv._[0]) : ">";

if (argv.h || argv.help || !subject) {
  console.log("Usage: nats-sub [-s server] [-q queue] subject");
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
  console.log(`[${sub.getReceived()}]: ${m.subject}: ${m.data}`);
}

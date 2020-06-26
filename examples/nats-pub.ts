#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std/flags/mod.ts";
import { ConnectionOptions, connect } from "../src/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
    },
    default: {
      s: "nats://localhost:4222",
    },
  },
);

const opts = { url: argv.s } as ConnectionOptions;
const subject = String(argv._[0]);
const payload = argv._[1] || "";

if (!subject) {
  console.log("Usage: nats-pub [--s server] subject [msg]");
  Deno.exit(1);
}

const nc = await connect(opts);

nc.addEventListener("error", (err: Error): void => {
  console.error(err);
});

nc.publish(subject, payload);
await nc.flush();
await nc.close();

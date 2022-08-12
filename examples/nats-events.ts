#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.152.0/flags/mod.ts";
import { connect, ConnectionOptions } from "../src/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
    },
    default: {
      s: "127.0.0.1:4222",
    },
  },
);

const opts = { servers: argv.s } as ConnectionOptions;

const nc = await connect(opts);
(async () => {
  console.info(`connected ${nc.getServer()}`);
  for await (const s of nc.status()) {
    console.info(`${s.type}: ${JSON.stringify(s.data)}`);
  }
})().then();

await nc.closed()
  .then((err) => {
    if (err) {
      console.error(`closed with an error: ${err.message}`);
    }
  });

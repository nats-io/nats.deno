#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std/flags/mod.ts";
import { ConnectionOptions, connect, Events } from "../src/mod.ts";

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
const subject = argv._[0] ? String(argv._[0]) : ">";

const nc = await connect(opts);
nc.status().then(() => {
  console.log("client closed");
  Deno.exit(1);
});

nc.addEventListener(Events.DISCONNECT, () => {
  console.log("disconnected");
});

nc.addEventListener(Events.RECONNECT, () => {
  console.log("reconnected");
});

console.log(`subscribing to ${subject}`);
const sub = nc.subscribe(subject, (err, msg) => {
  if (err) {
    console.error(err);
    return;
  }
  console.log(`[${sub.getReceived()}]: ${msg.subject}: ${msg.data}`);
});

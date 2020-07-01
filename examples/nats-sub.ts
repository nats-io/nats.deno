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

opts.maxReconnectAttempts = 10;
opts.reconnectTimeWait = 5;

const nc = await connect(opts);
console.info(`connected ${nc.getServer()}`);
nc.status().then(() => {
  console.log(`closed ${nc.getServer()}`);
  Deno.exit(1);
});

nc.addEventListener(Events.DISCONNECT, () => {
  console.info(`disconnected ${nc.getServer()}`);
});

nc.addEventListener(Events.RECONNECT, () => {
  console.info(`reconnected ${nc.getServer()}`);
});

nc.addEventListener(
  Events.UPDATE,
  ((evt: CustomEvent) => {
    console.info(`cluster updated`);
    console.table(evt.detail);
  }) as EventListener,
);

console.info(`subscribing to ${subject}`);
const sub = nc.subscribe(subject, (err, msg) => {
  if (err) {
    console.error(err);
    return;
  }
  console.log(`[${sub.getReceived()}]: ${msg.subject}: ${msg.data}`);
});

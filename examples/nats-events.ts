#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.61.0/flags/mod.ts";
import { ConnectionOptions, connect, Events } from "../src/mod.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
    },
    default: {
      s: "nats://127.0.0.1:4222",
    },
  },
);

const opts = { url: argv.s } as ConnectionOptions;

const nc = await connect(opts);
console.info(`connected ${nc.getServer()}`);

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

await nc.closed()
  .then((err) => {
    if (err) {
      console.error(`closed with an error: ${err.message}`);
    }
  });

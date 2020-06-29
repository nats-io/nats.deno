#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std/flags/mod.ts";
import { ConnectionOptions, connect } from "../src/mod.ts";
import { CLOSE_EVT } from "../nats-base-client/mod.ts";
import {
  DISCONNECT_EVT,
  RECONNECT_EVT,
} from "../nats-base-client/types.ts";

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

nc.addEventListener(CLOSE_EVT, () => {
  console.log("client closed");
});
nc.addEventListener(
  CLOSE_EVT,
  ((evt: ErrorEvent): void => {
    console.error(evt.error);
    Deno.exit(1);
  }) as EventListener,
);
nc.addEventListener(DISCONNECT_EVT, () => {
  console.log("disconnected");
});

nc.addEventListener(RECONNECT_EVT, () => {
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

#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.63.0/flags/mod.ts";
import { connect, Empty, Nuid } from "../src/mod.ts";
const defaults = {
  s: "127.0.0.1:4222",
  c: 1000000,
  p: 0,
  subj: new Nuid().next(),
};

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "c": ["count"],
      "d": ["debug"],
      "p": ["payload"],
    },
    default: defaults,
    string: [
      "subj",
    ],
  },
);

if (argv.h || argv.help || (!argv.sub && !argv.pub && !argv.req)) {
  console.log(
    "usage: bench.ts [--pub] [--sub] [--req (--async)] [--count messages:1M] [--payload <#bytes>] [--server server]\n",
  );
  Deno.exit(0);
}

const server = argv.server;
const count = parseInt(argv.count);
const subj = argv.subj;
const bytes = parseInt(argv.payload);
const payload = bytes ? new Uint8Array(bytes) : Empty;

const nc = await connect({ servers: server, debug: argv.debug });
const start = Date.now();

if (argv.req) {
  const sub = nc.subscribe(subj, { max: count });
  const iter = (async () => {
    for await (const m of sub) {
      m.respond(payload);
    }
  })();
}

let j = 0;
if (argv.sub) {
  const sub = nc.subscribe(subj, { max: count });
  const iter = (async () => {
    for await (const m of sub) {}
    console.log("sub done");
  })();
}

let i = 0;
if (argv.pub) {
  for (; i < count; i++) {
    nc.publish(subj, payload);
  }
}

if (argv.req) {
  if (argv.async) {
    const a = [];
    for (; i < count; i++) {
      a.push(nc.request(subj, payload, { timeout: 20000 }));
    }
    await Promise.all(a);
  } else {
    for (; i < count; i++) {
      await nc.request(subj);
    }
  }
}

nc.closed()
  .then((err) => {
    if (err) {
      console.error(`bench closed with an error: ${err.message}`);
    }
  });

await nc.drain();
const millis = Date.now() - start;
const secs = millis / 1000;

if ((argv.pub && !argv.sub) || (argv.sub && !argv.pub)) {
  console.log(`${Math.round(i / secs)} msgs/sec - ${millis} millis`);
} else {
  console.log(
    `${Math.round((argv.c * 2) / secs)} msgs/sec - ${millis} millis - ${j}`,
  );
}

if (argv.req) {
  const rps = parseInt(String(argv.c / secs), 10);
  console.log(`${rps} req-resp/sec - ${millis} millis - ${j}`);
  const lat = Math.round((millis * 1000) / (argv.c * 2));
  console.log(`average latency ${lat} µs`);
}

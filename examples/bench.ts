#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@v0.56.0/flags/mod.ts";
import { connect, Nuid } from "https://deno.land/x/nats/src/mod.ts";
const defaults = {
  s: "nats://localhost:4222",
  c: 1000000,
};

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "c": ["count"],
      "d": ["debug"],
    },
    default: defaults,
  },
);

if (argv.h || argv.help) {
  console.log(
    "usage: bench.ts [--pub] [--sub] [--req (--async)] [--count messages:1M] [--server server]\n",
  );
  Deno.exit(0);
}

const server = String(argv.server);
const count = parseInt(String(argv.count));
const subj = String(argv.subj) || new Nuid().next();

const nc = await connect({ url: server, debug: argv.debug });
nc.addEventListener("error", (err: Error): void => {
  console.error(err);
});
const start = Date.now();

if (argv.req) {
  nc.subscribe(subj, (_, m) => {
    m.respond("ok");
  });
}

let j = 0;
if (argv.sub) {
  nc.subscribe(subj, () => {
    j++;
  });
}

let i = 0;
if (argv.pub) {
  for (; i < count; i++) {
    nc.publish(subj, "ok");
  }
}

if (argv.req) {
  if (argv.async) {
    const a = [];
    for (; i < count; i++) {
      a.push(nc.request(subj, 20000));
    }
    await Promise.all(a);
  } else {
    for (; i < count; i++) {
      await nc.request(subj);
    }
  }
}

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

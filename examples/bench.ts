#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.63.0/flags/mod.ts";
import { connect, Empty, Nuid } from "../src/mod.ts";
import { Bench, BenchOpts, Metric } from "../nats-base-client/bench.ts";
const defaults = {
  s: "127.0.0.1:4222",
  c: 100000,
  p: 128,
  subject: new Nuid().next(),
  i: 1,
};

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "c": ["count"],
      "d": ["debug"],
      "p": ["payload"],
      "i": ["iterations"],
    },
    default: defaults,
    string: [
      "subject",
    ],
    boolean: [
      "async",
    ],
  },
);

if (argv.h || argv.help || (!argv.sub && !argv.pub && !argv.req)) {
  console.log(
    "usage: bench.ts [--iterations <#loop: 1>] [--pub] [--sub] [--req (--async)] [--count messages:1M] [--payload <#bytes>=128] [--server server] [--subject <subj>]\n",
  );
  Deno.exit(0);
}

const server = argv.server;
const count = parseInt(argv.count);
const bytes = parseInt(argv.payload);
const iters = parseInt(argv.iterations);
const metrics: Metric[] = [];

for (let i = 0; i < iters; i++) {
  const nc = await connect({ servers: server, debug: argv.debug });
  const opts = {
    msgs: count,
    size: bytes,
    async: argv.async,
    pub: argv.pub,
    sub: argv.sub,
    req: argv.req,
    subject: argv.subject,
  } as BenchOpts;

  const bench = new Bench(nc, opts);
  const m = await bench.run();
  metrics.push(...m);
  await nc.close();
}

const reducer = (a: Metric, m: Metric) => {
  if (a) {
    a.name = m.name;
    a.payload = m.payload;
    a.bytes += m.bytes;
    a.duration += m.duration;
    a.msgs += m.msgs;
    a.lang = m.lang;
    a.version = m.version;
    a.async = m.async;

    a.max = Math.max((a.max === undefined ? 0 : a.max), m.duration);
    a.min = Math.min((a.min === undefined ? m.duration : a.max), m.duration);
  }
  return a;
};

const pubsub = metrics.filter((m) => m.name === "pubsub").reduce(
  reducer,
  new Metric("pubsub", 0),
);
const pub = metrics.filter((m) => m.name === "pub").reduce(
  reducer,
  new Metric("pub", 0),
);
const sub = metrics.filter((m) => m.name === "sub").reduce(
  reducer,
  new Metric("sub", 0),
);
const req = metrics.filter((m) => m.name === "req").reduce(
  reducer,
  new Metric("req", 0),
);

if (pubsub && pubsub.msgs) {
  console.log(pubsub.toString());
}
if (pub && pub.msgs) {
  console.log(pub.toString());
}
if (sub && sub.msgs) {
  console.log(sub.toString());
}
if (req && req.msgs) {
  console.log(req.toString());
}

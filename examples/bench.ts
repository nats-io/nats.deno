#!/usr/bin/env deno run --allow-all --unstable

import { parse } from "https://deno.land/std@0.63.0/flags/mod.ts";
import { connect, Empty, Nuid } from "../src/mod.ts";
const defaults = {
  s: "127.0.0.1:4222",
  c: 100000,
  p: 128,
  subject: new Nuid().next(),
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
      "subject",
    ],
    boolean: [
      "async",
    ],
  },
);

if (argv.h || argv.help || (!argv.sub && !argv.pub && !argv.req)) {
  console.log(
    "usage: bench.ts [--pub] [--sub] [--req (--async)] [--count messages:1M] [--payload <#bytes>=128] [--server server] [--subject <subj>]\n",
  );
  Deno.exit(0);
}

const server = argv.server;
const count = parseInt(argv.count);
const bytes = parseInt(argv.payload);
const payload = bytes ? new Uint8Array(bytes) : Empty;

const nc = await connect({ servers: server, debug: argv.debug });
const jobs: Promise<void>[] = [];
const p = new Performance();

if (argv.req) {
  const sub = nc.subscribe(argv.subject, { max: count });
  const job = (async () => {
    for await (const m of sub) {
      m.respond(payload);
    }
  })();
  jobs.push(job);
}

if (argv.sub) {
  let first = false;
  const sub = nc.subscribe(argv.subject, { max: count });
  const job = (async () => {
    for await (const m of sub) {
      if (!first) {
        p.mark("subStart");
        first = true;
      }
    }
    p.mark("subStop");
    p.measure("sub", "subStart", "subStop");
  })();
  jobs.push(job);
}

if (argv.pub) {
  const job = (async () => {
    p.mark("pubStart");
    for (let i = 0; i < count; i++) {
      nc.publish(argv.subject, payload);
    }
    await nc.flush();
    p.mark("pubStop");
    p.measure("pub", "pubStart", "pubStop");
  })();
  jobs.push(job);
}

if (argv.req) {
  const job = (async () => {
    if (argv.async) {
      p.mark("reqStart");
      const a = [];
      for (let i = 0; i < count; i++) {
        a.push(nc.request(argv.subject, payload, { timeout: 20000 }));
      }
      await Promise.all(a);
      p.mark("reqStop");
      p.measure("req", "reqStart", "reqStop");
    } else {
      p.mark("reqStart");
      for (let i = 0; i < count; i++) {
        await nc.request(argv.subject);
      }
      p.mark("reqStop");
      p.measure("req", "reqStart", "reqStop");
    }
  })();
  jobs.push(job);
}

nc.closed()
  .then((err) => {
    if (err) {
      console.error(`bench closed with an error: ${err.message}`);
    }
  });

await Promise.all(jobs);
const measures = p.getEntries();
const req = measures.find((m) => m.name === "req");
const pub = measures.find((m) => m.name === "pub");
const sub = measures.find((m) => m.name === "sub");

const stats = nc.stats();

if (pub && sub) {
  const sec = (pub.duration + sub.duration) / 1000;
  const mps = Math.round((argv.c * 2) / sec);
  console.log(
    `pubsub ${humanizeNumber(mps)} msgs/sec - [${sec.toFixed(2)} secs] ~ ${
      throughput(stats.inBytes + stats.outBytes, sec)
    }`,
  );
}
if (pub) {
  const sec = pub.duration / 1000;
  const mps = Math.round(argv.c / sec);
  console.log(
    `pub    ${humanizeNumber(mps)} msgs/sec - [${sec.toFixed(2)} secs] ~ ${
      throughput(stats.outBytes, sec)
    }`,
  );
}
if (sub) {
  const sec = sub.duration / 1000;
  const mps = Math.round(argv.c / sec);
  console.log(
    `sub    ${humanizeNumber(mps)} msgs/sec - [${sec.toFixed(2)} secs] ~ ${
      throughput(stats.inBytes, sec)
    }`,
  );
}

if (req) {
  const sec = req.duration / 1000;
  const mps = Math.round((argv.c * 2) / req.duration);
  const label = argv.async ? "async" : "serial";
  console.log(
    `req ${label} ${humanizeNumber(mps)} msgs/sec - [${sec.toFixed(2)} secs]`,
  );
}

console.table(nc.stats());

await nc.close();

function throughput(bytes: number, seconds: number): string {
  return humanizeBytes(bytes / seconds);
}

function humanizeBytes(bytes: number, si = false): string {
  const base = si ? 1000 : 1024;
  const pre = si
    ? ["k", "M", "G", "T", "P", "E"]
    : ["K", "M", "G", "T", "P", "E"];
  const post = si ? "iB" : "B";

  if (bytes < base) {
    return `${bytes.toFixed(2)} ${post}/sec`;
  }
  const exp = parseInt(Math.log(bytes) / Math.log(base) + "");
  let index = parseInt((exp - 1) + "");
  return `${(bytes / Math.pow(base, exp)).toFixed(2)} ${pre[index]}${post}/sec`;
}

function humanizeNumber(n: number) {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

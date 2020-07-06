import {
  NatsServer,
} from "./mod.ts";
import { parse } from "https://deno.land/std/flags/mod.ts";

const defaults = {
  c: 2,
  p: 4222,
};

const argv = parse(
  Deno.args,
  {
    alias: {
      "p": ["port"],
      "c": ["count"],
      "d": ["debug"],
    },
    default: defaults,
  },
);

if (argv.h || argv.help) {
  console.log(
    "usage: cluster [--count 2] [--port 4222] [--debug]\n",
  );
  Deno.exit(0);
}

try {
  const cluster = await NatsServer.cluster(
    argv.count,
    { port: argv.port },
    argv.debug,
  );
  cluster.forEach((s) => {
    console.log(
      `launched server [${s.process.pid}] at ${s.hostname}:${s.port}`,
    );
  });

  await waitForStop();
} catch (err) {
  console.error(err);
}

async function waitForStop() {
  console.log("control+c to terminate");
  const interval = setInterval(() => {}, Number.MAX_SAFE_INTEGER);
  Deno.signal(Deno.Signal.SIGINT)
    .then(() => {
      clearInterval(interval);
    });
}

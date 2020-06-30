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
    "usage: cluster --count 2\n",
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
    console.log(`launched server at ${s.port}`);
  });

  console.log("control+c to terminate");
  await new Promise((resolve) => {
    Deno.signal(Deno.Signal.SIGINT)
      .then(() => {
        resolve();
      });
  });
} catch (err) {
  console.error(err);
}

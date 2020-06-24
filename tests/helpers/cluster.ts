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
  const ns = await NatsServer.start({
    port: argv.port,
    cluster: {
      listen: "127.0.0.1:-1",
    },
  }, argv.debug);
  console.log(`launched server at ${ns.port}`);

  for (let i = 0; i < argv.count; i++) {
    const s = await NatsServer.start({
      cluster: {
        listen: "127.0.0.1:-1",
        routes: [`nats://${ns.hostname}:${ns.cluster}`],
      },
    }, argv.debug);
    console.log(`launched cluster member at ${s.port}`);
  }

  console.log("control+c to terminate");
  await new Promise((resolve, reject) => {
    Deno.signal(Deno.Signal.SIGINT)
      .then(() => {
        resolve();
      });
  });
} catch (err) {
  console.error(err);
}

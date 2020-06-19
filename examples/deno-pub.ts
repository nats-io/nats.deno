import parseArgs from "https://deno.land/x/deno_minimist@1.0.0/mod.ts";
import { connect, ConnectionOptions, DEFAULT_URL } from "../src/mod.ts";

const argv = parseArgs(Deno.args);
const opts = {} as ConnectionOptions;

opts.url = String(argv.s) || DEFAULT_URL;

const subject = String(argv._[0]);
const payload = argv._[1] || "";

if (!subject) {
  console.log("Usage: deno-pub [-s server] <subject> [msg]");
  Deno.exit(1);
}

const nc = await connect(opts);

nc.addEventListener("error", (err: Error): void => {
  console.log("error", err);
});

nc.publish(subject, payload);
await nc.flush();
await nc.close();

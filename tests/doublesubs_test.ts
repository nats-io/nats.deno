import { NatsServer } from "./helpers/launcher.ts";
import { connect } from "../src/connect.ts";
import {
  deferred,
  Empty,
  Events,
  headers,
  NatsConnectionImpl,
  StringCodec,
} from "../nats-base-client/internal_mod.ts";
import {
  assertEquals,
  assertArrayContains,
} from "https://deno.land/std@0.63.0/testing/asserts.ts";
import { extend } from "../nats-base-client/util.ts";
import { join, resolve } from "https://deno.land/std@0.63.0/path/mod.ts";

async function runDoubleSubsTest(tls: boolean) {
  const cwd = Deno.cwd();

  let opts = { trace: true, host: "0.0.0.0" };

  const tlsconfig = {
    tls: {
      cert_file: resolve(join(cwd, "./tests/certs/localhost.crt")),
      key_file: resolve(join(cwd, "./tests/certs/localhost.key")),
      ca_file: resolve(join(cwd, "./tests/certs/RootCA.crt")),
    },
  };

  if (tls) {
    opts = extend(opts, tlsconfig);
  }

  let srv = await NatsServer.start(opts);

  let connOpts = {
    servers: `localhost:${srv.port}`,
    reconnectTimeWait: 500,
    maxReconnectAttempts: -1,
    headers: true,
  };

  const cert = {
    tls: {
      caFile: resolve(join(cwd, "./tests/certs/RootCA.crt")),
    },
  };
  if (tls) {
    connOpts = extend(connOpts, cert);
  }
  let nc = await connect(connOpts) as NatsConnectionImpl;

  const disconnected = deferred<void>();
  const reconnected = deferred<void>();
  let _ = (async () => {
    for await (const e of nc.status()) {
      switch (e.type) {
        case Events.DISCONNECT:
          disconnected.resolve();
          break;
        case Events.RECONNECT:
          reconnected.resolve();
          break;
      }
    }
  })();

  await nc.flush();
  await srv.stop();
  await disconnected;

  const foo = nc.subscribe("foo");
  const bar = nc.subscribe("bar");
  const baz = nc.subscribe("baz");
  nc.publish("foo", Empty);
  nc.publish("bar", StringCodec().encode("hello"));
  const h = headers();
  h.set("foo", "bar");
  nc.publish("baz", Empty, { headers: h });

  srv = await srv.restart();
  await reconnected;
  await nc.flush();

  assertEquals(foo.getReceived(), 1);
  assertEquals(bar.getReceived(), 1);
  assertEquals(baz.getReceived(), 1);

  await nc.close();
  await srv.stop();

  const log = srv.getLog();

  let count = 0;
  const subs: string[] = [];
  const sub = /\[SUB (\S+) \d ]/;
  log.split("\n").forEach((s) => {
    const m = sub.exec(s);
    if (m) {
      count++;
      subs.push(m[1]);
    }
  });

  assertEquals(count, 3);
  assertArrayContains(subs, ["foo", "bar", "baz"]);
}

Deno.test("doublesubs - standard", async () => {
  await runDoubleSubsTest(false);
});

Deno.test("doublesubs - tls", async () => {
  await runDoubleSubsTest(true);
});

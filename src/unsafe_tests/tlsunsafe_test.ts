import { join, resolve } from "jsr:@std/path";
import { NatsServer } from "../tests/helpers/launcher.ts";
import { connect } from "../mod.ts";

Deno.test("tls-unsafe - handshake first", async () => {
  const cwd = Deno.cwd();
  const config = {
    host: "localhost",
    tls: {
      handshake_first: true,
      cert_file: resolve(join(cwd, "./src/tests/certs/localhost.crt")),
      key_file: resolve(join(cwd, "./src/tests/certs/localhost.key")),
      ca_file: resolve(join(cwd, "./src/tests/certs/RootCA.crt")),
    },
  };

  const ns = await NatsServer.start(config);
  const nc = await connect({
    debug: true,
    servers: `localhost:${ns.port}`,
    tls: {
      handshakeFirst: true,
      caFile: config.tls.ca_file,
    },
  });
  nc.subscribe("foo", {
    callback(_err, msg) {
      msg.respond(msg.data);
    },
  });

  await nc.request("foo", "hello");
  await nc.close();
  await ns.stop();
});

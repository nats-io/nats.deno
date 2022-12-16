import { cleanup, setup } from "./jstest_util.ts";
import {
  credsAuthenticator,
  deferred,
  Events,
  jwtAuthenticator,
  NatsConnection,
  nkeyAuthenticator,
  tokenAuthenticator,
  usernamePasswordAuthenticator,
} from "../nats-base-client/mod.ts";
import { assertEquals } from "https://deno.land/std@0.168.0/testing/asserts.ts";
import { nkeys } from "../nats-base-client/nkeys.ts";
import {
  encodeAccount,
  encodeOperator,
  encodeUser,
  fmtCreds,
} from "https://raw.githubusercontent.com/nats-io/jwt.js/main/src/jwt.ts";

function disconnectReconnect(nc: NatsConnection): Promise<void> {
  const done = deferred<void>();
  const disconnect = deferred();
  const reconnect = deferred();
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === Events.Disconnect) {
        disconnect.resolve();
      } else if (s.type === Events.Reconnect) {
        reconnect.resolve();
      }
    }
  })().then();

  Promise.all([disconnect, reconnect])
    .then(() => done.resolve()).catch((err) => done.reject(err));
  return done;
}

Deno.test("authenticator - username password fns", async () => {
  let user = "a";
  let pass = "a";
  const authenticator = usernamePasswordAuthenticator(() => {
    return user;
  }, () => {
    return pass;
  });

  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
      }],
    },
  }, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    user = "b";
    pass = "b";
    await ns.reload({
      authorization: {
        users: [{
          user: "b",
          password: "b",
        }],
      },
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("authenticator - username string password fn", async () => {
  let pass = "a";
  const authenticator = usernamePasswordAuthenticator("a", () => {
    return pass;
  });

  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
      }],
    },
  }, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    pass = "b";
    await ns.reload({
      authorization: {
        users: [{
          user: "a",
          password: "b",
        }],
      },
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("authenticator - username fn password string", async () => {
  let user = "a";
  const authenticator = usernamePasswordAuthenticator(() => {
    return user;
  }, "a");

  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
      }],
    },
  }, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    user = "b";
    await ns.reload({
      authorization: {
        users: [{
          user: "b",
          password: "a",
        }],
      },
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("authenticator - token fn", async () => {
  let token = "tok";
  const authenticator = tokenAuthenticator(() => {
    return token;
  });

  const { ns, nc } = await setup({
    authorization: {
      token,
    },
  }, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    token = "token2";
    await ns.reload({
      authorization: {
        token,
      },
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("authenticator - nkey fn", async () => {
  const user = nkeys.createUser();
  let seed = user.getSeed();
  let nkey = user.getPublicKey();

  const authenticator = nkeyAuthenticator(() => {
    console.log(`using ${seed}`);
    return seed;
  });

  const { ns, nc } = await setup({
    authorization: {
      users: [
        { nkey },
      ],
    },
  }, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    const user = nkeys.createUser();
    seed = user.getSeed();
    nkey = user.getPublicKey();

    await ns.reload({
      authorization: {
        users: [
          { nkey },
        ],
      },
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("authenticator - jwt bearer fn", async () => {
  const O = nkeys.createOperator();
  let A = nkeys.createAccount();
  let U = nkeys.createUser();
  let ujwt = await encodeUser("U", U, A, { bearer_token: true }, {
    exp: Math.round(Date.now() / 1000) + 3,
  });

  const authenticator = jwtAuthenticator(() => {
    return ujwt;
  });

  let resolver: Record<string, string> = {};
  resolver[A.getPublicKey()] = await encodeAccount("A", A, {
    limits: {
      conn: -1,
      subs: -1,
    },
  }, { signer: O });
  const conf = {
    operator: await encodeOperator("O", O),
    resolver: "MEMORY",
    "resolver_preload": resolver,
  };

  const { ns, nc } = await setup(conf, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    A = nkeys.createAccount();
    U = nkeys.createUser();
    ujwt = await encodeUser("U", U, A, { bearer_token: true }, {
      exp: Math.round(Date.now() / 1000) + 3,
    });
    resolver = {};
    resolver[A.getPublicKey()] = await encodeAccount("AA", A, {
      limits: {
        conn: -1,
        subs: -1,
      },
    }, { signer: O });

    await ns.reload({
      "resolver_preload": resolver,
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("authenticator - jwt fn", async () => {
  const O = nkeys.createOperator();
  let A = nkeys.createAccount();
  let U = nkeys.createUser();
  let ujwt = await encodeUser("U", U, A, {}, {
    exp: Math.round(Date.now() / 1000) + 3,
  });

  const authenticator = jwtAuthenticator(() => {
    return ujwt;
  }, () => {
    return U.getSeed();
  });

  let resolver: Record<string, string> = {};
  resolver[A.getPublicKey()] = await encodeAccount("A", A, {
    limits: {
      conn: -1,
      subs: -1,
    },
  }, { signer: O });
  const conf = {
    operator: await encodeOperator("O", O),
    resolver: "MEMORY",
    "resolver_preload": resolver,
  };

  const { ns, nc } = await setup(conf, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    A = nkeys.createAccount();
    U = nkeys.createUser();
    ujwt = await encodeUser("U", U, A, {}, {
      exp: Math.round(Date.now() / 1000) + 3,
    });
    resolver = {};
    resolver[A.getPublicKey()] = await encodeAccount("AA", A, {
      limits: {
        conn: -1,
        subs: -1,
      },
    }, { signer: O });

    await ns.reload({
      "resolver_preload": resolver,
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

Deno.test("authenticator - creds fn", async () => {
  const O = nkeys.createOperator();
  let A = nkeys.createAccount();
  let U = nkeys.createUser();
  let ujwt = await encodeUser("U", U, A, {}, {
    exp: Math.round(Date.now() / 1000) + 3,
  });
  let creds = fmtCreds(ujwt, U);

  const authenticator = credsAuthenticator(() => {
    return creds;
  });

  let resolver: Record<string, string> = {};
  resolver[A.getPublicKey()] = await encodeAccount("A", A, {
    limits: {
      conn: -1,
      subs: -1,
    },
  }, { signer: O });
  const conf = {
    operator: await encodeOperator("O", O),
    resolver: "MEMORY",
    "resolver_preload": resolver,
  };

  const { ns, nc } = await setup(conf, {
    authenticator,
  });

  const cycle = disconnectReconnect(nc);

  setTimeout(async () => {
    A = nkeys.createAccount();
    U = nkeys.createUser();
    ujwt = await encodeUser("U", U, A, {}, {
      exp: Math.round(Date.now() / 1000) + 3,
    });
    creds = fmtCreds(ujwt, U);
    resolver = {};
    resolver[A.getPublicKey()] = await encodeAccount("AA", A, {
      limits: {
        conn: -1,
        subs: -1,
      },
    }, { signer: O });

    await ns.reload({
      "resolver_preload": resolver,
    });
  }, 2000);

  await cycle;
  assertEquals(nc.isClosed(), false);
  await cleanup(ns, nc);
});

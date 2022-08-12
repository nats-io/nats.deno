/*
 * Copyright 2018-2022 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  assertEquals,
  assertRejects,
  fail,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import {
  connect,
  createInbox,
  credsAuthenticator,
  Empty,
  ErrorCode,
  Events,
  jwtAuthenticator,
  NatsConnection,
  NatsError,
  nkeyAuthenticator,
  Status,
  StringCodec,
  tokenAuthenticator,
  usernamePasswordAuthenticator,
  UserPass,
} from "../src/mod.ts";
import { assertErrorCode, NatsServer } from "./helpers/mod.ts";
import { deferred, nkeys } from "../nats-base-client/internal_mod.ts";
import { NKeyAuth } from "../nats-base-client/authenticator.ts";
import { assert } from "../nats-base-client/denobuffer.ts";
import { cleanup, setup } from "./jstest_util.ts";
import {
  encodeAccount,
  encodeOperator,
  encodeUser,
} from "https://raw.githubusercontent.com/nats-io/jwt.js/main/src/jwt.ts";

const conf = {
  authorization: {
    users: [{
      user: "derek",
      password: "foobar",
      permission: {
        subscribe: "bar",
        publish: "foo",
      },
    }],
  },
};

Deno.test("auth - none", async () => {
  const ns = await NatsServer.start(conf);
  try {
    const nc = await connect(
      { port: ns.port },
    );
    await nc.close();
    fail("shouldnt have been able to connect");
  } catch (ex) {
    assertErrorCode(ex, ErrorCode.AuthorizationViolation);
  }
  await ns.stop();
});

Deno.test("auth - bad", async () => {
  const ns = await NatsServer.start(conf);
  try {
    const nc = await connect(
      { port: ns.port, user: "me", pass: "hello" },
    );
    await nc.close();
    fail("shouldnt have been able to connect");
  } catch (ex) {
    assertErrorCode(ex, ErrorCode.AuthorizationViolation);
  }
  await ns.stop();
});

Deno.test("auth - weird chars", async () => {
  const pass = "§12§12§12";
  const ns = await NatsServer.start({
    authorization: {
      username: "admin",
      password: pass,
    },
  });

  const nc = await connect(
    { port: ns.port, user: "admin", pass: pass },
  );
  await nc.flush;
  await nc.close();
  await ns.stop();
});

Deno.test("auth - un/pw", async () => {
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    { port: ns.port, user: "derek", pass: "foobar" },
  );
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - un/pw authenticator", async () => {
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    {
      port: ns.port,
      authenticator: usernamePasswordAuthenticator("derek", "foobar"),
    },
  );
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - sub no permissions keeps connection", async () => {
  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
        permissions: { subscribe: "foo" },
      }],
    },
  }, { user: "a", pass: "a", reconnect: false });

  const errStatus = deferred<Status>();
  const _ = (async () => {
    for await (const s of nc.status()) {
      errStatus.resolve(s);
    }
  })();

  const cbErr = deferred<Error | null>();
  const sub = nc.subscribe("bar", {
    callback: (err, _msg) => {
      cbErr.resolve(err);
    },
  });

  const v = await Promise.all([errStatus, cbErr, sub.closed]);
  assertEquals(v[0].data, ErrorCode.PermissionsViolation);
  assertEquals(
    v[1]?.message,
    "'Permissions Violation for Subscription to \"bar\"'",
  );
  assertEquals(nc.isClosed(), false);

  await cleanup(ns, nc);
});

Deno.test("auth - sub iterator no permissions keeps connection", async () => {
  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
        permissions: { subscribe: "foo" },
      }],
    },
  }, { user: "a", pass: "a", reconnect: false });

  const errStatus = deferred<Status>();
  const _ = (async () => {
    for await (const s of nc.status()) {
      errStatus.resolve(s);
    }
  })();

  const iterErr = deferred<Error | null>();
  const sub = nc.subscribe("bar");
  (async () => {
    for await (const _m of sub) {
      // ignored
    }
  })().catch((err) => {
    iterErr.resolve(err);
  });

  await nc.flush();

  const v = await Promise.all([errStatus, iterErr, sub.closed]);
  assertEquals(v[0].data, ErrorCode.PermissionsViolation);
  assertEquals(
    v[1]?.message,
    "'Permissions Violation for Subscription to \"bar\"'",
  );
  assertEquals(sub.isClosed(), true);
  assertEquals(nc.isClosed(), false);

  await cleanup(ns, nc);
});

Deno.test("auth - pub permissions keep connection", async () => {
  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
        permissions: { publish: "foo" },
      }],
    },
  }, { user: "a", pass: "a", reconnect: false });

  const errStatus = deferred<Status>();
  const _ = (async () => {
    for await (const s of nc.status()) {
      errStatus.resolve(s);
    }
  })();

  nc.publish("bar");

  const v = await errStatus;
  assertEquals(v.data, ErrorCode.PermissionsViolation);
  assertEquals(nc.isClosed(), false);

  await cleanup(ns, nc);
});

Deno.test("auth - req permissions keep connection", async () => {
  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
        permissions: { publish: "foo" },
      }],
    },
  }, { user: "a", pass: "a", reconnect: false });

  const errStatus = deferred<Status>();
  const _ = (async () => {
    for await (const s of nc.status()) {
      errStatus.resolve(s);
    }
  })();

  await assertRejects(
    async () => {
      await nc.request("bar");
    },
    (err: Error) => {
      const ne = err as NatsError;
      assertEquals(ne.code, ErrorCode.PermissionsViolation);
    },
  );

  const v = await errStatus;
  assertEquals(v.data, ErrorCode.PermissionsViolation);
  assertEquals(nc.isClosed(), false);

  await cleanup(ns, nc);
});

Deno.test("auth - user and token is rejected", () => {
  connect({ servers: "127.0.0.1:4222", user: "derek", token: "foobar" })
    .then(async (nc) => {
      await nc.close();
      fail("should not have connected");
    })
    .catch((err) => {
      assertErrorCode(err, ErrorCode.BadAuthentication);
    });
});

Deno.test("auth - token", async () => {
  const ns = await NatsServer.start({ authorization: { token: "foo" } });
  const nc = await connect({ port: ns.port, token: "foo" });
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - token authenticator", async () => {
  const ns = await NatsServer.start({ authorization: { token: "foo" } });
  const nc = await connect({
    port: ns.port,
    authenticator: tokenAuthenticator("foo"),
  });
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - nkey", async () => {
  const kp = nkeys.createUser();
  const pk = kp.getPublicKey();
  const seed = kp.getSeed();
  const conf = {
    authorization: {
      users: [
        { nkey: pk },
      ],
    },
  };
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    { port: ns.port, authenticator: nkeyAuthenticator(seed) },
  );
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - creds", async () => {
  const creds = `-----BEGIN NATS USER JWT-----
    eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJFU1VQS1NSNFhGR0pLN0FHUk5ZRjc0STVQNTZHMkFGWERYQ01CUUdHSklKUEVNUVhMSDJBIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJBQ1pTV0JKNFNZSUxLN1FWREVMTzY0VlgzRUZXQjZDWENQTUVCVUtBMzZNSkpRUlBYR0VFUTJXSiIsInN1YiI6IlVBSDQyVUc2UFY1NTJQNVNXTFdUQlAzSDNTNUJIQVZDTzJJRUtFWFVBTkpYUjc1SjYzUlE1V002IiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ.kCR9Erm9zzux4G6M-V2bp7wKMKgnSNqMBACX05nwePRWQa37aO_yObbhcJWFGYjo1Ix-oepOkoyVLxOJeuD8Bw
  ------END NATS USER JWT------

************************* IMPORTANT *************************
  NKEY Seed printed below can be used sign and prove identity.
    NKEYs are sensitive and should be treated as secrets.

  -----BEGIN USER NKEY SEED-----
    SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4
  ------END USER NKEY SEED------
`;

  const conf = {
    operator:
      "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJhdWQiOiJURVNUUyIsImV4cCI6MTg1OTEyMTI3NSwianRpIjoiWE5MWjZYWVBIVE1ESlFSTlFPSFVPSlFHV0NVN01JNVc1SlhDWk5YQllVS0VRVzY3STI1USIsImlhdCI6MTU0Mzc2MTI3NSwiaXNzIjoiT0NBVDMzTVRWVTJWVU9JTUdOR1VOWEo2NkFIMlJMU0RBRjNNVUJDWUFZNVFNSUw2NU5RTTZYUUciLCJuYW1lIjoiU3luYWRpYSBDb21tdW5pY2F0aW9ucyBJbmMuIiwibmJmIjoxNTQzNzYxMjc1LCJzdWIiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInR5cGUiOiJvcGVyYXRvciIsIm5hdHMiOnsic2lnbmluZ19rZXlzIjpbIk9EU0tSN01ZRlFaNU1NQUo2RlBNRUVUQ1RFM1JJSE9GTFRZUEpSTUFWVk40T0xWMllZQU1IQ0FDIiwiT0RTS0FDU1JCV1A1MzdEWkRSVko2NTdKT0lHT1BPUTZLRzdUNEhONk9LNEY2SUVDR1hEQUhOUDIiLCJPRFNLSTM2TFpCNDRPWTVJVkNSNlA1MkZaSlpZTVlXWlZXTlVEVExFWjVUSzJQTjNPRU1SVEFCUiJdfX0.hyfz6E39BMUh0GLzovFfk3wT4OfualftjdJ_eYkLfPvu5tZubYQ_Pn9oFYGCV_6yKy3KMGhWGUCyCdHaPhalBw",
    resolver: "MEMORY",
    "resolver_preload": {
      ACZSWBJ4SYILK7QVDELO64VX3EFWB6CXCPMEBUKA36MJJQRPXGEEQ2WJ:
        "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJXVFdYVDNCT1JWSFNLQkc2T0pIVVdFQ01QRVdBNldZVEhNRzVEWkJBUUo1TUtGU1dHM1FRIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInN1YiI6IkFDWlNXQko0U1lJTEs3UVZERUxPNjRWWDNFRldCNkNYQ1BNRUJVS0EzNk1KSlFSUFhHRUVRMldKIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiY29ubiI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ3aWxkY2FyZHMiOnRydWV9fX0.q-E7bBGTU0uoTmM9Vn7WaEHDzCUrqvPDb9mPMQbry_PNzVAjf0RG9vd15lGxW5lu7CuGVqpj4CYKhNDHluIJAg",
    },
  };
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    {
      port: ns.port,
      authenticator: credsAuthenticator(new TextEncoder().encode(creds)),
    },
  );
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - custom", async () => {
  const jwt =
    "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJFU1VQS1NSNFhGR0pLN0FHUk5ZRjc0STVQNTZHMkFGWERYQ01CUUdHSklKUEVNUVhMSDJBIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJBQ1pTV0JKNFNZSUxLN1FWREVMTzY0VlgzRUZXQjZDWENQTUVCVUtBMzZNSkpRUlBYR0VFUTJXSiIsInN1YiI6IlVBSDQyVUc2UFY1NTJQNVNXTFdUQlAzSDNTNUJIQVZDTzJJRUtFWFVBTkpYUjc1SjYzUlE1V002IiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ.kCR9Erm9zzux4G6M-V2bp7wKMKgnSNqMBACX05nwePRWQa37aO_yObbhcJWFGYjo1Ix-oepOkoyVLxOJeuD8Bw";
  const useed = "SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4";

  const conf = {
    operator:
      "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJhdWQiOiJURVNUUyIsImV4cCI6MTg1OTEyMTI3NSwianRpIjoiWE5MWjZYWVBIVE1ESlFSTlFPSFVPSlFHV0NVN01JNVc1SlhDWk5YQllVS0VRVzY3STI1USIsImlhdCI6MTU0Mzc2MTI3NSwiaXNzIjoiT0NBVDMzTVRWVTJWVU9JTUdOR1VOWEo2NkFIMlJMU0RBRjNNVUJDWUFZNVFNSUw2NU5RTTZYUUciLCJuYW1lIjoiU3luYWRpYSBDb21tdW5pY2F0aW9ucyBJbmMuIiwibmJmIjoxNTQzNzYxMjc1LCJzdWIiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInR5cGUiOiJvcGVyYXRvciIsIm5hdHMiOnsic2lnbmluZ19rZXlzIjpbIk9EU0tSN01ZRlFaNU1NQUo2RlBNRUVUQ1RFM1JJSE9GTFRZUEpSTUFWVk40T0xWMllZQU1IQ0FDIiwiT0RTS0FDU1JCV1A1MzdEWkRSVko2NTdKT0lHT1BPUTZLRzdUNEhONk9LNEY2SUVDR1hEQUhOUDIiLCJPRFNLSTM2TFpCNDRPWTVJVkNSNlA1MkZaSlpZTVlXWlZXTlVEVExFWjVUSzJQTjNPRU1SVEFCUiJdfX0.hyfz6E39BMUh0GLzovFfk3wT4OfualftjdJ_eYkLfPvu5tZubYQ_Pn9oFYGCV_6yKy3KMGhWGUCyCdHaPhalBw",
    resolver: "MEMORY",
    "resolver_preload": {
      ACZSWBJ4SYILK7QVDELO64VX3EFWB6CXCPMEBUKA36MJJQRPXGEEQ2WJ:
        "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJXVFdYVDNCT1JWSFNLQkc2T0pIVVdFQ01QRVdBNldZVEhNRzVEWkJBUUo1TUtGU1dHM1FRIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInN1YiI6IkFDWlNXQko0U1lJTEs3UVZERUxPNjRWWDNFRldCNkNYQ1BNRUJVS0EzNk1KSlFSUFhHRUVRMldKIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiY29ubiI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ3aWxkY2FyZHMiOnRydWV9fX0.q-E7bBGTU0uoTmM9Vn7WaEHDzCUrqvPDb9mPMQbry_PNzVAjf0RG9vd15lGxW5lu7CuGVqpj4CYKhNDHluIJAg",
    },
  };
  const ns = await NatsServer.start(conf);
  const authenticator = (nonce?: string) => {
    const seed = nkeys.fromSeed(new TextEncoder().encode(useed));
    const nkey = seed.getPublicKey();
    const hash = seed.sign(new TextEncoder().encode(nonce));
    const sig = nkeys.encode(hash);

    return { nkey, sig, jwt };
  };
  const nc = await connect(
    {
      port: ns.port,
      authenticator: authenticator,
    },
  );
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - jwt", async () => {
  const jwt =
    "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJFU1VQS1NSNFhGR0pLN0FHUk5ZRjc0STVQNTZHMkFGWERYQ01CUUdHSklKUEVNUVhMSDJBIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJBQ1pTV0JKNFNZSUxLN1FWREVMTzY0VlgzRUZXQjZDWENQTUVCVUtBMzZNSkpRUlBYR0VFUTJXSiIsInN1YiI6IlVBSDQyVUc2UFY1NTJQNVNXTFdUQlAzSDNTNUJIQVZDTzJJRUtFWFVBTkpYUjc1SjYzUlE1V002IiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ.kCR9Erm9zzux4G6M-V2bp7wKMKgnSNqMBACX05nwePRWQa37aO_yObbhcJWFGYjo1Ix-oepOkoyVLxOJeuD8Bw";
  const useed = "SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4";

  const conf = {
    operator:
      "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJhdWQiOiJURVNUUyIsImV4cCI6MTg1OTEyMTI3NSwianRpIjoiWE5MWjZYWVBIVE1ESlFSTlFPSFVPSlFHV0NVN01JNVc1SlhDWk5YQllVS0VRVzY3STI1USIsImlhdCI6MTU0Mzc2MTI3NSwiaXNzIjoiT0NBVDMzTVRWVTJWVU9JTUdOR1VOWEo2NkFIMlJMU0RBRjNNVUJDWUFZNVFNSUw2NU5RTTZYUUciLCJuYW1lIjoiU3luYWRpYSBDb21tdW5pY2F0aW9ucyBJbmMuIiwibmJmIjoxNTQzNzYxMjc1LCJzdWIiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInR5cGUiOiJvcGVyYXRvciIsIm5hdHMiOnsic2lnbmluZ19rZXlzIjpbIk9EU0tSN01ZRlFaNU1NQUo2RlBNRUVUQ1RFM1JJSE9GTFRZUEpSTUFWVk40T0xWMllZQU1IQ0FDIiwiT0RTS0FDU1JCV1A1MzdEWkRSVko2NTdKT0lHT1BPUTZLRzdUNEhONk9LNEY2SUVDR1hEQUhOUDIiLCJPRFNLSTM2TFpCNDRPWTVJVkNSNlA1MkZaSlpZTVlXWlZXTlVEVExFWjVUSzJQTjNPRU1SVEFCUiJdfX0.hyfz6E39BMUh0GLzovFfk3wT4OfualftjdJ_eYkLfPvu5tZubYQ_Pn9oFYGCV_6yKy3KMGhWGUCyCdHaPhalBw",
    resolver: "MEMORY",
    "resolver_preload": {
      ACZSWBJ4SYILK7QVDELO64VX3EFWB6CXCPMEBUKA36MJJQRPXGEEQ2WJ:
        "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJXVFdYVDNCT1JWSFNLQkc2T0pIVVdFQ01QRVdBNldZVEhNRzVEWkJBUUo1TUtGU1dHM1FRIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInN1YiI6IkFDWlNXQko0U1lJTEs3UVZERUxPNjRWWDNFRldCNkNYQ1BNRUJVS0EzNk1KSlFSUFhHRUVRMldKIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiY29ubiI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ3aWxkY2FyZHMiOnRydWV9fX0.q-E7bBGTU0uoTmM9Vn7WaEHDzCUrqvPDb9mPMQbry_PNzVAjf0RG9vd15lGxW5lu7CuGVqpj4CYKhNDHluIJAg",
    },
  };
  const ns = await NatsServer.start(conf);
  let nc = await connect(
    {
      port: ns.port,
      authenticator: jwtAuthenticator(jwt, new TextEncoder().encode(useed)),
    },
  );
  await nc.flush();
  await nc.close();

  nc = await connect(
    {
      port: ns.port,
      authenticator: jwtAuthenticator((): string => {
        return jwt;
      }, new TextEncoder().encode(useed)),
    },
  );
  await nc.flush();
  await nc.close();

  await ns.stop();
});

Deno.test("auth - custom error", async () => {
  const ns = await NatsServer.start(conf);
  const authenticator = () => {
    throw new Error("user code exploded");
  };
  await connect(
    {
      port: ns.port,
      maxReconnectAttempts: 1,
      authenticator: authenticator,
    },
  ).then(() => {
    fail("shouldn't have connected");
  }).catch((err) => {
    assertErrorCode(err, ErrorCode.BadAuthentication);
  });
  await ns.stop();
});

Deno.test("basics - bad auth", async () => {
  try {
    await connect(
      {
        servers: "connect.ngs.global",
        waitOnFirstConnect: true,
        user: "me",
        pass: "you",
      },
    );
  } catch (err) {
    assertErrorCode(err, ErrorCode.AuthorizationViolation);
  }
});

Deno.test("auth - nkey authentication", async () => {
  const ukp = nkeys.createUser();
  const conf = {
    authorization: {
      users: [{
        nkey: ukp.getPublicKey(),
      }],
    },
  };

  // static
  const ns = await NatsServer.start(conf);
  let nc = await connect({
    port: ns.port,
    authenticator: nkeyAuthenticator(ukp.getSeed()),
  });
  await nc.flush();
  await nc.close();

  // from function
  nc = await connect({
    port: ns.port,
    authenticator: nkeyAuthenticator((): Uint8Array => {
      return ukp.getSeed();
    }),
  });
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("auth - creds authenticator validation", () => {
  const jwt =
    `eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJFU1VQS1NSNFhGR0pLN0FHUk5ZRjc0STVQNTZHMkFGWERYQ01CUUdHSklKUEVNUVhMSDJBIiwiaWF0IjoxNTQ0MjE3NzU3LCJpc3MiOiJBQ1pTV0JKNFNZSUxLN1FWREVMTzY0VlgzRUZXQjZDWENQTUVCVUtBMzZNSkpRUlBYR0VFUTJXSiIsInN1YiI6IlVBSDQyVUc2UFY1NTJQNVNXTFdUQlAzSDNTNUJIQVZDTzJJRUtFWFVBTkpYUjc1SjYzUlE1V002IiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ.kCR9Erm9zzux4G6M-V2bp7wKMKgnSNqMBACX05nwePRWQa37aO_yObbhcJWFGYjo1Ix-oepOkoyVLxOJeuD8Bw`;
  const ukp = nkeys.createUser();
  const upk = ukp.getPublicKey();
  const sc = StringCodec();
  const seed = sc.decode(ukp.getSeed());

  function creds(ajwt = "", aseed = ""): string {
    return `-----BEGIN NATS USER JWT-----
    ${ajwt}
  ------END NATS USER JWT------

************************* IMPORTANT *************************
  NKEY Seed printed below can be used sign and prove identity.
    NKEYs are sensitive and should be treated as secrets.

  -----BEGIN USER NKEY SEED-----
    ${aseed}
  ------END USER NKEY SEED------
 `;
  }

  type test = [string, string, boolean, string];
  const tests: test[] = [];
  tests.push(["", "", false, "no jwt, no seed"]);
  tests.push([jwt, "", false, "no seed"]);
  tests.push(["", seed, false, "no jwt"]);
  tests.push([jwt, seed, true, "jwt and seed"]);

  tests.forEach((v) => {
    const d = sc.encode(creds(v[0], v[1]));
    try {
      const auth = credsAuthenticator(d);
      if (!v[2]) {
        fail(`should have failed: ${v[3]}`);
      }
      const { nkey, sig } = auth("helloworld") as unknown as NKeyAuth;
      assertEquals(nkey, upk);
      assert(sig.length > 0);
    } catch (_err) {
      if (v[2]) {
        fail(`should have passed: ${v[3]}`);
      }
    }
  });
});

Deno.test("auth - expiration is notified", async () => {
  const O = nkeys.createOperator();
  const A = nkeys.createAccount();

  const resolver: Record<string, string> = {};
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

  const ns = await NatsServer.start(conf);

  const U = nkeys.createUser();
  const ujwt = await encodeUser("U", U, A, { bearer_token: true }, {
    exp: Math.round(Date.now() / 1000) + 3,
  });

  const nc = await connect({
    port: ns.port,
    maxReconnectAttempts: -1,
    authenticator: jwtAuthenticator(ujwt),
  });

  let authErrors = 0;
  (async () => {
    for await (const s of nc.status()) {
      if (
        s.type === Events.Error && s.data === ErrorCode.AuthenticationExpired
      ) {
        authErrors++;
      }
    }
  })().then();

  const err = await nc.closed();
  assert(authErrors >= 1);
  assertErrorCode(err!, ErrorCode.AuthenticationExpired);
  await cleanup(ns);
});

Deno.test("auth - bad auth is notified", async () => {
  let ns = await NatsServer.start(conf);

  let count = 0;

  // authenticator that works once
  const authenticator = (): UserPass => {
    const pass = count === 0 ? "foobar" : "bad";
    count++;
    return { user: "derek", pass };
  };

  const nc = await connect(
    { port: ns.port, authenticator },
  );
  let badAuths = 0;
  (async () => {
    for await (const s of nc.status()) {
      if (
        s.type === Events.Error && s.data === ErrorCode.AuthorizationViolation
      ) {
        badAuths++;
      }
    }
  })().then();

  await ns.stop();
  ns = await ns.restart();

  const err = await nc.closed();
  assert(badAuths > 1);
  assertErrorCode(err!, ErrorCode.AuthorizationViolation);

  await ns.stop();
});

Deno.test("auth - perm request error", async () => {
  const ns = await NatsServer.start({
    authorization: {
      users: [{
        user: "a",
        password: "b",
        permission: {
          publish: "r",
        },
      }, {
        user: "s",
        password: "s",
        permission: {
          subscribe: "q",
        },
      }],
    },
  });

  const [nc, sc] = await Promise.all([
    connect(
      { port: ns.port, user: "a", pass: "b" },
    ),
    connect(
      { port: ns.port, user: "s", pass: "s" },
    ),
  ]);

  sc.subscribe("q", {
    callback: (err, msg) => {
      if (err) {
        return;
      }
      msg.respond();
    },
  });

  const status = deferred<Status>();
  (async () => {
    for await (const s of nc.status()) {
      if (
        s.permissionContext?.operation === "publish" &&
        s.permissionContext?.subject === "q"
      ) {
        status.resolve(s);
      }
    }
  })().then();

  const response = deferred<Error>();
  nc.request("q")
    .catch((err) => {
      response.resolve(err);
    });

  const [r, s] = await Promise.all([response, status]);
  assertErrorCode(r, ErrorCode.PermissionsViolation);
  const ne = r as NatsError;
  assertEquals(ne.permissionContext?.operation, "publish");
  assertEquals(ne.permissionContext?.subject, "q");

  assertEquals(s.type, Events.Error);
  assertEquals(s.data, ErrorCode.PermissionsViolation);
  assertEquals(s.permissionContext?.operation, "publish");
  assertEquals(s.permissionContext?.subject, "q");

  await cleanup(ns, nc, sc);
});

Deno.test("auth - perm request error no mux", async () => {
  const ns = await NatsServer.start({
    authorization: {
      users: [{
        user: "a",
        password: "b",
        permission: {
          publish: "r",
        },
      }, {
        user: "s",
        password: "s",
        permission: {
          subscribe: "q",
        },
      }],
    },
  });

  const [nc, sc] = await Promise.all([
    connect(
      { port: ns.port, user: "a", pass: "b" },
    ),
    connect(
      { port: ns.port, user: "s", pass: "s" },
    ),
  ]);

  sc.subscribe("q", {
    callback: (err, msg) => {
      if (err) {
        return;
      }
      msg.respond();
    },
  });

  const status = deferred<Status>();
  (async () => {
    for await (const s of nc.status()) {
      if (
        s.permissionContext?.operation === "publish" &&
        s.permissionContext?.subject === "q"
      ) {
        status.resolve(s);
      }
    }
  })().then();

  const response = deferred<Error>();
  nc.request("q", Empty, { noMux: true, timeout: 1000 })
    .catch((err) => {
      response.resolve(err);
    });

  const [r, s] = await Promise.all([response, status]);
  assertErrorCode(r, ErrorCode.PermissionsViolation);
  const ne = r as NatsError;
  assertEquals(ne.permissionContext?.operation, "publish");
  assertEquals(ne.permissionContext?.subject, "q");

  assertEquals(s.type, Events.Error);
  assertEquals(s.data, ErrorCode.PermissionsViolation);
  assertEquals(s.permissionContext?.operation, "publish");
  assertEquals(s.permissionContext?.subject, "q");

  await cleanup(ns, nc, sc);
});

Deno.test("auth - perm request error deliver to sub", async () => {
  const ns = await NatsServer.start({
    authorization: {
      users: [{
        user: "a",
        password: "b",
        permission: {
          publish: "r",
        },
      }, {
        user: "s",
        password: "s",
        permission: {
          subscribe: "q",
        },
      }],
    },
  });

  const [nc, sc] = await Promise.all([
    connect(
      { port: ns.port, user: "a", pass: "b" },
    ),
    connect(
      { port: ns.port, user: "s", pass: "s" },
    ),
  ]);

  sc.subscribe("q", {
    callback: (err, msg) => {
      if (err) {
        return;
      }
      msg.respond();
    },
  });

  const status = deferred<Status>();
  (async () => {
    for await (const s of nc.status()) {
      if (
        s.permissionContext?.operation === "publish" &&
        s.permissionContext?.subject === "q"
      ) {
        status.resolve(s);
      }
    }
  })().then();

  const inbox = createInbox();
  const sub = nc.subscribe(inbox, {
    callback: () => {
    },
  });

  const response = deferred<Error>();
  nc.request("q", Empty, { noMux: true, reply: inbox, timeout: 1000 })
    .catch((err) => {
      response.resolve(err);
    });

  const [r, s] = await Promise.all([response, status]);
  assertErrorCode(r, ErrorCode.PermissionsViolation);
  const ne = r as NatsError;
  assertEquals(ne.permissionContext?.operation, "publish");
  assertEquals(ne.permissionContext?.subject, "q");

  assertEquals(s.type, Events.Error);
  assertEquals(s.data, ErrorCode.PermissionsViolation);
  assertEquals(s.permissionContext?.operation, "publish");
  assertEquals(s.permissionContext?.subject, "q");

  assertEquals(sub.isClosed(), false);

  await cleanup(ns, nc, sc);
});

Deno.test("auth - mux sub ok", async () => {
  const conf = {
    authorization: {
      users: [{
        user: "a",
        password: "b",
        permission: {
          subscribe: "r",
        },
      }, {
        user: "s",
        password: "s",
        permission: {
          subscribe: "q",
        },
      }],
    },
  };
  let ns = await NatsServer.start(conf);

  const [nc, sc] = await Promise.all([
    connect(
      { port: ns.port, user: "a", pass: "b", maxReconnectAttempts: -1 },
    ),
    connect(
      { port: ns.port, user: "s", pass: "s", maxReconnectAttempts: -1 },
    ),
  ]);

  sc.subscribe("q", {
    callback: (_err, msg) => {
      msg.respond();
    },
  });

  const response = deferred<NatsError>();
  nc.request("q")
    .catch((err) => {
      response.resolve(err);
    });
  const ne = await response as NatsError;
  assertEquals(ne.permissionContext?.operation, "subscription");
  //@ts-ignore: test
  assertEquals(nc.protocol.subscriptions.getMux(), null);

  function reconnected(nc: NatsConnection): Promise<void> {
    const v = deferred<void>();
    (async () => {
      for await (const s of nc.status()) {
        if (s.type === Events.Reconnect) {
          v.resolve();
          break;
        }
      }
    })().then();
    return v;
  }

  // restart the server with new permissions, client should be able to request
  const port = ns.port;
  await ns.stop();
  const proms = Promise.all([reconnected(nc), reconnected(sc)]);

  ns = await NatsServer.start({
    port: port,
    authorization: {
      users: [{
        user: "a",
        password: "b",
      }, {
        user: "s",
        password: "s",
        permission: {
          subscribe: "q",
        },
      }],
    },
  });

  await proms;
  await nc.request("q");
  await cleanup(ns, nc, sc);
});

Deno.test("auth - perm sub iterator error", async () => {
  const ns = await NatsServer.start({
    authorization: {
      users: [{
        user: "a",
        password: "b",
        permission: {
          subscribe: "s",
        },
      }],
    },
  });

  const nc = await connect({ port: ns.port, user: "a", pass: "b" });

  const status = deferred<Status>();
  (async () => {
    for await (const s of nc.status()) {
      if (
        s.permissionContext?.operation === "subscription" &&
        s.permissionContext?.subject === "q"
      ) {
        status.resolve(s);
      }
    }
  })().then();

  const sub = nc.subscribe("q");
  const iterReject = deferred<NatsError>();
  (async () => {
    for await (const _m of sub) {
      // ignored
    }
  })().catch((err) => {
    iterReject.resolve(err as NatsError);
  });

  const [s, i] = await Promise.all([status, iterReject]);
  assertEquals(s.type, Events.Error);
  assertEquals(s.data, ErrorCode.PermissionsViolation);
  assertEquals(s.permissionContext?.operation, "subscription");
  assertEquals(s.permissionContext?.subject, "q");

  assertEquals(i.code, ErrorCode.PermissionsViolation);
  assertEquals(i.permissionContext?.operation, "subscription");
  assertEquals(i.permissionContext?.subject, "q");

  await cleanup(ns, nc);
});

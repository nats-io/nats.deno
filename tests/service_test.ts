/*
 * Copyright 2022-2023 The NATS Authors
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
import { cleanup, setup } from "./jstest_util.ts";
import {
  Service,
  ServiceConfig,
  ServiceError,
  ServiceErrorCodeHeader,
  ServiceErrorHeader,
  ServiceIdentity,
  ServiceImpl,
  ServiceInfo,
  ServiceResponseType,
  ServiceSchema,
  ServiceStats,
} from "../nats-base-client/service.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
  assertExists,
  assertRejects,
  assertThrows,
  fail,
} from "https://deno.land/std@0.168.0/testing/asserts.ts";

import { collect, delay } from "../nats-base-client/util.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";
import {
  connect,
  createInbox,
  ErrorCode,
  JSONCodec,
  Msg,
  NatsError,
  nuid,
  QueuedIterator,
  ServiceVerb,
  StringCodec,
} from "../src/mod.ts";

Deno.test("service - control subject", () => {
  const test = (verb: ServiceVerb) => {
    assertEquals(ServiceImpl.controlSubject(verb), `$SRV.${verb}`);
    assertEquals(ServiceImpl.controlSubject(verb, "NamE"), `$SRV.${verb}.NamE`);
    assertEquals(
      ServiceImpl.controlSubject(verb, "nAmE", "Id"),
      `$SRV.${verb}.nAmE.Id`,
    );
    assertEquals(
      ServiceImpl.controlSubject(verb, "nAMe", "iD", "hello.service"),
      `hello.service.${verb}.nAMe.iD`,
    );
  };
  [ServiceVerb.INFO, ServiceVerb.PING, ServiceVerb.SCHEMA, ServiceVerb.STATS]
    .forEach((v) => {
      test(v);
    });
});

Deno.test("service - bad name", async () => {
  const { ns, nc } = await setup({}, {});
  const t = async (name: string, msg: string) => {
    await assertRejects(
      async () => {
        await nc.services.add({
          name: name,
          version: "1.0.0",
          schema: {
            request: "a",
            response: "b",
          },
        });
      },
      Error,
      msg,
    );
  };

  await t("/", "name cannot contain '/'");
  await t(" ", "name cannot contain ' '");
  await t(">", "name cannot contain '>'");
  await t("", "name required");
  await cleanup(ns, nc);
});

Deno.test("service - client", async () => {
  const { ns, nc } = await setup({}, {});
  const sc = StringCodec();
  const subj = createInbox();
  const srv = await nc.services.add({
    name: "test",
    version: "1.0.0",
    description: "responds with hello",
    schema: {
      request: "a",
      response: "b",
    },
    endpoint: {
      subject: subj,
      handler: (_err, msg) => {
        msg?.respond(sc.encode("hello"));
      },
    },
  }) as ServiceImpl;

  await nc.request(subj);
  await nc.request(subj);

  const m = nc.services.client();

  function verifyIdentity(ids: ServiceIdentity[]) {
    assertEquals(ids.length, 1);
    const e = ids[0];
    assertEquals(e.id, srv.id);
    assertEquals(e.name, srv.name);
    assertEquals(e.version, srv.version);
  }

  function verifyPing(pings: ServiceIdentity[]) {
    verifyIdentity(pings);
    const ping = pings[0];
    assertEquals(ping.type, ServiceResponseType.PING);
    const r = ping as unknown as Record<string, unknown>;
    delete r.version;
    delete r.name;
    delete r.id;
    delete r.type;
    assertEquals(Object.keys(r).length, 0, JSON.stringify(r));
  }
  verifyPing(await collect(await m.ping()));
  verifyPing(await collect(await m.ping("test")));
  verifyPing(await collect(await m.ping("test", srv.id)));

  function verifyInfo(infos: ServiceInfo[]) {
    verifyIdentity(infos);
    const info = infos[0];
    assertEquals(info.type, ServiceResponseType.INFO);
    assertEquals(info.description, srv.description);
    assertEquals(info.subjects.length, srv.subjects.length);
    assertArrayIncludes(info.subjects, srv.subjects);
    const r = info as unknown as Record<string, unknown>;
    delete r.type;
    delete r.version;
    delete r.name;
    delete r.id;
    delete r.description;
    delete r.subjects;
    assertEquals(Object.keys(r).length, 0, JSON.stringify(r));
  }

  // info
  verifyInfo(await collect(await m.info()));
  verifyInfo(await collect(await m.info("test")));
  verifyInfo(await collect(await m.info("test", srv.id)));

  function verifyStats(stats: ServiceStats[]) {
    verifyIdentity(stats);
    const stat = stats[0];
    assertEquals(stat.type, ServiceResponseType.STATS);
    assert(Date.parse(stat.started) > 0);
    const s = stat.endpoints?.[0]!;
    assertEquals(s.num_requests, 2);
    assertEquals(s.num_errors, 0);
    assertEquals(typeof s.processing_time, "number");
    assertEquals(typeof s.average_processing_time, "number");

    // assert(Date.parse(stat.started) - Date.now() > 0, JSON.stringify(stat));

    const r = stat as unknown as Record<string, unknown>;
    delete r.type;
    delete r.version;
    delete r.name;
    delete r.id;
    delete r.started;
    delete r.endpoints;
    assertEquals(Object.keys(r).length, 0, JSON.stringify(r));
  }

  verifyStats(await collect(await m.stats()));
  verifyStats(await collect(await m.stats("test")));
  verifyStats(await collect(await m.stats("test", srv.id)));

  function verifySchema(schemas: ServiceSchema[]) {
    verifyIdentity(schemas);
    const schema = schemas[0];
    assertExists(schema.schema);
    assertEquals(schema.type, ServiceResponseType.SCHEMA);
    assertEquals(schema.schema?.request, srv.config.schema?.request);
    assertEquals(schema.schema?.response, srv.config.schema?.response);

    const r = schema as unknown as Record<string, unknown>;
    delete r.type;
    delete r.version;
    delete r.name;
    delete r.id;
    delete r.schema;
    assertEquals(Object.keys(r).length, 0, JSON.stringify(r));
  }

  // schema
  verifySchema(await collect(await m.schema()));
  verifySchema(await collect(await m.schema("test")));
  verifySchema(await collect(await m.schema("test", srv.id)));

  await cleanup(ns, nc);
});

Deno.test("service - basics", async () => {
  const { ns, nc } = await setup({}, {});
  const srvA = await nc.services.add({
    name: "test",
    version: "0.0.0",
    endpoint: {
      subject: "foo",
      handler: (_err: Error | null, msg: Msg) => {
        msg?.respond();
      },
    },
  }) as ServiceImpl;

  const srvB = await nc.services.add({
    name: "test",
    version: "0.0.0",
    endpoint: {
      subject: "foo",
      handler: (_err: Error | null, msg: Msg) => {
        msg?.respond();
      },
    },
  }) as ServiceImpl;

  const m = nc.services.client();
  const count = async (
    p: Promise<QueuedIterator<unknown>>,
  ): Promise<number> => {
    return (await collect(await p)).length;
  };

  assertEquals(await count(m.ping()), 2);
  assertEquals(await count(m.ping("test")), 2);
  assertEquals(await count(m.ping("test", srvA.id)), 1);
  await assertRejects(
    async () => {
      await collect(await m.ping("test", "c"));
    },
    Error,
    ErrorCode.NoResponders,
  );

  assertEquals(await count(m.info()), 2);
  assertEquals(await count(m.info("test")), 2);
  assertEquals(await count(m.info("test", srvB.id)), 1);
  await assertRejects(
    async () => {
      await collect(await m.info("test", "c"));
    },
    Error,
    ErrorCode.NoResponders,
  );

  assertEquals(await count(m.stats()), 2);
  assertEquals(await count(m.stats("test")), 2);
  assertEquals(await count(m.stats("test", srvB.id)), 1);
  await assertRejects(
    async () => {
      await collect(await m.stats("test", "c"));
    },
    Error,
    ErrorCode.NoResponders,
  );

  assertEquals(await count(m.schema()), 2);
  assertEquals(await count(m.schema("test")), 2);
  assertEquals(await count(m.schema("test", srvB.id)), 1);
  await assertRejects(
    async () => {
      await collect(await m.schema("test", "c"));
    },
    Error,
    ErrorCode.NoResponders,
  );

  await srvA.stop();
  await srvB.stop();

  await cleanup(ns, nc);
});

Deno.test("service - stop error", async () => {
  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
        permissions: {
          subscribe: {
            deny: "fail",
          },
        },
      }],
    },
  }, { user: "a", pass: "a" });

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "fail",
      handler: () => {
        if (err) {
          service.stop(err);
          return;
        }
        fail("shouldn't have subscribed");
      },
    },
  });

  const err = await service.stopped as NatsError;
  assertEquals(
    err.code,
    ErrorCode.PermissionsViolation,
  );

  await cleanup(ns, nc);
});

Deno.test("service - start error", async () => {
  const { ns, nc } = await setup({
    authorization: {
      users: [{
        user: "a",
        password: "a",
        permissions: {
          subscribe: {
            deny: "fail",
          },
        },
      }],
    },
  }, { user: "a", pass: "a" });

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "fail",
      handler: (_err, msg) => {
        msg?.respond();
      },
    },
  });

  const err = await service.stopped as NatsError;
  assertEquals(
    err.code,
    ErrorCode.PermissionsViolation,
  );

  await cleanup(ns, nc);
});

Deno.test("service - callback error", async () => {
  const { ns, nc } = await setup();
  await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "fail",
      handler: (err) => {
        if (err === null) {
          throw new Error("boom");
        }
      },
    },
  });

  const m = await nc.request("fail");
  assertEquals(m.headers?.get(ServiceErrorHeader), "boom");
  assertEquals(m.headers?.get(ServiceErrorCodeHeader), "500");

  await cleanup(ns, nc);
});

Deno.test("service -service error is headers", async () => {
  const { ns, nc } = await setup();
  const srv = await nc.services.add({
    name: "test",
    version: "2.0.0",
  });
  srv.addEndpoint("fail", (): void => {
    // tossing service error should have the code/description
    throw new ServiceError(1210, "something");
  });

  const m = await nc.request("fail");
  assertEquals(m.headers?.get(ServiceErrorHeader), "something");
  assertEquals(m.headers?.get(ServiceErrorCodeHeader), "1210");

  await cleanup(ns, nc);
});

Deno.test("service - sub stop", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
  });
  service.addEndpoint("q", (_err, m) => {
    m.respond();
  });

  const nci = nc as NatsConnectionImpl;
  for (const s of nci.protocol.subscriptions.subs.values()) {
    if (s.subject === "q") {
      s.close();
      break;
    }
  }
  const err = await service.stopped as Error;
  assertEquals(err.message, "required subscription q stopped");

  await cleanup(ns, nc);
});

Deno.test("service - monitoring sub stop", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
  });
  service.addEndpoint("q", (_err, m) => {
    m.respond();
  });

  const nci = nc as NatsConnectionImpl;
  for (const s of nci.protocol.subscriptions.subs.values()) {
    if (s.subject === "$SRV.PING") {
      s.close();
      break;
    }
  }
  const err = await service.stopped as Error;
  assertEquals(err.message, "required subscription $SRV.PING stopped");

  await cleanup(ns, nc);
});

Deno.test("service - custom stats handler", async () => {
  const { ns, nc } = await setup();

  const srv = await nc.services.add({
    name: "test",
    version: "2.0.0",
    statsHandler: (): Promise<unknown> => {
      return Promise.resolve({ hello: "world" });
    },
  });
  srv.addEndpoint("q", (_err, m) => {
    m.respond();
  });

  const m = nc.services.client();
  const stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(
    (stats[0].endpoints?.[0].data as Record<string, unknown>).hello,
    "world",
  );

  await cleanup(ns, nc);
});

Deno.test("service - bad stats handler", async () => {
  const { ns, nc } = await setup();

  const config = {
    name: "test",
    version: "2.0.0",
    // @ts-ignore: test
    statsHandler: "hello world",
  };

  const srv = await nc.services.add(config as unknown as ServiceConfig);
  srv.addEndpoint("q", (_err, m) => {
    m.respond();
  });

  const m = nc.services.client();
  const stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].endpoints?.[0].data, undefined);

  await cleanup(ns, nc);
});

Deno.test("service - stats handler error", async () => {
  const { ns, nc } = await setup();

  const srv = await nc.services.add({
    name: "test",
    version: "2.0.0",
    statsHandler: (): Promise<unknown> => {
      throw new Error("bad stats handler");
    },
  });
  srv.addEndpoint("q", (_err, m) => {
    m.respond();
  });

  const m = nc.services.client();
  const stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].endpoints?.length, 1);
  const s = stats[0].endpoints?.[0]!;
  assertEquals(s.data, undefined);
  assertEquals(s.last_error, "bad stats handler");
  assertEquals(s.num_errors, 1);

  await cleanup(ns, nc);
});

Deno.test("service - reset", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
  }) as ServiceImpl;

  service.addEndpoint("q", (_err, m) => {
    m.respond();
  });

  await nc.request("q");
  await nc.request("q");

  service.handlers[0].stats.countError(new Error("hello"));

  const m = nc.services.client();
  let stats = await collect(await m.stats());
  assertEquals(stats[0].endpoints?.length, 1);
  assertEquals(stats.length, 1);
  let stat = stats[0].endpoints?.[0]!;
  assert(stat.processing_time >= 0);
  assert(stat.average_processing_time >= 0);
  assertEquals(stat.num_errors, 1);
  assertEquals(stat.last_error, "hello");

  service.reset();
  stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].endpoints?.length, 1);
  stat = stats[0].endpoints?.[0]!;
  assertEquals(stat.num_requests, 0);
  assertEquals(stat.processing_time, 0);
  assertEquals(stat.average_processing_time, 0);
  assertEquals(stat.num_errors, 0);
  assertEquals(stat.last_error, undefined);

  await cleanup(ns, nc);
});

Deno.test("service - iter", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
  }) as ServiceImpl;
  const iter = service.addEndpoint("q");
  (async () => {
    for await (const m of iter) {
      await delay(500);
      m.respond();
    }
  })().then();

  await nc.request("q");
  await nc.request("q");
  service.handlers[0].stats.countError(new Error("hello"));

  const m = nc.services.client();
  let stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].endpoints?.length, 1);
  const stat = stats[0].endpoints?.[0]!;
  assert(stat.processing_time >= 0);
  assert(stat.average_processing_time >= 0);
  assertEquals(stat.num_errors, 1);
  assertEquals(stat.last_error, "hello");

  service.reset();
  stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  await cleanup(ns, nc);
});

Deno.test("service - iter closed", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
  });
  const iter = service.addEndpoint("q");
  (async () => {
    for await (const m of iter) {
      m.respond();
      break;
    }
  })().then();

  await nc.request("q");
  const err = await service.stopped;
  assertEquals(err, null);

  await cleanup(ns, nc);
});

Deno.test("service - version must be semver", async () => {
  const { ns, nc } = await setup();
  const test = (v?: string): Promise<Service> => {
    return nc.services.add({
      name: "test",
      version: v!,
    });
  };

  await assertRejects(
    async () => {
      await test();
    },
    Error,
    "'' is not a semver value",
  );

  await assertRejects(
    async () => {
      await test("a.b.c");
    },
    Error,
    "'a.b.c' is not a semver value",
  );

  const srv = await test("v1.2.3-hello") as ServiceImpl;
  const info = srv.info();
  assertEquals(info.id, srv.id);
  assertEquals(info.name, srv.name);
  assertEquals(info.version, "v1.2.3-hello");
  assertEquals(info.description, srv.description);
  assertEquals(info.subjects.length, 0);

  await cleanup(ns, nc);
});

Deno.test("service - service errors", async () => {
  const { ns, nc } = await setup();
  const srv = await nc.services.add({
    name: "test",
    version: "2.0.0",
  });

  const iter = srv.addEndpoint("q");
  (async () => {
    for await (const m of iter) {
      m.data.length ? m.respond() : m.respondError(411, "data required");
    }
  })().then();

  let r = await nc.request("q");
  assertEquals(ServiceError.isServiceError(r), true);
  const serr = ServiceError.toServiceError(r);
  assertEquals(serr?.code, 411);
  assertEquals(serr?.message, "data required");

  r = await nc.request("q", new Uint8Array(1));
  assertEquals(ServiceError.isServiceError(r), false);
  assertEquals(ServiceError.toServiceError(r), null);
  await cleanup(ns, nc);
});

Deno.test("service - cross platform service test", async () => {
  const nc = await connect({ servers: "demo.nats.io" });
  const name = `echo_${nuid.next()}`;
  await nc.services.add({
    name,
    version: "0.0.1",
    schema: { request: "a", response: "b" },
    statsHandler: (): Promise<unknown> => {
      return Promise.resolve("hello world");
    },
    endpoint: {
      subject: createInbox(),
      handler: (_err, m): void => {
        if (m.data.length === 0) {
          m.respondError(400, "need a string", JSONCodec().encode(""));
        } else {
          if (StringCodec().decode(m.data) === "error") {
            throw new Error("service asked to throw an error");
          }
          m.respond(m.data);
        }
      },
    },
  }) as ServiceImpl;

  const args = [
    "deno",
    "run",
    "-A",
    "./tests/helpers/service-check.ts",
    "--name",
    name,
    "--server",
    "demo.nats.io",
  ];

  const p = Deno.run({ cmd: args, stderr: "piped", stdout: "piped" });
  const [status, _stdout, stderr] = await Promise.all([
    p.status(),
    p.stderrOutput(),
  ]);

  if (!status.success) {
    fail(StringCodec().decode(stderr));
  }
  p.close();

  await nc.close();
});

Deno.test("service - stats name respects assigned name", async () => {
  const { ns, nc } = await setup();
  const test = await nc.services.add({
    name: "tEsT",
    // @ts-ignore: testing
    version: "0.0.1",
    endpoint: {
      subject: "q",
      handler: (_err, msg) => {
        msg?.respond();
      },
    },
  });
  const stats = await test.stats();
  assertEquals(stats.name, "tEsT");
  const r = await nc.request(`$SRV.PING.tEsT`);
  const si = JSONCodec<ServiceIdentity>().decode(r.data);
  assertEquals(si.name, "tEsT");

  await cleanup(ns, nc);
});

Deno.test("service - multiple endpoints", async () => {
  const { ns, nc } = await setup();
  const ms = await nc.services.add({
    name: "multi",
    version: "0.0.1",
  });
  const sc = StringCodec();
  ms.addEndpoint("hey", (_err, m) => {
    m.respond(sc.encode("hi"));
  });
  ms.addGroup("service").addEndpoint("echo", (_err, m) => {
    m.respond(m.data);
  });

  let r = await nc.request(`hey`);
  assertEquals(sc.decode(r.data), "hi");
  r = await nc.request(`service.echo`, sc.encode("yo!"));
  assertEquals(sc.decode(r.data), "yo!");

  r = await nc.request(`$SRV.STATS`);
  const stats = JSONCodec().decode(r.data) as ServiceStats;

  function t(name: string) {
    const v = stats.endpoints?.find((n) => {
      return n.name === name;
    });
    assertExists(v);
    assertEquals(v.num_requests, 1);
  }
  t("hey");
  t("echo");

  await cleanup(ns, nc);
});

Deno.test("service - multi cb/iterator", async () => {
  const { ns, nc } = await setup();
  const srv = await nc.services.add({
    name: "example",
    version: "0.0.1",
  });
  srv.addGroup("cb").addGroup("b").addGroup("c").addEndpoint(
    "test",
    (_err, msg) => {
      msg?.respond();
    },
  );
  await nc.request("cb.b.c.test");

  const iter = srv.addGroup("iter.b.c").addEndpoint("test");
  (async () => {
    for await (const m of iter) {
      m.respond();
    }
  })().then();
  await nc.request("iter.b.c.test");

  await cleanup(ns, nc);
});

Deno.test("service - group and endpoint names", async () => {
  const { ns, nc } = await setup();
  const srv = await nc.services.add({
    name: "example",
    version: "0.0.1",
  });

  const t = (group: string, endpoint: string, expect: string) => {
    assertThrows(
      () => {
        srv.addGroup(group).addEndpoint(endpoint);
      },
      Error,
      expect,
    );
  };
  t("", "", "endpoint name required");
  t("", "*", "endpoint name cannot contain '*'");
  t("", ">", "endpoint name cannot contain '>'");
  t("", " ", "endpoint name cannot contain ' '");
  t("", "hello.world", "endpoint name cannot contain '.'");
  t("a.>", "hello", "service group name cannot contain internal '>'");
  t(">", "hello", "service group name cannot contain internal '>'");
  await cleanup(ns, nc);
});

Deno.test("service - group subs", async () => {
  const { ns, nc } = await setup();
  const srv = await nc.services.add({
    name: "example",
    version: "0.0.1",
    endpoint: {
      subject: "default.>",
    },
  });
  const t = (subject: string) => {
    const sub = (nc as NatsConnectionImpl).protocol.subscriptions.all().find(
      (s) => {
        return s.subject === subject;
      },
    );
    assertExists(sub);
  };
  t("default.>");
  srv.addGroup("").addEndpoint("root");
  t("root");
  srv.addGroup("a").addEndpoint("add");
  t("a.add");
  srv.addGroup("b").addEndpoint("add");
  t("b.add");
  srv.addGroup("one.*.three").addEndpoint("add");
  t("one.*.three.add");
  srv.addGroup("$SYS.SOMETHING.OR.OTHER").addEndpoint({
    name: "wild",
    subject: "*",
  });
  t("$SYS.SOMETHING.OR.OTHER.*");
  await cleanup(ns, nc);
});

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
  ServiceSchema,
  ServiceStats,
} from "../nats-base-client/service.ts";
import {
  assert,
  assertEquals,
  assertExists,
  assertRejects,
  fail,
} from "https://deno.land/std@0.125.0/testing/asserts.ts";
import {
  createInbox,
  ErrorCode,
  Msg,
  NatsError,
  QueuedIterator,
  ServiceVerb,
  StringCodec,
} from "../nats-base-client/mod.ts";
import { collect, delay } from "../nats-base-client/util.ts";
import { NatsConnectionImpl } from "../nats-base-client/nats.ts";

Deno.test("service - control subject", () => {
  const test = (verb: ServiceVerb) => {
    assertEquals(ServiceImpl.controlSubject(verb), `$SRV.${verb}`);
    assertEquals(ServiceImpl.controlSubject(verb, "name"), `$SRV.${verb}.NAME`);
    assertEquals(
      ServiceImpl.controlSubject(verb, "name", "id"),
      `$SRV.${verb}.NAME.ID`,
    );
    assertEquals(
      ServiceImpl.controlSubject(verb, "name", "id", "hello.service"),
      `hello.service.${verb}.NAME.ID`,
    );
  };
  [ServiceVerb.INFO, ServiceVerb.PING, ServiceVerb.SCHEMA, ServiceVerb.STATS]
    .forEach((v) => {
      test(v);
    });
});

Deno.test("service - bad name", async () => {
  const { ns, nc } = await setup({}, {});
  const subj = createInbox();
  await assertRejects(
    async () => {
      const _s = await nc.services.add({
        name: "/hello.world",
        version: "1.0.0",
        schema: {
          request: "a",
          response: "b",
        },
        endpoint: {
          subject: subj,
          handler: (_err: Error | null, msg: Msg): Promise<void> => {
            msg?.respond();
            return Promise.resolve();
          },
        },
      }) as ServiceImpl;
    },
    Error,
    "name cannot contain '/'",
  );

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
      handler: (_err: Error | null, msg: Msg): Promise<void> => {
        msg?.respond(sc.encode("hello"));
        return Promise.resolve();
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
    const r = ping as unknown as Record<string, unknown>;
    delete r.version;
    delete r.name;
    delete r.id;
    assertEquals(Object.keys(r).length, 0, JSON.stringify(r));
  }
  verifyPing(await collect(await m.ping()));
  verifyPing(await collect(await m.ping("test")));
  verifyPing(await collect(await m.ping("test", srv.id)));

  function verifyInfo(infos: ServiceInfo[]) {
    verifyIdentity(infos);
    const info = infos[0];
    assertEquals(info.description, srv.description);
    assertEquals(info.subject, srv.subject);
    const r = info as unknown as Record<string, unknown>;
    delete r.version;
    delete r.name;
    delete r.id;
    delete r.description;
    delete r.subject;
    assertEquals(Object.keys(r).length, 0, JSON.stringify(r));
  }

  // info
  verifyInfo(await collect(await m.info()));
  verifyInfo(await collect(await m.info("test")));
  verifyInfo(await collect(await m.info("test", srv.id)));

  function verifyStats(stats: ServiceStats[]) {
    verifyIdentity(stats);
    const stat = stats[0];
    assertEquals(stat.num_requests, 2);
    assertEquals(stat.num_errors, 0);
    assertEquals(typeof stat.processing_time, "number");
    assertEquals(typeof stat.average_processing_time, "number");
    assert(Date.parse(stat.started) > 0);

    // assert(Date.parse(stat.started) - Date.now() > 0, JSON.stringify(stat));

    const r = stat as unknown as Record<string, unknown>;
    delete r.version;
    delete r.name;
    delete r.id;
    delete r.num_requests;
    delete r.num_errors;
    delete r.processing_time;
    delete r.average_processing_time;
    delete r.started;
    assertEquals(Object.keys(r).length, 0, JSON.stringify(r));
  }

  verifyStats(await collect(await m.stats()));
  verifyStats(await collect(await m.stats("test")));
  verifyStats(await collect(await m.stats("test", srv.id)));

  function verifySchema(schemas: ServiceSchema[]) {
    verifyIdentity(schemas);
    const schema = schemas[0];
    assertExists(schema.schema);
    assertEquals(schema.schema?.request, srv.config.schema?.request);
    assertEquals(schema.schema?.response, srv.config.schema?.response);

    const r = schema as unknown as Record<string, unknown>;
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
      handler: (_err: Error | null, msg: Msg): Promise<void> => {
        msg?.respond();
        return Promise.resolve();
      },
    },
  }) as ServiceImpl;

  const srvB = await nc.services.add({
    name: "test",
    version: "0.0.0",
    endpoint: {
      subject: "foo",
      handler: (_err: Error | null, msg: Msg): Promise<void> => {
        msg?.respond();
        return Promise.resolve();
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

Deno.test("service - handler error", async () => {
  const { ns, nc } = await setup();

  await nc.services.add({
    name: "test",
    version: "1.2.3",
    endpoint: {
      subject: "fail",
      handler: (): Promise<void> => {
        throw new Error("cb error");
      },
    },
  });

  const r = await nc.request("fail");
  assertEquals(r.headers?.get(ServiceErrorHeader), "cb error");
  assertEquals(r.headers?.get(ServiceErrorCodeHeader), "400");

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
      handler: (err): Promise<void> => {
        if (err) {
          service.stop(err);
          return Promise.reject(err);
        }
        fail("shouldn't have subscribed");
        return Promise.resolve();
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
      handler: (): Promise<void> => {
        return Promise.resolve();
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
      handler: (err): Promise<void> => {
        if (err === null) {
          throw new Error("boom");
        } else {
          console.log("error here", err);
        }
        return Promise.resolve();
      },
    },
  });

  const m = await nc.request("fail");
  assertEquals(m.headers?.get(ServiceErrorHeader), "boom");
  assertEquals(m.headers?.get(ServiceErrorCodeHeader), "400");

  await cleanup(ns, nc);
});

Deno.test("service -service error is headers", async () => {
  const { ns, nc } = await setup();
  await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "fail",
      handler: (): void => {
        throw new ServiceError(1210, "something");
      },
    },
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
    endpoint: {
      subject: "q",
      handler: (): Promise<void> => {
        return Promise.resolve();
      },
    },
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
    endpoint: {
      subject: "q",
      handler: (): Promise<void> => {
        return Promise.resolve();
      },
    },
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

  await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "q",
      handler: (): Promise<void> => {
        return Promise.resolve();
      },
    },
    statsHandler: (): Promise<unknown> => {
      return Promise.resolve({ hello: "world" });
    },
  });

  const m = nc.services.client();
  const stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(((stats[0].data) as Record<string, string>).hello, "world");

  await cleanup(ns, nc);
});

Deno.test("service - bad stats handler", async () => {
  const { ns, nc } = await setup();

  const config = {
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "q",
      handler: (): Promise<void> => {
        return Promise.resolve();
      },
    },
    // @ts-ignore: test
    statsHandler: "hello world",
  };
  await nc.services.add(config as unknown as ServiceConfig);
  const m = nc.services.client();
  const stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].data, undefined);

  await cleanup(ns, nc);
});

Deno.test("service - stats handler error", async () => {
  const { ns, nc } = await setup();

  await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "q",
      handler: (): Promise<void> => {
        return Promise.resolve();
      },
    },
    statsHandler: (): Promise<unknown> => {
      throw new Error("bad stats handler");
    },
  });
  const m = nc.services.client();
  const stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].data, undefined);
  assertEquals(stats[0].last_error, "bad stats handler");
  assertEquals(stats[0].num_errors, 1);

  await cleanup(ns, nc);
});

Deno.test("service - reset", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "q",
      handler: (_err, msg): void => {
        msg.respond();
      },
    },
  }) as ServiceImpl;

  await nc.request("q");
  await nc.request("q");

  service._stats.num_errors = 1;
  service._stats.last_error = new Error("hello");

  const m = nc.services.client();
  let stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assert(stats[0].processing_time >= 0);
  assert(stats[0].average_processing_time >= 0);
  assertEquals(stats[0].num_errors, 1);
  assertEquals(stats[0].last_error, "hello");

  service.reset();
  stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].num_requests, 0);
  assertEquals(stats[0].processing_time, 0);
  assertEquals(stats[0].average_processing_time, 0);
  assertEquals(stats[0].num_errors, 0);
  assertEquals(stats[0].last_error, undefined);

  await cleanup(ns, nc);
});

Deno.test("service - iter", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "q",
    },
  }) as ServiceImpl;

  (async () => {
    for await (const m of service) {
      await delay(500);
      m.respond();
    }
  })().then();

  await nc.request("q");
  await nc.request("q");

  service._stats.num_errors = 1;
  service._stats.last_error = new Error("hello");

  const m = nc.services.client();
  let stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assert(stats[0].processing_time >= 0);
  assert(stats[0].average_processing_time > 0);
  assertEquals(stats[0].num_errors, 1);
  assertEquals(stats[0].last_error, "hello");

  service.reset();
  stats = await collect(await m.stats());
  assertEquals(stats.length, 1);
  assertEquals(stats[0].num_requests, 0);
  assertEquals(stats[0].processing_time, 0);
  assertEquals(stats[0].average_processing_time, 0);
  assertEquals(stats[0].num_errors, 0);
  assertEquals(stats[0].last_error, undefined);

  await cleanup(ns, nc);
});

Deno.test("service - iter closed", async () => {
  const { ns, nc } = await setup();

  const service = await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "q",
    },
  });

  (async () => {
    for await (const m of service) {
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
      // @ts-ignore: testing
      version: v,
      endpoint: {
        subject: "q",
      },
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
  assertEquals(info.subject, "q");

  await cleanup(ns, nc);
});

Deno.test("service - service errors", async () => {
  const { ns, nc } = await setup();
  const srv = await nc.services.add({
    name: "test",
    version: "2.0.0",
    endpoint: {
      subject: "q",
    },
  });

  (async () => {
    for await (const m of srv) {
      m.data.length ? m.respond() : m.respondError(411, "data required");
    }
  })().then();

  let r = await nc.request("q");
  assertEquals(ServiceError.isServiceError(r), true);
  const serr = ServiceError.toServiceError(r);
  assertEquals(serr?.code, 411);
  assertEquals(serr?.message, "request requires some data");

  r = await nc.request("q");
  assertEquals(ServiceError.isServiceError(r), false);
  assertEquals(ServiceError.toServiceError(r), null);
  await cleanup(ns, nc);
});

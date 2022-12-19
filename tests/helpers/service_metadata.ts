import { cli } from "https://deno.land/x/cobra@v0.0.9/mod.ts";
import {
  connect,
  EndpointStats,
  NatsConnection,
  ServiceError,
  ServiceIdentity,
  ServiceInfo,
  ServiceSchema,
  ServiceVerb,
  StringCodec,
} from "../../src/mod.ts";

import { collect } from "../../nats-base-client/util.ts";
import { ServiceClientImpl } from "../../nats-base-client/serviceclient.ts";

const root = cli({
  use: "service-check [--name name] [--server host:port]",
  run: async (cmd, _args, flags): Promise<number> => {
    const servers = [flags.value<string>("server")];
    const name = flags.value<string>("name");

    let error;
    let nc: NatsConnection | null = null;
    try {
      nc = await connect({ servers });
      await invoke(nc, name);
      await checkPing(nc, name);
      await checkInfo(nc, name);
      await checkSchema(nc, name);
      await checkStats(nc, name);
    } catch (err) {
      cmd.stderr(err.message);
      console.log(err);
      error = err;
    } finally {
      await nc?.close();
    }
    return error ? Promise.resolve(1) : Promise.resolve(0);
  },
});
root.addFlag({
  short: "n",
  name: "name",
  type: "string",
  usage: "service name to filter on",
  default: "",
  persistent: true,
  required: true,
});

root.addFlag({
  name: "server",
  type: "string",
  usage: "NATS server to connect to",
  default: "localhost:4222",
  persistent: true,
});

function filter<T extends ServiceIdentity>(name: string, responses: T[]): T[] {
  const n = name.toUpperCase();
  return responses.filter((r) => {
    return r.name === n;
  });
}
function filterExpectingOnly<T extends ServiceIdentity>(
  tag: string,
  name: string,
  responses: T[],
): T[] {
  const filtered = filter(name, responses);
  assertEquals(
    filtered.length,
    responses.length,
    `expected ${tag} to have only services named ${name}`,
  );
  return filtered;
}

function checkResponse<T extends ServiceIdentity>(
  tag: string,
  responses: T[],
  requiredKeys: string[],
) {
  assert(responses.length > 0, `expected responses for ${tag}`);
  requiredKeys.forEach((k) => {
    responses.forEach((r) => {
      assert(
        (r as Record<string, unknown>)[k] !== "undefined",
        `expected ${tag} responses to have field ${k}`,
      );
      delete (r as Record<string, unknown>)[k];
    });
  });
  responses.forEach((r) => {
    assert(
      Object.keys(r).length === 0,
      `expected ${tag} to not contain other properties ${JSON.stringify(r)}`,
    );
  });
}

async function checkStats(nc: NatsConnection, name: string) {
  await check(nc, ServiceVerb.STATS, name, [
    "name",
    "id",
    "version",
    "num_requests",
    "num_errors",
    "last_error",
    "data",
    "processing_time",
    "average_processing_time",
    "started",
  ], (v) => {
    const stats = v as EndpointStats;
    assertEquals(typeof stats.name, "string", "name");
    assertEquals(typeof stats.id, "string", "id");
    assertEquals(typeof stats.version, "string", "version");
    assertEquals(typeof stats.num_requests, "number", "num_requests");
    assertEquals(typeof stats.num_errors, "number", "num_errors");
    assertEquals(typeof stats.last_error, "string", "last_error");
    assertEquals(typeof stats.data, "string", "data");
    assertEquals(typeof stats.processing_time, "number", "processing_time");
    assertEquals(
      typeof stats.average_processing_time,
      "number",
      "average_processing_time",
    );
    assertEquals(typeof stats.started, "string", "started");
  });
}

async function checkSchema(nc: NatsConnection, name: string) {
  await check(nc, ServiceVerb.SCHEMA, name, [
    "name",
    "id",
    "version",
    "schema",
  ], (v) => {
    const o = v as ServiceSchema;
    assertEquals(typeof o.name, "string", "name");
    assertEquals(typeof o.id, "string", "id");
    assertEquals(typeof o.version, "string", "version");
    assertEquals(typeof o.schema, "object", "schema");
    assertEquals(typeof o.schema.request, "string", "schema.request");
    assertEquals(typeof o.schema.response, "string", "schema.response");
  });
}

async function checkInfo(nc: NatsConnection, name: string) {
  await check(nc, ServiceVerb.INFO, name, [
    "name",
    "id",
    "version",
    "description",
    "subject",
  ], (v) => {
    const info = v as ServiceInfo;
    assertEquals(typeof v.name, "string", "name");
    assertEquals(typeof v.id, "string", "id");
    assertEquals(typeof v.version, "string", "version");
    assertEquals(typeof info.description, "string", "description");
    assertEquals(typeof info.subject, "string", "subject");
  });
}

async function checkPing(nc: NatsConnection, name: string) {
  await check(nc, ServiceVerb.PING, name, ["name", "id", "version"], (v) => {
    assertEquals(typeof v.name, "string", "name");
    assertEquals(typeof v.id, "string", "id");
    assertEquals(typeof v.version, "string", "version");
  });
}

async function invoke(nc: NatsConnection, name: string): Promise<void> {
  const sc = nc.services.client();
  const infos = await collect(await sc.info(name));

  let proms = infos.map((v) => {
    return nc.request(v.subject);
  });
  let responses = await Promise.all(proms);
  responses.forEach((m) => {
    assertEquals(
      ServiceError.isServiceError(m),
      true,
      "expected service without payload to return error",
    );
  });

  // the service should throw/register an error if "error" is specified as payload
  proms = infos.map((v) => {
    return nc.request(v.subject, StringCodec().encode("error"));
  });
  responses = await Promise.all(proms);
  responses.forEach((m) => {
    assertEquals(
      ServiceError.isServiceError(m),
      true,
      "expected service without payload to return error",
    );
  });

  proms = infos.map((v, idx) => {
    return nc.request(v.subject, StringCodec().encode(`hello ${idx}`));
  });
  responses = await Promise.all(proms);
  responses.forEach((m, idx) => {
    const r = `hello ${idx}`;
    assertEquals(
      StringCodec().decode(m.data),
      r,
      `expected service response ${r}`,
    );
  });
}

async function check(
  nc: NatsConnection,
  verb: ServiceVerb,
  name: string,
  keys: string[],
  check?: (v: ServiceIdentity) => void,
) {
  const fn = (d: ServiceIdentity[]): void => {
    if (check) {
      try {
        d.forEach(check);
      } catch (err) {
        throw new Error(`${verb} check: ${err.message}`);
      }
    }
  };

  const sc = nc.services.client() as ServiceClientImpl;
  // all
  let responses = filter(
    name,
    await collect(await sc.q<ServiceIdentity>(verb)),
  );

  assert(responses.length >= 1);
  fn(responses);
  checkResponse(`${verb}()`, responses, keys);

  // just matching name
  responses = filterExpectingOnly(
    `${verb}(${name})`,
    name,
    await collect(await sc.q<ServiceIdentity>(verb, name)),
  );
  assert(responses.length >= 1);
  fn(responses);
  checkResponse(`${verb}(${name})`, responses, keys);

  // specific service
  responses = filterExpectingOnly(
    `${verb}(${name})`,
    name,
    await collect(await sc.q<ServiceIdentity>(verb, name, responses[0].id)),
  );
  assert(responses.length === 1);
  fn(responses);
  checkResponse(`${verb}(${name})`, responses, keys);
}

function assert(v: unknown, msg?: string) {
  if (typeof v === undefined || typeof v === null || v === false) {
    throw new Error(msg || "expected value to be truthy");
  }
}

function assertEquals(v: unknown, expected: unknown, msg?: string) {
  if (v !== expected) {
    throw new Error(msg || `expected ${v} === ${expected}`);
  }
}

Deno.exit(await root.execute(Deno.args));

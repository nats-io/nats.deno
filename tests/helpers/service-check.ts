import { cli } from "https://deno.land/x/cobra@v0.0.9/mod.ts";
import {
  connect,
  NatsConnection,
  ServiceError,
  ServiceIdentity,
  ServiceInfo,
  ServiceSchema,
  ServiceStats,
  ServiceVerb,
  StringCodec,
} from "../../src/mod.ts";

import { collect } from "../../nats-base-client/util.ts";
import { ServiceClientImpl } from "../../nats-base-client/serviceclient.ts";
import Ajv, { JSONSchemaType, ValidateFunction } from "npm:ajv";

import { parseSemVer } from "../../nats-base-client/semver.ts";
import { ServiceResponseType } from "../../nats-base-client/service.ts";

const ajv = new Ajv();

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
  return responses.filter((r) => {
    return r.name === name;
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
  validator: ValidateFunction<T>,
) {
  assert(responses.length > 0, `expected responses for ${tag}`);

  responses.forEach((r) => {
    const valid = validator(r);
    if (!valid) {
      console.log(validator.errors);
      throw new Error(validator.errors[0].message);
    }
  });
}
//
async function checkStats(nc: NatsConnection, name: string) {
  const validateFn = ajv.compile(StatsSchema);
  await check(nc, ServiceVerb.STATS, name, validateFn, (v) => {
    assertEquals(v.type, ServiceResponseType.STATS);
    parseSemVer(v.version);
  });
}

async function checkSchema(nc: NatsConnection, name: string) {
  const validateFn = ajv.compile(ServiceSchema);
  await check(nc, ServiceVerb.SCHEMA, name, validateFn, (v) => {
    assertEquals(v.type, ServiceResponseType.SCHEMA);
    parseSemVer(v.version);
  });
}

async function checkInfo(nc: NatsConnection, name: string) {
  const validateFn = ajv.compile(InfoSchema);
  await check(nc, ServiceVerb.INFO, name, validateFn, (v) => {
    assertEquals(v.type, ServiceResponseType.INFO);
    parseSemVer(v.version);
  });
}

async function checkPing(nc: NatsConnection, name: string) {
  const validateFn = ajv.compile(PingSchema);
  await check(nc, ServiceVerb.PING, name, validateFn, (v) => {
    assertEquals(v.type, ServiceResponseType.PING);
    parseSemVer(v.version);
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
  validateFn: ValidateFunction,
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

  assert(responses.length >= 1, `expected at least 1 response to ${verb}`);
  fn(responses);
  checkResponse(`${verb}()`, responses, validateFn);

  // just matching name
  responses = filterExpectingOnly(
    `${verb}(${name})`,
    name,
    await collect(await sc.q<ServiceIdentity>(verb, name)),
  );
  assert(
    responses.length >= 1,
    `expected at least 1 response to ${verb}.${name}`,
  );
  fn(responses);
  checkResponse(`${verb}(${name})`, responses, validateFn);

  // specific service
  responses = filterExpectingOnly(
    `${verb}(${name})`,
    name,
    await collect(await sc.q<ServiceIdentity>(verb, name, responses[0].id)),
  );
  assert(
    responses.length === 1,
    `expected at least 1 response to ${verb}.${name}.${responses[0].id}`,
  );
  fn(responses);
  checkResponse(`${verb}(${name})`, responses, validateFn);
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

export const StatsSchema: JSONSchemaType<ServiceStats> = {
  type: "object",
  properties: {
    type: { type: "string" },
    name: { type: "string" },
    id: { type: "string" },
    version: { type: "string" },
    num_requests: { type: "number" },
    num_errors: { type: "number" },
    last_error: { type: "string" },
    processing_time: { type: "number" },
    average_processing_time: { type: "number" },
    started: { type: "string" },
    data: { type: "string" },
  },
  required: [
    "type",
    "name",
    "id",
    "version",
    "num_requests",
    "num_errors",
    "last_error",
    "processing_time",
    "average_processing_time",
    "started",
    "data",
  ],
  additionalProperties: false,
};

export const ServiceSchema: JSONSchemaType<ServiceSchema> = {
  type: "object",
  properties: {
    type: { type: "string" },
    name: { type: "string" },
    id: { type: "string" },
    version: { type: "string" },
    schema: {
      type: "object",
      properties: {
        request: { type: "string" },
        response: { type: "string" },
      },
      required: ["request", "response"],
    },
  },
  required: ["type", "name", "id", "version", "schema"],
  additionalProperties: false,
};

const InfoSchema: JSONSchemaType<ServiceInfo> = {
  type: "object",
  properties: {
    type: { type: "string" },
    name: { type: "string" },
    id: { type: "string" },
    version: { type: "string" },
    description: { type: "string" },
    subject: { type: "string" },
  },
  required: ["type", "name", "id", "version", "subject"],
  additionalProperties: false,
};

export const PingSchema: JSONSchemaType<ServiceIdentity> = {
  type: "object",
  properties: {
    type: { type: "string" },
    name: { type: "string" },
    id: { type: "string" },
    version: { type: "string" },
  },
  required: ["type", "name", "id", "version"],
  additionalProperties: false,
};

Deno.exit(await root.execute(Deno.args));

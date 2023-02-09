/*
 * Copyright 2021-2023 The NATS Authors
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

import * as path from "https://deno.land/std@0.177.0/path/mod.ts";
import { NatsServer } from "../tests/helpers/mod.ts";
import { connect } from "../src/mod.ts";
import { assert } from "https://deno.land/std@0.177.0/testing/asserts.ts";
import {
  ConnectionOptions,
  extend,
  NatsConnection,
  nuid,
} from "../nats-base-client/internal_mod.ts";
import { StreamConfig } from "../nats-base-client/types.ts";

export function jsopts() {
  return {
    // debug: true,
    // trace: true,
    jetstream: {
      max_file_store: 1024 * 1024,
      max_memory_store: 1024 * 1024,
      store_dir: "/tmp",
    },
  };
}

export function jetstreamExportServerConf(
  opts: unknown = {},
  prefix = "IPA.>",
  randomStoreDir = true,
): Record<string, unknown> {
  const template = {
    "no_auth_user": "a",
    accounts: {
      JS: {
        jetstream: "enabled",
        users: [{ user: "js", password: "js" }],
        exports: [
          { service: "$JS.API.>" },
          { service: "$JS.ACK.>" },
          { stream: "A.>", accounts: ["A"] },
        ],
      },
      A: {
        users: [{ user: "a", password: "s3cret" }],
        imports: [
          { service: { subject: "$JS.API.>", account: "JS" }, to: prefix },
          { service: { subject: "$JS.ACK.>", account: "JS" } },
          { stream: { subject: "A.>", account: "JS" } },
        ],
      },
    },
  };
  const conf = Object.assign(template, opts);
  return jetstreamServerConf(conf, randomStoreDir);
}

export function jetstreamServerConf(
  opts: unknown = {},
  randomStoreDir = true,
): Record<string, unknown> {
  const conf = Object.assign(jsopts(), opts);
  if (randomStoreDir) {
    conf.jetstream.store_dir = path.join("/tmp", "jetstream", nuid.next());
  }
  Deno.mkdirSync(conf.jetstream.store_dir, { recursive: true });
  return conf as Record<string, unknown>;
}
export async function setup(
  serverConf?: Record<string, unknown>,
  clientOpts?: Partial<ConnectionOptions>,
): Promise<{ ns: NatsServer; nc: NatsConnection }> {
  const dt = serverConf as { debug: boolean; trace: boolean };
  const debug = dt && (dt.debug || dt.trace);
  const ns = await NatsServer.start(serverConf, debug);
  clientOpts = clientOpts ? clientOpts : {};
  const copts = extend({ port: ns.port }, clientOpts) as ConnectionOptions;
  const nc = await connect(copts);
  return { ns, nc };
}

export async function cleanup(
  ns: NatsServer,
  ...nc: NatsConnection[]
): Promise<void> {
  const conns: Promise<void>[] = [];
  nc.forEach((v) => {
    conns.push(v.close());
  });
  await Promise.all(conns);
  await ns.stop();
}

export async function initStream(
  nc: NatsConnection,
  stream: string = nuid.next(),
  opts: Partial<StreamConfig> = {},
): Promise<{ stream: string; subj: string }> {
  const jsm = await nc.jetstreamManager();
  const subj = `${stream}.A`;
  const sc = Object.assign({ name: stream, subjects: [subj] }, opts);
  await jsm.streams.add(sc);
  return { stream, subj };
}

export function time(): Mark {
  return new Mark();
}

export class Mark {
  measures: [number, number][];
  constructor() {
    this.measures = [];
    this.measures.push([Date.now(), 0]);
  }

  mark() {
    const now = Date.now();
    const idx = this.measures.length - 1;
    if (this.measures[idx][1] === 0) {
      this.measures[idx][1] = now;
    } else {
      this.measures.push([now, 0]);
    }
  }

  duration(): number {
    const idx = this.measures.length - 1;
    if (this.measures[idx][1] === 0) {
      this.measures.pop();
    }
    const times = this.measures.map((v) => v[1] - v[0]);
    return times.reduce((result, item) => {
      return result + item;
    });
  }

  assertLess(target: number) {
    const d = this.duration();
    assert(
      target >= d,
      `duration ${d} not in range - ${target} ≥ ${d}`,
    );
  }

  assertInRange(target: number) {
    const min = .8 * target;
    const max = 1.2 * target;
    const d = this.duration();
    assert(
      d >= min && max >= d,
      `duration ${d} not in range - ${min} ≥ ${d} && ${max} ≥ ${d}`,
    );
  }
}

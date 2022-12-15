/*
 * Copyright 2022 The NATS Authors
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
  connect,
  JSONCodec,
  SchemaInfo,
  ServiceError,
  ServiceMsg,
} from "../../src/mod.ts";
import { assertEquals } from "https://deno.land/std@0.161.0/testing/asserts.ts";

const nc = await connect();

// this is a pseudo JSON schema - current requirements is that it is a string
const schema: SchemaInfo = {
  request: JSON.stringify({
    type: "array",
    minItems: 1,
    items: { type: "number" },
  }),
  response: JSON.stringify({ type: "number" }),
};

// the service keeps track of the largest number it has seen
// and in the stats, reports it via the statsHandler
let maxMax = 0;
const statsHandler = (): Promise<unknown> => {
  return Promise.resolve({ max: maxMax });
};

// decoder extracts a JSON payload and performs some basic sanity
// or rejects the input
const decoder = (r: ServiceMsg): Promise<number[]> => {
  const jc = JSONCodec<number[]>();
  try {
    const a = jc.decode(r.data);
    if (a.length < 1) {
      return Promise.reject(
        new ServiceError(400, "input must have more than one element"),
      );
    }
    const bad = a.find((e) => {
      return typeof e !== "number";
    });
    if (bad) {
      return Promise.reject(
        new ServiceError(400, "input contains invalid types"),
      );
    }
    return Promise.resolve(a);
  } catch (err) {
    return Promise.reject(new ServiceError(400, "input is not an array"));
  }
};

// create a service - using the statsHandler and decoder
const service = await nc.services.add({
  name: "max",
  version: "0.0.1",
  description: "returns max number in a request",
  schema,
  endpoint: {
    subject: "max",
  },
  statsHandler,
});
// starting the service as an async function so that
// we can have this example be in a single file
const jc = JSONCodec<number>();
(async () => {
  for await (const r of service) {
    decoder(r)
      .then((a) => {
        a.sort((a, b) => b - a);
        const max = a[0];
        maxMax = Math.max(maxMax, max);
        r.respond(jc.encode(max));
      })
      .catch((err) => {
        r.respondError(
          (err as ServiceError).code || 400,
          err.message,
          jc.encode(0),
        );
      });
  }
})();

// now that the service is running:

// we call the service without any payload
let r = await nc.request("max");
assertEquals();

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

const jc = JSONCodec();

// connect to NATS on demo.nats.io
const nc = await connect({ servers: ["demo.nats.io"] });

// this is a pseudo JSON schema - current requirements is that it is a string
// so more conveniently return an URL to your schemas.
const schema: SchemaInfo = {
  request: JSON.stringify({
    type: "array",
    minItems: 1,
    items: { type: "number" },
  }),
  response: JSON.stringify({ type: "number" }),
};

// create a service - using the statsHandler and decoder
const calc = await nc.services.add({
  name: "calc",
  version: "0.0.1",
  description: "example calculator service",
  apiURL: "http://somesite.com",
});

// For this example the thing we want to showcase is how you can
// create service that has multiple endpoints.
// The service will have `sum`, `max`, `average` and `min` operations.
// While we could create a service that listens on `sum`, `max`, etc.,
// creating a complex hierarchy will allow you to carve the subject
// name space to allow for better access control, and organize your
// services better.
// In the service API, you `addGroup()` to the service, which will
// introduce a prefix for the calculator's endpoints. The group name
// can be any valid subject that can be prefixed into another.
const g = calc.addGroup("calc");
// We can now add endpoints under this which will augment the subject
// space, adding an endpoint. Endpoints can only have a simple name
// and can specify an optional callback:

// this is the simplest endpoint - returns an iterator
// additional options such as a handler, subject, or schema can be
// specified.
// this endpoint is accessible as `calc.sum`
const sums = g.addEndpoint("sum");
(async () => {
  for await (const m of sums) {
    const numbers = decode(m);
    const s = numbers.reduce((sum, v) => {
      return sum + v;
    });
    m.respond(jc.encode(s));
  }
})().then();

// Here's another implemented using a callback, will be accessible by `calc.average`:
g.addEndpoint("average", (err, m) => {
  if (err) {
    calc.stop(err);
    return;
  }
  const numbers = decode(m);
  const sum = numbers.reduce((sum, v) => {
    return sum + v;
  });
  m.respond(jc.encode(sum / numbers.length));
});

// and another using a callback, and specifying our schema:
g.addEndpoint("min", {
  schema,
  handler: (err, m) => {
    if (err) {
      calc.stop(err);
      return;
    }
    const numbers = decode(m);
    const min = numbers.reduce((n, v) => {
      return Math.min(n, v);
    });
    m.respond(jc.encode(min));
  },
});

g.addEndpoint("max", (err, m) => {
  if (err) {
    calc.stop(err);
    return;
  }
  const numbers = decode(m);
  const max = numbers.reduce((n, v) => {
    return Math.max(n, v);
  });
  m.respond(jc.encode(max));
});

calc.stopped.then((err: Error | null) => {
  console.log(`calc stopped ${err ? "because: " + err.message : ""}`);
});

// Now we switch gears and look at a client making a request
async function calculate(op: string, a: number[]): Promise<void> {
  const r = await nc.request(`calc.${op}`, jc.encode(a));
  if (ServiceError.isServiceError(r)) {
    console.log(ServiceError.toServiceError(r));
    return;
  }
  const ans = jc.decode<number>(r.data);
  console.log(`${op} ${a.join(", ")} = ${ans}`);
}

await Promise.all([
  calculate("sum", [5, 10, 15]),
  calculate("average", [5, 10, 15]),
  calculate("min", [5, 10, 15]),
  calculate("max", [5, 10, 15]),
]);

// stop the service
await calc.stop();
// and close the connection
await nc.close();

// a simple decoder that tosses a ServiceError if the input is not what we want.
function decode(m: ServiceMsg): number[] {
  const a = jc.decode<number[]>(m.data);
  if (!Array.isArray(a)) {
    throw new ServiceError(400, "input requires array");
  }
  if (a.length === 0) {
    throw new ServiceError(400, "array must have at least one number");
  }
  a.forEach((v) => {
    if (typeof v !== "number") {
      throw new ServiceError(400, "array elements must be numbers");
    }
  });
  return a;
}

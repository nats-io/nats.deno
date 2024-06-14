# nats-core.js

The nats-core.js implements the _nats-core_ functionality for JavaScript
clients. _Nats-core_ is the basic functionality of a NATS client with respect
to:

- Authentication
- Connection and connection lifecycle
- Messaging functionality (publish, subscribe and request reply)
- And other standard client protocol handling functionality

JavaScript NATS clients implement native runtime transports (node, deno,
browser) and export a `connect` function that returns a concrete instance of a
NATS client transport which uses and re-exports this library to expose the APIs
codified in this library.

You can use this library to implement other JavaScript functionality that uses a
NATS client connection without binding your implementation to a particular
JavaScript runtime. For example, the @nats-io/jetstream library depends on
@nats-io/nats-core to implement all of its JetStream protocol.

# Installation

If you are not implementing a NATS client compatible library, you can use this
repository to view the documentation of the NATS core functionality. Your NATS
client instance already uses and re-exports the library implemented here.

Note that this library is distributed in two different bundles:

- npm a node-specific library supporting CJS (`require`) and ESM (`import`)
- jsr a node and other ESM (`import`) compatible runtimes (deno, browser, node)

If your application doesn't use `require`, you can simply depend on the JSR
version.

## NPM

The NPM registry hosts a node-only compatible version of the library
[@nats-io/nats-core](https://www.npmjs.com/package/@nats-io/nats-core)
supporting both CJS and ESM:

```bash
npm install @nats-io/nats-core
```

## JSR

The JSR registry hosts the EMS-only
[@nats-io/nats-core](https://jsr.io/@nats-io/nats-core) version of the library.

```bash
deno add @nats-io/nats-core
```

```bash
npx jsr add @nats-io/nats-core
```

```bash
yarn dlx jsr add @nats-io/nats-core
```

```bash
bunx jsr add @nats-io/nats-core
```

## Referencing the library

Once you import the library, you can reference in your code as:

```javascript
import * as nats_core from "@nats-io/nats-core";
// or in node (only when using CJS)
const nats_core = require("@nats-io/nats-core");
```

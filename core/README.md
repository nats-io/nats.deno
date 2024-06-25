# NATS Core

The _core_ module implements the _core_ functionality for JavaScript clients:

- Connection, authentication, connection lifecycle
- NATS protocol handling - messaging functionality (publish, subscribe and
  request reply)

A native transports (node, deno, browser) module exports a `connect` function
that returns a concrete instance of a `NatsConnection` which exports all the
functionality in this module.

You can use this module as a runtime agnostic dependency and implement
functionality that uses a NATS client connection without binding your code to a
particular JavaScript runtime. For example, the @nats-io/jetstream library
depends on @nats-io/nats-core to implement all of its JetStream protocol.

# Installation

If you are not implementing a NATS client compatible module, you can use this
repository to view the documentation of the NATS core functionality. Your NATS
client instance already uses and re-exports the module implemented here, so
there's no need for you to directly depend on this library.

Note that this module is distributed in two different registries:

- npm a node-specific library supporting CJS (`require`) and ESM (`import`) for
  node specific projects
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

The JSR registry hosts the ESM-only
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

The main entry point for this library is the `NatsConnection`.

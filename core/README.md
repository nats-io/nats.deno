# core

The _core_ module implements the _nats-core_ functionality for JavaScript
clients. _Nats-core_ is the basic functionality of a NATS client with respect
to:

- Connection, authentication connection lifecycle, and NATS protocol handling
- Messaging functionality (publish, subscribe and request reply)

JavaScript NATS clients implement specific native runtime transports (node,
deno, browser) and export a `connect` function that returns a concrete instance
of a NatsConnection which wraps the specific runtime transport. The specific
runtime transport re-exports this library to expose the APIs implemented in this
library.

You can use this library as a runtime agnostic dependency and implement other
JavaScript functionality that uses a NATS client connection without binding your
implementation to a particular JavaScript runtime. For example, the
@nats-io/jetstream library depends on @nats-io/nats-core to implement all of its
JetStream protocol.

# Installation

If you are not implementing a NATS client compatible library, you can use this
repository to view the documentation of the NATS core functionality. Your NATS
client instance already uses and re-exports the library implemented here, so
there's no need for you to directly depend on this library.

Note that this library is distributed in two different registries:

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

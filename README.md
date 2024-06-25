# NATS.js - A [NATS](http://nats.io) client for JavaScript

[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](./LICENSE)
![Test NATS.deno](https://github.com/nats-io/nats.deno/workflows/NATS.deno/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/nats.deno/badge.svg?branch=main)](https://coveralls.io/github/nats-io/nats.deno?branch=main)

This repository hosts NATS clients for JavaScript runtimes, including:

- Deno
- Node/Bun
- Browsers (W3C websocket)

The repository is a mono-repo which the various runtime transports and a set of
common modules (libraries) that shape the transports into a NATS client.

Previous versions of the NATS clients merged the specific transport
functionality (runtime) with the "NATS Base Client", the library providing the
APIs to interact with the NATS server.

In order to be more flexible and allow the ecosystem to more easily grow the
basic functionality has been split into 5 modules:

- [Core](core/README.md) which implements basic NATS core functionality
- [JetStream](jetstream/README.md) which implements JetStream functionality
- [Kv](kv/README.md) which implements NATS KV functionality (uses JetStream)
- [Obj](obj/README.md) which implements NATS Object Store functionality (uses
  JetStream)
- [Services](obj/README.md) which implements a framework for building NATS
  services

The above modules provide a different way for working with NATS. If you are
getting started, perhaps you heard about the NATS KV and would incorporate it
into your app. The KV module may be all that you need to get started. You will
of course need a transport, but once you know how to make a connection to NATS,
you can bypass much of the APIs and simply focus on the one that grabbed your
attention. From there feel free to explore the different aspects of NATS. At
some point, we are certain that your use most, if not all, of the modules.

This allows basic clients to be smaller as features such as JetStream or KV are
opt-in components, and also allowing each module to be versioned separately.
This provides some additional developer comfort as it allows each module to
specify its own [semantic version](https://semver.org/), and thus prevent
surprises when upgrading.

The decoupling of the NATS client functionality from the actual runtime, also
enables developers to write modules that can run on different runtimes provided
they follow a pattern where a `NatsConnection` is used regardless of the actual
runtime. For example, the JetStream module exposes a `jetstream()` function that
will return a JetStream API that you use to interact with JetStream. The actual
connection type is not important, and the library will work regardless of the
runtime provided the runtime has the minimum support required by the library.

[//]: # (- [Node Transport]&#40;&#41; which implements a TCP transport for Node.js)
[//]: # (- [WebSocket Transport]&#40;&#41; which implements a W3C compatible websocket transport)
[//]: # (  that can run in Deno, Node.js &#40;22&#41;, and Browsers.)

# Getting Started

If you are migrating from the legacy nats.deno or nats.js or nats.ws clients
don't despair. Changes are very easy to on-board, and are all
[described here](migration.md).

If you want to get started with NATS the best starting point is the transport
that matches the runtime you want to use:

- [Deno Transport](transport-deno/README.md) which implements a TCP transport
  for [Deno](https://deno.land)

The module for the transport will tell you how to install it, and how to use it.

If you want to write a library that uses NATS under the cover, your starting
point is likely [Core](core/README.md). If data oriented it may be JetStream.

## Documentation

Each of the modules has an introductory page that shows the main API usage for
the module. It also hosts its JsDoc.

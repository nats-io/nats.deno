# NATS.deno - A [NATS](http://nats.io) client for [Deno](https://deno.land)


A Deno client for the [NATS messaging system](https://nats.io).

[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](./LICENSE)
![Test NATS.deno](https://github.com/nats-io/nats.deno/workflows/NATS.deno/badge.svg)


# Installation

** :warning: NATS.deno is a release candidate** you can get the current development version by:

```bash
import * as nats from "https://raw.githubusercontent.com/nats-io/nats.deno/main/src/mod.ts"
```

To see a list of items that are under development see [TODO](TODO.md).


## Basics


### Connecting to a nats-server

To connect to a server you use the `connect()` function. It returns
a connection that you can use to interact with the server. You can customize
the connection by specifying `ConnectionOptions`.

By default, a connection will attempt a connection on`127.0.0.1:4222`. 
If the connection is dropped, the client will attempt to reconnect. 
You can customize the server you want to connect to by specifying `port` 
(for local connections), or full host port on the `servers` option.
Note that the `servers` option can be a single hostport (a string) or
an array of hostports.

When a connection is lost, any messages that have not been
sent to the server are lost. A client can queue  new messages to be sent when 
the connection resumes. If the connection  cannot be re-established the 
client will give up and `close` the connection.

To learn when a connection closes, wait for the promise returned by the `closed()`
function. If the close was due to an error, the promise will resolve to an error.

To disconnect from the nats-server, call `close()` on the connection.
A connection can also be terminated if there's an error. For example, the
server returns a run-time error. In those cases, the client will re-initiate
a connection.

This first example looks a bit complex, because it shows all of the things
discussed above. The example attempts to connect to a nats-server by specifying
different connect options. At least two of them should work if your
internet is working.

```typescript
// import the connect function
import { connect, NatsConnection } from "../../src/mod.ts";

// the connection is configured by a set of ConnectionOptions
// if none is set, the client will connect to 127.0.0.1:4222:
const localhostAtStandardPort = {};

// you can also specify a port:
const localPort = { port: 4222 };

// or a host using the standard 4222 port:
const hostAtStdPort = { servers: "demo.nats.io" };

// or the full host port
const hostPort = { servers: "demo.nats.io:4222" };

// let's try to connect to all the above, some may fail
const dials: Promise<NatsConnection>[] = [];
[localhostAtStandardPort, localPort, hostAtStdPort, hostPort].forEach((v) => {
  dials.push(connect(v));
});

const conns: NatsConnection[] = [];
// wait until all the dialed connections resolve or fail
// allSettled returns a tuple with `closed` and `value`:
await Promise.allSettled(dials)
  .then((a) => {
    // filter all the ones that succeeded
    const fulfilled = a.filter((v) => {
      return v.status === "fulfilled";
    });
    // and now extract all the connections
    //@ts-ignore
    const values = fulfilled.map((v) => v.value);
    conns.push(...values);
  });

// Print where we connected, and register a close handler
conns.forEach((nc) => {
  console.log(`connected to ${nc.getServer()}`);
  // you can get notified when the client exits by getting `closed()`.
  // closed resolves void or with an error if the connection
  // closed because of an error
  nc.closed()
    .then((err) => {
      let m = `connection to ${nc.getServer()} closed`;
      if (err) {
        m = `${m} with an error: ${err.message}`;
      }
      console.log(m);
    });
});

// now close all the connections, and wait for the close to finish
const closed = conns.map((nc) => nc.close());
await Promise.all(closed);
```

### Publish and Subscribe

The basic client operations are publish to `send` messages and
`subscribe` to receive messages. 

Messages are published to a subject. Subscriptions listen for 
messages on a subject. When a published message matches a subscription,
the server forwards the message to it.

In JavaScript clients (websocket, deno, or node) subscriptions work as an
async iterator - clients simply loop to process messages.

NATS messages are payload agnostic, meaning payloads are
`Uint8Arrays`.  You can easily send JSON or strings by using a 
`StringCodec` or a `JSONCodec`, or create a Codec of your own that 
handles the encoding or decoding of the data you are working with.

To stop a subscription, you call `unsubscribe()` or `drain()` on it.
You can drain all subscriptions and close the connection by calling
`drain()` on the connection. Drain unsubscribes from the subscription,
but gives the client a chance to process all messages it has
received but not yet processed.

```typescript
// import the connect function
import { connect, StringCodec } from "../../src/mod.ts";

// to create a connection to a nats-server:
const nc = await connect({ servers: "demo.nats.io:4222" });

// create a codec
const sc = StringCodec();
// create a simple subscriber and iterate over messages
// matching the subscription
const sub = nc.subscribe("hello");
(async () => {
  for await (const m of sub) {
    console.log(`[${sub.getProcessed()}]: ${sc.decode(m.data)}`);
  }
  console.log("subscription closed");
})();

nc.publish("hello", sc.encode("world"));
nc.publish("hello", sc.encode("again"));

// we want to insure that messages that are in flight
// get processed, so we are going to drain the
// connection. Drain is the same as close, but makes
// sure that all messages in flight get seen
// by the iterator. After calling drain on the connection
// the connection closes.
await nc.drain();
```


### Wildcard Subscriptions

Subjects can be used to organize messages into hierarchies.
For example the subject may add additional information that
can be useful in providing a context to the message.

Instead of subscribing to each specific subject for which you may want to
receive a message, you can create subscriptions that have wildcards.
Wildcards match one or more tokens in a subject, but tokens
represented by a wildcard are not specified.

Each subscription is independent. If two different subscriptions
match a subject, both will get to process the message:

```javascript
import { connect, StringCodec, Subscription } from "../../src/mod.ts";
const nc = await connect({ servers: "demo.nats.io:4222" });
const sc = StringCodec();

// subscriptions can have wildcard subjects
// the '*' matches any string in the specified token position
const s1 = nc.subscribe("help.*.system");
const s2 = nc.subscribe("help.me.*");
// the '>' matches any tokens in that position or following
// '>' can only be specified at the end of the subject
const s3 = nc.subscribe("help.>");

async function printMsgs(s: Subscription) {
  let subj = s.getSubject();
  console.log(`listening for ${subj}`);
  const c = (13 - subj.length);
  const pad = "".padEnd(c);
  for await (const m of s) {
    console.log(
      `[${subj}]${pad} #${s.getProcessed()} - ${m.subject} ${
        m.data ? " " + sc.decode(m.data) : ""
      }`,
    );
  }
}

printMsgs(s1);
printMsgs(s2);
printMsgs(s3);

// don't exit until the client closes
await nc.closed();

```

### Services: Request/Reply

When you publish a message, you have the ability to specify a `reply`
subject. The `reply` subject specifies a subject for a subscription
where the client making the request is waiting for a response.

### Services
A service is a client that responds to requests from other clients.

This example is a bit complicated, because we are going to use NATS
not only to implement a service.

```typescript
import { connect, StringCodec, Subscription } from "../../src/mod.ts";

// create a connection
const nc = await connect({ servers: "demo.nats.io" });

// create a codec
const sc = StringCodec();

// this subscription listens for `time` requests and returns the current time
const sub = nc.subscribe("time");
(async (sub: Subscription) => {
  console.log(`listening for ${sub.getSubject()} requests...`);
  for await (const m of sub) {
    if (m.respond(sc.encode(new Date().toISOString()))) {
      console.info(`[time] handled #${sub.getProcessed()}`);
    } else {
      console.log(`[time] #${sub.getProcessed()} ignored - no reply subject`);
    }
  }
  console.log(`subscription ${sub.getSubject()} drained.`);
})(sub);

// this subscription listens for admin.uptime and admin.stop
// requests to admin.uptime returns how long the service has been running
// requests to admin.stop gracefully stop the client by draining
// the connection
const started = Date.now();
const msub = nc.subscribe("admin.*");
(async (sub: Subscription) => {
  console.log(`listening for ${sub.getSubject()} requests [uptime | stop]`);
  // it would be very good to verify the origin of the request
  // before implementing something that allows your service to be managed.
  // NATS can limit which client can send or receive on what subjects.
  for await (const m of sub) {
    const chunks = m.subject.split(".");
    console.info(`[admin] #${sub.getProcessed()} handling ${chunks[1]}`);
    switch (chunks[1]) {
      case "uptime":
        // send the number of millis since up
        m.respond(sc.encode(`${Date.now() - started}`));
        break;
      case "stop": {
        m.respond(sc.encode(`[admin] #${sub.getProcessed()} stopping....`));
        // gracefully shutdown
        nc.drain()
          .catch((err) => {
            console.log("error draining", err);
          });
        break;
      }
      default:
        console.log(
          `[admin] #${sub.getProcessed()} ignoring request for ${m.subject}`,
        );
    }
  }
  console.log(`subscription ${sub.getSubject()} drained.`);
})(msub);

// wait for the client to close here.
await nc.closed().then((err?: void | Error) => {
  let m = `connection to ${nc.getServer()} closed`;
  if (err) {
    m = `${m} with an error: ${err.message}`;
  }
  console.log(m);
});

```

### Making Requests

```typescript
import { connect, StringCodec, Empty } from "../../src/mod.ts";

// create a connection
const nc = await connect({ servers: "demo.nats.io:4222" });

// create an encoder
const sc = StringCodec();

// the client makes a request and receives a promise for a message
// by default the request times out after 1s (1000 millis) and has
// no payload.
await nc.request("time", Empty, { timeout: 1000 })
  .then((m) => {
    console.log(`got response: ${sc.decode(m.data)}`);
  })
  .catch((err) => {
    console.log(`problem with request: ${err.message}`);
  });

await nc.close();

```

### Queue Groups
Queue groups allow scaling of services horizontally. Subscriptions for members of a 
queue group are treated as a single service, meaning when you send a message
only a single client in a queue group will receive it. There can be multiple queue 
groups, and each is treated as an independent group. Non-queue subscriptions are
also independent.

```typescript
import {
  connect,
  NatsConnection,
  StringCodec,
  Subscription,
} from "../../src/mod.ts";

async function createService(
  name: string,
  count: number = 1,
  queue: string = "",
): Promise<NatsConnection[]> {
  const conns: NatsConnection[] = [];
  for (let i = 1; i <= count; i++) {
    const n = queue ? `${name}-${i}` : name;
    const nc = await connect(
      { servers: "demo.nats.io:4222", name: `${n}` },
    );
    nc.closed()
      .then((err) => {
        if (err) {
          console.error(
            `service ${n} exited because of error: ${err.message}`,
          );
        }
      });
    // create a subscription - note the option for a queue, if set
    // any client with the same queue will be the queue group.
    const sub = nc.subscribe("echo", { queue: queue });
    const _ = handleRequest(n, sub);
    console.log(`${nc.options.name} is listening for 'echo' requests...`);
    conns.push(nc);
  }
  return conns;
}

const sc = StringCodec();

// simple handler for service requests
async function handleRequest(name: string, s: Subscription) {
  const p = 12 - name.length;
  const pad = "".padEnd(p);
  for await (const m of s) {
    // respond returns true if the message had a reply subject, thus it could respond
    if (m.respond(m.data)) {
      console.log(
        `[${name}]:${pad} #${s.getProcessed()} echoed ${sc.decode(m.data)}`,
      );
    } else {
      console.log(
        `[${name}]:${pad} #${s.getProcessed()} ignoring request - no reply subject`,
      );
    }
  }
}

// let's create two queue groups and a standalone subscriber
const conns: NatsConnection[] = [];
conns.push(...await createService("echo", 3, "echo"));
conns.push(...await createService("other-echo", 2, "other-echo"));
conns.push(...await createService("standalone"));

const a: Promise<void | Error>[] = [];
conns.forEach((c) => {
  a.push(c.closed());
});
await Promise.all(a);
```

## Advanced Usage

### Headers

New NATS servers offer the ability to add additional metadata to a message.
The metadata is added in the form of headers. NATS headers are very close to
HTTP headers. Note that headers are not guaranteed. If the client doesn't
want to support headers it will not receive them.

```typescript
import { connect, createInbox, Empty, headers } from "../../src/mod.ts";
import { nuid } from "../../nats-base-client/nuid.ts";

const nc = await connect(
  {
    servers: `demo.nats.io`,
    headers: true,
  },
);

const subj = createInbox();
const sub = nc.subscribe(subj);
(async () => {
  for await (const m of sub) {
    if (m.headers) {
      for (const [key, value] of m.headers) {
        console.log(`${key}=${value}`);
      }
      // reading/setting a header is not case sensitive
      console.log("id", m.headers.get("id"));
    }
  }
})().then();

// headers always have their names turned into a canonical mime header key
// header names can be any printable ASCII character with the  exception of `:`.
// header values can be any ASCII character except `\r` or `\n`.
// see https://www.ietf.org/rfc/rfc822.txt
const h = headers();
h.append("id", nuid.next());
h.append("unix_time", Date.now().toString());
nc.publish(subj, Empty, { headers: h });

await nc.flush();
await nc.close();

```


### No Responders

Requests can fail for many reasons. A common reason is when a request is made
to a subject that no service is listening on. Typically these surface as a
timeout error. With a nats-server that supports `headers` and `noResponders`,
the nats-server can report immediately if there is no interest on the request
subject:

```typescript
const nc = await connect({
    servers: `demo.nats.io`, noResponders: true, headers: true },
);

try {
  const m = await nc.request('hello.world');
  console.log(m.data);
} catch(err) {
  const nerr = err as NatsError
  switch (nerr.code) {
    case ErrorCode.NO_RESPONDERS:
      console.log("no one is listening to 'hello.world'")
      break;
    case ErrorCode.TIMEOUT:
      console.log("someone is listening but didn't respond")
      break;
    default:
      console.log("request failed", err)
  }
}

await nc.close();

```


### Authentication
```typescript
// if the connection requires authentication, provide `user` and `pass` or 
// `token` options in the NatsConnectionOptions
import { connect } from "src/mod.ts";

const nc1 = await connect({servers: "127.0.0.1:4222", user: "jenny", pass: "867-5309"});
const nc2 = await connect({port: 4222, token: "t0pS3cret!"});
```

#### Authenticators

For user/password and token authentication, you can simply provide them
as `ConnectionOptions` - see `user`, `pass`, `token`. Internally these
mechanisms are implemented as an `Authenticator`. An `Authenticator` is
simply a function that handles the type of authentication specified.

Setting the `user`/`pass` or `token` options, simply initializes an `Authenticator`
and sets the username/password. NKeys and JWT authentication are more complex,
as they cryptographically respond to a server challenge.

Because nkey and JWT authentication may require reading data from a file or
an HTTP cookie, these forms of authentication will require a bit more from
the developer to activate them. However, the work is related to accessing
these resources on the platform they are working with.

After the data is read, you can use one of these functions in your code to
generate the authenticator and assign it to the `authenticator` property of
the `ConnectionOptions`:

- `nkeyAuthenticator(seed?: Uint8Array | (() => Uint8Array)): Authenticator`
- `jwtAuthenticator(jwt: string | (() => string), seed?: Uint8Array | (()=> Uint8Array)): Authenticator`
- `credsAuthenticator(creds: Uint8Array): Authenticator`


The first two options also provide the ability to specify functions that return
the desired value. This enables dynamic environment such as a browser where
values accessed by fetching a value from a cookie.


Here's an example:

```javascript
  // read the creds file as necessary, in the case it
  // is part of the code for illustration purposes
  const creds = `-----BEGIN NATS USER JWT-----
    eyJ0eXAiOiJqdSDJB....
  ------END NATS USER JWT------

************************* IMPORTANT *************************
  NKEY Seed printed below can be used sign and prove identity.
  NKEYs are sensitive and should be treated as secrets.

  -----BEGIN USER NKEY SEED-----
    SUAIBDPBAUTW....
  ------END USER NKEY SEED------
`;

  const nc = await connect(
    {
      port: 4222,
      authenticator: credsAuthenticator(new TextEncoder().encode(creds)),
    },
  );
```

### Flush
```javascript
// flush sends a PING protocol request to the servers and returns a promise
// when the servers responds with a PONG. The flush guarantees that
// things you published have been delivered to the server. Typically
// it is not necessary to use flush, but on tests it can be invaluable.
nc.publish('foo');
nc.publish('bar');
await nc.flush();
```

### `PublishOptions`

When you publish a message you can specify some options:

- `reply` - this is a subject to receive a reply (you must setup a subscription) before you publish.
- `headers` - a set of headers to decorate the message.

### `SubscriptionOptions`
You can specify several options when creating a subscription:
- `max`: maximum number of messages to receive - auto unsubscribe
- `timeout`: how long to wait for the first message
- `queue`: the [queue group](#Queue-Groups) name the subscriber belongs to
- `callback`: a function with the signature `(err: NatsError|null, msg: Msg) => void;` that should be used for handling the message. Subscriptions with callbacks are NOT iterators.

#### Auto Unsubscribe
```javascript
// subscriptions can auto unsubscribe after a certain number of messages
nc.subscribe('foo', { max: 10 });
```

#### Timeout Subscriptions
```javascript
// create subscription with a timeout, if no message arrives
// within the timeout, the function running the iterator with
// reject - depending on how you code it, you may need a
// try/catch block.
const sub = nc.subscribe("hello", { timeout: 1000 });
(async () => {
  for await (const m of sub) {
  }
})().catch((err) => {
  if (err.code === ErrorCode.TIMEOUT) {
    console.log(`sub timed out!`);
  } else {
    console.log(`sub iterator got an error!`);
  }
});
```

### `RequestOptions`

When making a request, there are several options you can pass:

- `timeout`: how long to wait for the response
- `headers`: optional headers to include with the message
- `noMux`: create a new subscription to handle the request. Normally a shared subscription is used to receive request messages.
- `reply`: optional subject where the reply should be received

#### `noMux` and `reply`

Under the hood the request API simply uses a wildcard subscription
to handle all requests you send.

In some cases the default subscription strategy doesn't work correctly.
For example the client may be constrained by the subjects from which it can
receive replies.

When `noMux` is set to `true`, the client will create a normal subscription for
receiving the response to a generated inbox subject. The `reply` option can
be used to override the generated inbox subject with an application
provided one. Note that setting `reply` requires `noMux` to be `true`:

```typescript
  const m = await nc.request(
    "q",
    Empty,
    { reply: "bar", noMux: true, timeout: 1000 },
  );
```

### Draining Connections and Subscriptions

Draining provides for a graceful way to unsubscribe or 
close a connection without losing messages that have 
already been dispatched to the client.

You can drain a subscription or all subscriptions in a connection.

When you drain a subscription, the client sends an `unsubscribe`
protocol message to the server followed by a `flush`. The
subscription handler is only removed when the server responds
with a pong. Thus by that time all pending messages for the
subscription have been processed by the client.

Draining a connection, drains all subscriptions. However
when you drain the connection it is impossible to make
new subscriptions or send new requests. After the last
subscription is drained it also becomes impossible to publish
a message. These restrictions do not exist when just draining
a subscription.


### Lifecycle/Informational Events
Clients can get notification on various event types:
- `Events.DISCONNECT`
- `Events.RECONNECT`
- `Events.UPDATE`
- `Events.LDM`

The first two fire when a client disconnects and reconnects respectively.
The payload will be the server where the event took place.

The `UPDATE` event notifies whenever the client receives a cluster configuration
update. The `ServersChanged` interface provides two arrays: `added` and `deleted`
listing the servers that were added or removed. 

The `LDM` event notifies that the current server has signaled that it
is running in _Lame Duck Mode_ and will evict clients. Depending on the server
configuration policy, the client may want to initiate an ordered shutdown, and
initiate a new connection to a different server in the cluster.

```javascript
const nc = await connect(opts);
(async () => {
  console.info(`connected ${nc.getServer()}`);
  for await (const s of nc.status()) {
    console.info(`${s.type}: ${s.data}`);
  }
})().then();

```

Be aware that when a client closes, you will need to wait for the `closed()` promise to resolve.
When it resolves, the client has finished and won't reconnect.


## Connection Options

The following is the list of connection options and default values.

| Option                 | Default                   | Description
|--------                |---------                  |------------
| `authenticator`        | none                      | Specifies the authenticator function that sets the client credentials.
| `debug`                | `false`                   | If `true`, the client prints protocol interactions to the console. Useful for debugging. 
| `headers`              | `false`                   | Client requires header support on the server.
| `maxPingOut`           | `2`                       | Max number of pings the client will allow unanswered before raising a stale connection error.
| `maxReconnectAttempts` | `10`                      | Sets the maximum number of reconnect attempts. The value of `-1` specifies no limit.
| `name`                 |                           | Optional client name - recommended to be set to a unique client name.
| `noEcho`               | `false`                   | Subscriptions receive messages published by the client. Requires server support (1.2.0). If set to true, and the server does not support the feature, an error with code `NO_ECHO_NOT_SUPPORTED` is emitted, and the connection is aborted. Note that it is possible for this error to be emitted on reconnect when the server reconnects to a server that does not support the feature.
| `noRandomize`          | `false`                   | If set, the order of user-specified servers is randomized.
| `noResponders`         | `false`                   | Requires `headers`. Fail immediately if there are no subscribers for a request.
| `pass`                 |                           | Sets the password for a connection.
| `pedantic`             | `false`                   | Turns on strict subject format checks.
| `pingInterval`         | `120000`                  | Number of milliseconds between client-sent pings.
| `port`                 | `4222`                    | Port to connect to (only used if `servers` is not specified).
| `reconnectTimeWait`    | `2000`                    | If disconnected, the client will wait the specified number of milliseconds between reconnect attempts.
| `reconnectJitter`      | `100`                     | Number of millis to randomize after `reconnectTimeWait`.
| `reconnectJitterTLS`   | `1000`                    | Number of millis to randomize after `reconnectTimeWait` when TLS options are specified.
| `reconnectDelayHandler`| Generated function        | A function that returns the number of millis to wait before the next connection to a server it connected to `()=>number`.
| `reconnect`            | `true`                    | If false, client will not attempt reconnecting.
| `servers`              | `"localhost:4222"`        | String or Array of hostport for servers.
| `timeout`              | 20000                     | Number of milliseconds the client will wait for a connection to be established. If it fails it will emit a `connection_timeout` event with a NatsError that provides the hostport of the server where the connection was attempted.
| `tls`                  | TlsOptions                | A configuration object for requiring a TLS connection (not applicable to nats.ws).
| `token`                |                           | Sets a authorization token for a connection.
| `user`                 |                           | Sets the username for a connection.
| `verbose`              | `false`                   | Turns on `+OK` protocol acknowledgements.
| `waitOnFirstConnect`   | `false`                   | If `true` the client will fall back to a reconnect mode if it fails its first connection attempt.
| `ignoreClusterUpdates` | `false`                   | If `true` the client will ignore any cluster updates provided by the server.


### TlsOptions

| Option       | Default | Description
|  `caFile`    |         | CA certificate filepath
|  `certFile`  |         | Client certificate file path - not applicable to Deno clients.
|  `keyFile`   |         | Client key file path - not applicable to Deno clients.

In some Node and Deno clients, having the option set to an empty option, requires the client have a secured connection.


### Jitter

The settings `reconnectTimeWait`, `reconnectJitter`, `reconnectJitterTLS`, `reconnectDelayHandler` are all related.
They control how long before the NATS client attempts to reconnect to a server it has previously connected.

The intention of the settings is to spread out the number of clients attempting to reconnect to a server over a period of time,
and thus preventing a ["Thundering Herd"](https://docs.nats.io/developing-with-nats/reconnect/random).

The relationship between these is:

- If `reconnectDelayHandler` is specified, the client will wait the value returned by this function. No other value will be taken into account.
- If the client specified TLS options, the client will generate a number between 0 and `reconnectJitterTLS` and add it to
  `reconnectTimeWait`.
- If the client didn't specify TLS options, the client will generate a number between 0 and `reconnectJitter` and add it to `reconnectTimeWait`.

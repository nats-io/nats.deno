# NATS.deno - A [NATS](http://nats.io) client for [Deno](https://deno.land)


A Deno client for the [NATS messaging system](https://nats.io).

[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](./LICENSE)
![Test NATS.deno](https://github.com/nats-io/nats.deno/workflows/NATS.deno/badge.svg)


# Installation

** :warning: NATS.deno is a preview** you can get the current development version by:

```bash
import * as nats from 'https://raw.githubusercontent.com/nats-io/nats.deno/master/src/mod.ts'
```
The Deno client is under active development. All tests are passing, but the APIs will change
slightly - See the [TODO](TODO.md).



## Basics


### Connecting to a nats-server

To connect to a server you use the `connect()` function. It returns
a connection that you can use to interact with the server.

By default, a connection will attempt to auto-reconnect when dropped
due to some networking type error. Messages that have not been
sent to the server when a disconnect happens are lost. A client can queue
new messages to be sent when the connection resumes. If the connection
cannot be re-established the client will give up and `close` the connection.

To learn when a connection closes, wait for the promise returned by the `closed()`
function. If the close was due to an error, it will resolve to an error.

To disconnect from the nats-server, you call `close()` on the connection.
Connections can also be closed if there's an error. For example, the
server returns some run-time error.

This first example looks a bit complex, because it shows all the things
discussed above. The example attempts to connect a nats-server by specifying
different connect options. At least two of them should work if your
internet is working.

```typescript
// import the connect function
import { connect, NatsConnection } from "src/mod.ts";

// the connection is configured by a set of ConnectionOptions
// if none is set, the client will connect to 127.0.0.1:4222.
// common options that you could pass look like:
const localhostAtStandardPort = {};
const localPort = { port: 4222 };
const hostAtStdPort = { url: "demo.nats.io" };
const hostPort = { url: "demo.nats.io:4222" };

// let's try to connect to all the above, some may fail
const dials: Promise<NatsConnection>[] = [];
[localhostAtStandardPort, localPort, hostAtStdPort, hostPort].forEach((v) => {
  dials.push(connect(v));
});

const conns: NatsConnection[] = [];
// wait until all the dialed connections resolve or fail
// allSettled returns a tupple with `closed` and `value`:
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
The basic client operations are to `subscribe` to receive messages,
and publish to `send` messages. A subscription works as an async
iterator where you process messages in a loop until the subscription
closes.

```typescript
import { connect } from "src/mod.ts";
const nc = await connect({ url: "demo.nats.io:4222" });

// create a simple subscriber and iterate over messages
// matching the subscription
const sub = nc.subscribe("hello");
(async () => {
  for await (const m of sub) {
    console.log(`[${sub.getProcessed()}]: ${m.data}`);
  }
  console.log("subscription closed");
})();

nc.publish("hello", "world");
nc.publish("hello", "again");

// we want to insure that messages that are in flight
// get processed, so we are going to drain the
// connection. Drain is the same as close, but makes
// sure that all messages in flight get seen
// by the iterator. After calling drain on the connection
// the connection closes.
await nc.drain();
```

### Streams
Streams are messages that are published at regular intervals.
To get the messages, you simply subscribe to them. To stop
getting the messages you unsubscribe.

```typescript
import { connect } from "src/mod.ts";
const nc = await connect({ url: "demo.nats.io" });

console.info("enter the following command to get messages from the stream");
console.info(
  "deno run --allow-all --unstable examples/nats-sub.ts stream.demo",
);

const start = Date.now();
let sequence = 0;
setInterval(() => {
  sequence++;
  const uptime = Date.now() - start;
  console.info(`publishing #${sequence}`)
  nc.publish("stream.demo", JSON.stringify({ sequence, uptime }));
}, 1000);
```

### Wildcard Subscriptions
Sometimes you want to process an event (message), based on the
subject that was used to send it. In NATS this is accomplished
by specifying wildcards in the subscription. Subjects that match
the wildcards, are sent to the client.

In the example below, I am using 3 different subscription
to highlight that each subscription is independent. And if
the subject you use matches one or more of them, they all
will get a chance at processing the message.

```javascript
import { connect, Subscription } from "src/mod.ts";
const nc = await connect({ url: "demo.nats.io:4222" });

// subscriptions can have wildcard subjects
// the '*' matches any string in the specified token position
const s1 = nc.subscribe("help.*.system");
const s2 = nc.subscribe("help.me.*");
// the '>' matches any tokens in that position or following
// '>' can only be specified at the end
const s3 = nc.subscribe("help.>");

async function printMsgs(s: Subscription) {
  console.log(`listening for ${s.subject}`);
  const c = (13 - s.subject.length);
  const pad = "".padEnd(c);
  for await (const m of s) {
    console.log(
      `[${s.subject}]${pad} #${s.getProcessed()} - ${m.subject} ${
        m.data ? " " + m.data : ""
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


### Services
A service is a client that responds to requests from other clients.
Now that you know how to create subscriptions, and know about wildcards,
it is time to develop a service that mocks something useful.

This example is a bit complicated, because we are going to use NATS
not only to provide a service, but also to control the service.

```typescript
import { connect, Subscription } from "src/mod.ts";
const nc = await connect({ url: "demo.nats.io" });

// A service is a subscriber that listens for requests
// for the current time and responds
const started = Date.now();
const sub = nc.subscribe("time");
// this function will handle requests for time
requestHandler(sub);

// If you wanted to manage a service - well NATS is awesome
// for just that - setup another subscription where admin
// messages can be sent
const msub = nc.subscribe("admin.*");
// this function will handle management requests
adminHandler(msub);

// wait for the client to close here.
await nc.closed().then((err?: void | Error) => {
  let m = `connection to ${nc.getServer()} closed`;
  if (err) {
    m = `${m} with an error: ${err.message}`;
  }
  console.log(m);
});

// this implements the handler for time requests
async function requestHandler(sub: Subscription) {
  console.log(`listening for ${sub.subject} requests...`);
  let serviced = 0;
  for await (const m of sub) {
    serviced++;
    if (m.respond(new Date().toISOString())) {
      console.info(
        `[${serviced}] handled ${m.data ? "- " + m.data : ""}`,
      );
    } else {
      console.log(`[${serviced}] ignored - no reply subject`);
    }
  }
}

// this implements the admin service, I use wildcards to handle
// the requests consicely here
async function adminHandler(sub: Subscription) {
  console.log(`listening for ${sub.subject} requests [uptime | stop]`);
  // it would be very good to verify the origin of the
  // request - before implementing something that allows your service
  // to be managed.
  for await (const m of sub) {
    const chunks = m.subject.split(".");
    console.info(`[admin] handling ${chunks[1]}`);
    switch (chunks[1]) {
      case "uptime":
        // send the number of millis since the service started
        const uptime = Date.now() - started
        m.respond(JSON.stringify({uptime}));
        break;
      case "stop":
        m.respond("stopping....");
        // finish requests by draining the subscription
        await sub.drain();
        // close the connection
        const _ = nc.close();
        break;
      default:
        console.log(`ignoring request`);
    }
  }
}
```

### Making Requests
```typescript
import { connect } from "src/mod.ts";
const nc = await connect({ url: "demo.nats.io:4222" });

// a client makes a request and receives a promise for a message
// by default the request times out after 1s (1000 millis) and has
// no payload.
await nc.request("time", "hello!", { timeout: 1000 })
  .then((m) => {
    console.log(`got response: ${m.data}`);
  })
  .catch((err) => {
    console.log(`problem with request: ${err.message}`);
  });

await nc.close();
```

### Queue Groups
Queue groups allow scaling of services horizontally. Subscriptions for members of a 
queue group are treated as a single service, that means when you send a message
only a single client in a queue group will receive it. There can be multiple queue 
groups, and each is treated as an independent group. Non-queue subscriptions are
also independent.
```typescript
import { connect, NatsConnection, Subscription } from "src/mod.ts";

async function createService(
  name: string,
  count: number = 1,
  queue: string = "",
): Promise<NatsConnection[]> {
  const conns: NatsConnection[] = [];
  for (let i = 1; i <= count; i++) {
    const n = queue ? `${name}-${i}` : name;
    const nc = await connect(
      { url: "demo.nats.io:4222", name: `${n}` },
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

// simple handler for service requests
async function handleRequest(name: string, s: Subscription) {
  const p = 12 - name.length;
  const pad = "".padEnd(p);
  for await (const m of s) {
    // respond returns true if the message had a reply subject, thus it could respond
    if (m.respond(m.data)) {
      console.log(`[${name}]:${pad} #${s.getProcessed()} echoed ${m.data}`);
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

### Authentication
```typescript
// if the connection requires authentication, provide `user` and `pass` or 
// `token` options in the NatsConnectionOptions
import { connect } from "src/mod.ts";

const nc1 = await connect({url: "nats://127.0.0.1:4222", user: "jenny", pass: "867-5309"});
const nc2 = await connect({port: 4222, token: "t0pS3cret!"});
```

### Flush
```javascript
// flush sends a PING request to the server and returns a promise
// when the server responds with a PONG. The flush guarantees that
// things you published have been delivered to the server. Typically
// it is not necessary to use flush, but on tests it can be invaluable.
nc.publish('foo');
nc.publish('bar');
await nc.flush();
```

### Auto Unsubscribe
```javascript
// subscriptions can auto unsubscribe after a certain number of messages
nc.subscribe('foo', {max:10});
```

### Timeout Subscriptions
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

To be aware of when a client closes, wait for the `closed()` promise to resolve.
When it resolves, the client has finished and won't reconnect.

# NATS.deno - A [NATS](http://nats.io) client for [Deno](https://deno.land)


A Deno client for the [NATS messaging system](https://nats.io).

[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](./LICENSE)
![Test NATS.deno](https://github.com/nats-io/nats.deno/workflows/NATS.deno/badge.svg)


# Installation

** :warning: NATS.deno is a preview** you can get the current development version by:

```bash
import * as nats from 'https://raw.githubusercontent.com/nats-io/nats.deno/master/src/mod.ts'
```

# Basic Usage

The Deno client is under active development. All tests are passing, but the APIs will change
slightly - See the [TODO](TODO.md).

```javascript
// import the connect function
  import { connect } from "https://deno.land/x/nats/src/mod.ts";
  
// create a connection
  const nc = await connect({ url: 'nats://127.0.0.1:4222', payload: Payload.STRING });

  // simple publisher
  nc.publish('hello', 'nats');

  // simple subscriber, if the message has a reply subject
  // send a reply
  const sub = nc.subscribe('help', (err, msg) => {
    if (err) {
      // handle error
    }
    else if (msg.reply) {
      nc.publish(msg.reply, `I can help ${msg.data}`);
    }
  })

  // unsubscribe
  sub.unsubscribe();

  // request data - requests only receive one message
  // to receive multiple messages, create a subscription
  const msg = await nc.request('help', 1000, 'nats request');
  console.log(msg.data);

  // publishing a request, is similar to publishing. You must have
  // a subscription ready to handle the reply subject. Requests
  // sent this way can get multiple replies
  nc.publish('help', '', 'replysubject');


  // close the connection
  await nc.close();
```

## Wildcard Subscriptions
```javascript
// the `*` matches any string in the subject token
const sub = nc.subscribe('help.*.system', (_, msg) => {
    if (msg.reply) {
        nc.publish(msg.reply, `I can help ${msg.data}`)
    }
});
sub.unsubscribe();

const sub2 = nc.subscribe('help.me.*', (_, msg) => {
  if (msg.reply) {
    nc.publish(msg.reply, `I can help ${msg.data}`);
  }
})

// the `>` matches any tokens, can only be at the last token
const sub3 = nc.subscribe('help.>', (_, msg) => {
  if (msg.reply) {
    nc.publish(msg.reply, `I can help ${msg.data}`);
  }
})
```

## Queue Groups
```javascript
// All subscriptions with the same queue name form a queue group.
// The server will select a single subscriber in each queue group
// matching the subscription to receive the message.
const qsub = nc.subscribe('urgent.help', (_, msg) => {
  if (msg.reply) {
     nc.publish(msg.reply, `I can help ${msg.data}`);
  }
}, {queue: "urgent"});
```

## Authentication
```javascript
// if the connection requires authentication, provide `user` and `pass` or 
// `token` options in the NatsConnectionOptions

const nc1 = await connect({url: "nats://127.0.0.1:4222", user: "me", pass: "secret"});
const nc2 = await connect({url: "localhost:8080", user: "jenny", token: "867-5309"});
const nc3 = await connect({port: 4222, token: "t0pS3cret!"});
```

## Advanced Usage

### Flush
```javascript
// flush does a round trip to the server. When it
// returns the the server saw it
await nc.flush();

// or publish a few things, and wait for the flush
nc.publish('foo');
nc.publish('bar');
await nc.flush();
```

### Auto unsubscribe
```javascript
// subscriptions can auto unsubscribe after a certain number of messages
const sub = nc.subscribe('foo', ()=> {}, {max:10})

// the number can be changed or set afterwards
// if the number is less than the number of received
// messages it cancels immediately
let next = sub.getReceived() + 1
sub.unsubscribe(next)
```

### Timeout Subscriptions
```javascript
// subscriptions can specify attach a timeout
// timeout will clear with first message
const sub = nc.subscribe('foo', ()=> {})
sub.setTimeout(300, ()=> {
  console.log('no messages received')
})

// if 10 messages are not received, timeout fires
const sub = nc.subscribe('foo', ()=> {}, {max:10})
sub.setTimeout(300, ()=> {
    console.log(`got ${sub.getReceived()} messages. Was expecting 10`)
})

// timeout can be cancelled
sub.clearTimeout()
```

### Error Handling
```javascript
// Any async error results in the client being closed.
// You can track the status of the client by registering
// for a promise that will return when the client closes:
nc.status().then((v) => {
  if(v) {
    console.log(`client closed with an error ${err.message}`);
  } else {
    console.log('client closed');
  }
});

```

### Lifecycle/Informational Events
```javascript
// Clients can get notification on various event types:
  nc.addEventListener(Events.RECONNECT, () => {
    console.info("reconnected!!!")
  });
  nc.addEventListener(Events.DISCONNECT, () => {
    console.error("disconnected!!!")
  });
  nc.addEventListener(Events.UPDATE, (evt) => {
    // ServersChanged is a { added: string[], deleted: string[] }
    const v = evt.detail;
    console.log("servers changed");
    console.table(v);
  });

```


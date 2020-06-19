# NATS.deno - A [NATS](http://nats.io) client for [Deno](https://deno.land)


A Deno client for the [NATS messaging system](https://nats.io).

[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](./LICENSE.txt)
![Test NATS.deno](https://github.com/nats-io/nats.deno/workflows/NATS.deno/badge.svg)


# Installation

** :warning: NATS.deno is a preview** you can get the current development version by:

```bash
import * as nats from 'https://raw.githubusercontent.com/nats-io/nats.deno/master/src/mod.ts'
```

# Basic Usage
nats.deno supports Promises, depending on the runtime environment you can also use async-await constructs.



In another script block, reference the 'nats' global:
```javascript
// create a connection
  const nc = await nats.connect({ url: 'nats://localhost:4222', payload: nats.Payload.STRING });

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
  nc.close();

}
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
// if the connection requires authentication, 
// provide it in the URL. NATS credentials are specified
// in the `user`, `pass` or `token` options in the NatsConnectionOptions

const nc = nats.connect({url: "nats://wsuser:wsuserpass@localhost:4222" });
const nc1 = nats.connect({url: "nats://localhost:4222", user: "me", pass: "secret"});
const nc2 = nats.connect({url: "nats://localhost:8080", user: "jenny", token: "867-5309"});
const nc3 = nats.connect({url: "nats://localhost:8080", token: "t0pS3cret!"});
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
// when server returns an error, you are notified asynchronously
nc.addEventListener('error', (ex)=> {
  console.log('server sent error: ', ex)
});

// when disconnected from the server, the 'close' event is sent
nc.addEventListener('close', ()=> {
  console.log('the connection closed')
})
```


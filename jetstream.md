# JetStream

JetStream is the NATS persistence engine providing streaming, message, and
worker queues with At-Least-Once semantics. JetStream stores messages in
_streams_. A stream defines how messages are stored and how long they persist.
To store a message in JetStream, you simply need to publish to a subject that is
associated with a stream.

Messages are replayed from a stream by _consumers_. A consumer configuration
specifies which messages should be presented. For example a consumer may only be
interested in viewing messages from a specific sequence or since a specific
time. The configuration also specifies if the server should require for a
message to be acknowledged, and how long to wait for acknowledgements. The
consumer configuration also specifies options to control the rate at which
messages are presented to the client.

For more information about JetStream, please visit the
[JetStream repo](https://github.com/nats-io/jetstream).

## JetStream client for JavaScript

The new generation of Javascript clients:

- [nats.js](https://github.com/nats-io/nats.js)
- [nats.ws](https://github.com/nats-io/nats.ws)
- [nats.deno](https://github.com/nats-io/nats.deno)

all support JetStream, however the functionality is still on _beta_, and the
APIs are subject to change. Please report any issues you find.

## JetStreamManager

The client provides CRUD functionality to manage streams and consumers, via
JetStreamManager. To create a JetStream manager:

```typescript
const jsm = await nc.jetstreamManager();

// list all the streams, the `next()` function
// retrieves a paged result.
const streams = await jsm.streams.list().next();

// add a stream
const stream = "mystream";
const subj = `mystream.A`;
await jsm.streams.add({ name: stream, subjects: [subj] });

// find a stream that stores a specific subject:
const name = await jsm.find("mystream.A");

// retrieve info about the stream by its name
const si = await jsm.streams.info(name);

// update a stream configuration
si.subjects.push("mystream.B");
await jsm.streams.update(si);

// get a particular stored message in the stream by sequence
// this is not associated with a consumer
let sm = await jsm.streams.getMessage(stream, 1);

// delete the 5th message in the stream, securely erasing it
await jsm.streams.deleteMessage(stream, 5, true);

// purge all messages in the stream, the stream itself
// remains.
await jsm.streams.purge(stream);

// list all consumers for a stream:
const consumers = await jsm.consumers.list(stream).next();

// add a new durable pull consumer
await jsm.consumers.add(stream, {
  durable_name: "me",
  ack_policy: AckPolicy.Explicit,
});

// retrieve a consumer's configuration
const ci = await jsm.consumers.info(stream, "me");

// delete a particular consumer
await jsm.consumers.delete(stream, "me");
```

## JetStream Client

The JetStream client functionality presents a JetStream view on a NATS client.
While you can implement your own API to interact with streams, the JetStream
APIs make this convenient.

```typescript
// create a jetstream client:
const js = nc.jetstream();

// to publish messages to a stream:
const pa = await js.publish("a.b");
// the jetstream returns an acknowledgement with the
// stream that captured the message, it's assigned sequence
// and whether the message is a duplicate.
const stream = pa.stream;
const seq = pa.seq;
const duplicate = pa.duplicate;

// More interesting is the ability to prevent duplicates
// on messages that are stored in the server. If
// you assign a message ID, the server will keep looking
// for the same ID for a configured amount of time, and
// reject messages that sport the same ID:
await js.publish("a.b", Empty, { msgID: "a" });

// you can also specify constraints that should be satisfied.
// For example, you can request the message to have as its
// last sequence before accepting the new message:
await js.publish("a.b", Empty, { expect: { lastMsgID: "a" } });
await js.publish("a.b", Empty, { expect: { lastSequence: 2 } });
await js.publish("a.b", Empty, { expect: { streamName: "a" } });
// you can also mix the above combinations
```

### Processing Messages

Messages are processed by subscriptions to the stream. The JavaScript client
provides several ways a consumer can read its messages from a stream.

#### Requesting a single message

The simplest mechanism is to request a single message. This requires a round
trip to the server, but it is very simple to use. If no messages are available,
the request fails. This API requires the consumer to already exist.

```typescript
let msg = await js.pull(stream, durableName);
msg.ack();
```

#### Requesting a batch of messages

You can also request more than one message at time. The request is a _long_
poll. So it remains open until the desired number of messages is received, or
the expiration time triggers.

```typescript
// To get multiple messages in one request you can:
let msgs = await js.pullBatch(stream, durable, { batch: 10, expires: 5000 });
// the request returns an iterator that will get at most 10 seconds or wait
// for 5000ms for messages to arrive.

const done = (async () => {
  for await (const m of iter) {
    // do something with the message
    // and if the consumer is not set to auto-ack, ack!
    m.ack();
  }
})();
// The iterator completed
await done;
```

### Push Subscriptions

A push subscription is similar to a standard NATS subscription. The consumer
configuration registers for messages to be delivered to a specific subject. When
a client subscribes to that subject, JetStream starts presenting messages.

The jetstream `subscribe()` provides the syntactic sugar to auto-create the
consumer for you if it doesn't exist by matching the stream to the requested
subject. It also presents messages to you as a JsMsg. To stop getting messages,
`unsubscribe()`. The consumer will be preserved, so the client can resume at a
later time. To destroy the consumer, call `destroy()`. This will stop the
subscription and destroy the consumer.

```typescript
const opts = consumerOpts();
opts.durable("me");
opts.manualAck();
opts.ackExplicit();
opts.deliverTo(createInbox());

let sub = await js.subscribe(subj, opts);
const done = (async () => {
  for await (const m of sub) {
    m.ack();
  }
})();

// when done (by some logic not shown here), you can delete
// the consumer by simply calling `destroy()`. Destroying
// a consumer removes all its state information.
sub.destroy();
```

### Pull Subscriptions

Pull subscriber API is similar to the _push subscriber_, but messages will be
delivered by `pull()`. The subscription remains open, but the client has to
periodically pull for messages. This allows the subscriber to determine when to
ask for messages and only for those messages it can handle during its pull
interval.

```typescript
const psub = await js.pullSubscribe(subj, { config: { durable_name: "c" } });
const done = (async () => {
  for await (const m of psub) {
    console.log(`${m.info.stream}[${m.seq}]`);
    m.ack();
  }
})();
psub.unsubscribe(4);

// To start receiving messages you pull the subscription
setInterval(() => {
  psub.pull({ batch: 10, expires: 10000 });
}, 10000);
```

Note the above example is contrived, as the pull interval is fixed based on some
interval.

### JsMsg

A `JsMsg` is a wrapped `Msg` - it has all the standard fields in a `Msg`, a
`JsMsg` and provides functionality for inspecting metadata encoded into the
message's reply subject. This metadata includes:

- sequence (`seq`)
- `redelivered`
- full info via info which shows the delivery sequence, and how many messages
  are pending among other things.
- Multiple ways to acknowledge a message:
  - `ack()`
  - `nak()` - like ack, but tells the server you failed to process it, and it
    should be resent.
  - `working()` - informs the server that you are still working on the message
    and thus prevent receiving the message again as a redelivery.
  - `term()` - specifies that you failed to process the server and instructs the
    server to not send it againn (to any consumer).

If you implement a standard nats subscription to process your JetStream
messages, you can use `toJsMsg()` to convert a message. Note that subscriptions
processing stream data can contain nats Msgs that are not convertible to
`JsMsg`.

### Callbacks

JetStream `subscribe()` and `pullSubscribe()` normally have iterators associated
with them, but you can also specify to handle the message in a callback. Unlike
the standard message callaback `(err: NatsError|null, msg: Msg)=>void`,
JetStream callbacks look like this:
`(err: NatsError | null, msg: JsMsg | null) => void`

As you can see, it is possible for your callback to get `null` for both the
error and the message argument. The reason for this, is that JetStream sends
standard nats `Msg` that contains headers to inform the client of various
things. When using iterators, any control message is handled, so your iterator
won't see them. When using callbacks this is not possible. Protocol messages
will be handled behind the scenes, but your callback will still be invoked.

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

all support JetStream. Please report any issues you find.

## JetStreamManager

The client provides CRUD functionality to manage streams and consumers, via
JetStreamManager. To create a JetStream manager:

```typescript
const jsm = await nc.jetstreamManager();

// list all the streams, the `next()` function
// retrieves a paged result.
const streams = await jsm.streams.list().next();
streams.forEach((si) => {
  console.log(si);
});

// add a stream
const stream = "mystream";
const subj = `mystream.*`;
await jsm.streams.add({ name: stream, subjects: [subj] });

// publish a reg nats message directly to the stream
for (let i = 0; i < 100; i++) {
  nc.publish(`${subj}.a`, Empty);
}

// find a stream that stores a specific subject:
const name = await jsm.streams.find("mystream.A");

// retrieve info about the stream by its name
const si = await jsm.streams.info(name);

// update a stream configuration
si.config.subjects?.push("a.b");
await jsm.streams.update(si.config);

// get a particular stored message in the stream by sequence
// this is not associated with a consumer
const sm = await jsm.streams.getMessage(stream, { seq: 1 });
console.log(sm.seq);

// delete the 5th message in the stream, securely erasing it
await jsm.streams.deleteMessage(stream, 5);

// purge all messages in the stream, the stream itself remains.
await jsm.streams.purge(stream);

// purge all messages with a specific subject (filter can be a wildcard)
await jsm.streams.purge(stream, { filter: "a.b" });

// purge messages with a specific subject keeping some messages
await jsm.streams.purge(stream, { filter: "a.c", keep: 5 });

// purge all messages with upto (not including seq)
await jsm.streams.purge(stream, { seq: 90 });

// purge all messages with upto sequence that have a matching subject
await jsm.streams.purge(stream, { filter: "a.d", seq: 100 });

// list all consumers for a stream:
const consumers = await jsm.consumers.list(stream).next();
consumers.forEach((ci) => {
  console.log(ci);
});

// add a new durable pull consumer
await jsm.consumers.add(stream, {
  durable_name: "me",
  ack_policy: AckPolicy.Explicit,
});

// retrieve a consumer's configuration
const ci = await jsm.consumers.info(stream, "me");
console.log(ci);

// delete a particular consumer
await jsm.consumers.delete(stream, "me");
```

## JetStream Client

The JetStream client functionality presents a JetStream view on a NATS client.
While you can implement your own API to interact with streams, the JetStream
APIs make this convenient.

```typescript
const jsm = await nc.jetstreamManager();
await jsm.streams.add({ name: "a", subjects: ["a.*"] });

// create a jetstream client:
const js = nc.jetstream();

// to publish messages to a stream:
let pa = await js.publish("a.b");
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
await js.publish("a.b", Empty, { expect: { lastSequence: 3 } });
// save the last sequence for this publish
pa = await js.publish("a.b", Empty, { expect: { streamName: "a" } });
// you can also mix the above combinations

// this stream here accepts wildcards, you can assert that the
// last message sequence recorded on a particular subject matches:
const buf: Promise<PubAck>[] = [];
for (let i = 0; i < 100; i++) {
  buf.push(js.publish("a.a", Empty));
}
await Promise.all(buf);
// if additional "a.b" has been recorded, this will fail
await js.publish("a.b", Empty, { expect: { lastSubjectSequence: pa.seq } });
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

#### Fetching batch of messages

You can also request more than one message at time. The request is a _long_
poll. So it remains open until the desired number of messages is received, or
the expiration time triggers.

```typescript
// To get multiple messages in one request you can:
let msgs = await js.fetch(stream, durable, { batch: 10, expires: 5000 });
// the request returns an iterator that will get at most 10 messages or wait
// for 5000ms for messages to arrive.

const done = (async () => {
  for await (const m of msgs) {
    // do something with the message
    // and if the consumer is not set to auto-ack, ack!
    m.ack();
  }
})();
// The iterator completed
await done;
```

#### Push Subscriptions

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

#### Pull Subscriptions

Pull subscriber API is similar to the _push subscriber_, but messages will be
delivered by `pull()`. The subscription remains open, but the client has to
periodically pull for messages. This allows the subscriber to determine when to
ask for messages and only for those messages it can handle during its pull
interval.

```typescript
const psub = await js.pullSubscribe(subj, {
  mack: true,
  // artificially low ack_wait, to show some messages
  // not getting acked being redelivered
  config: {
    durable_name: durable,
    ack_policy: AckPolicy.Explicit,
    ack_wait: nanos(4000),
  },
});

(async () => {
  for await (const m of psub) {
    console.log(
      `[${m.seq}] ${
        m.redelivered ? `- redelivery ${m.info.redeliveryCount}` : ""
      }`,
    );
    if (m.seq % 2 === 0) {
      m.ack();
    }
  }
})();

const fn = () => {
  console.log("[PULL]");
  psub.pull({ batch: 1000, expires: 10000 });
};

// do the initial pull
fn();
// and now schedule a pull every so often
const interval = setInterval(fn, 10000); // and repeat every 2s
```

Note the above example is contrived, as the pull interval is fixed based on some
interval.

#### Consumer Binding

JetStream's `subscribe()`, and `pullSubscribe()` can `bind` to a specific
durable consumer. The consumer must already exist, note that if your consumer is
working on a stream that is sourced from another `bind` is the only way you can
attach to the correct consumer on the correct stream:

```typescript
const inbox = createInbox();
await jsm.consumers.add("A", {
  durable_name: "me",
  ack_policy: AckPolicy.None,
  deliver_subject: inbox,
});

const opts = consumerOpts();
opts.bind("A", "me");

const sub = await js.subscribe(subj, opts);
// process messages...
```

#### JetStream Queue Consumers

Queue Consumers allow scaling the processing of messages stored in a stream. To
create a Queue Consumer, you have to set its `deliver_group` property to the
name of the queue group (or use the `ConsumerOptsBuilder#queue()`). Then reuse
the consumer from the various subscriptions:

```typescript
const opts = consumerOpts();
opts.queue("q");
opts.durable("n");
opts.deliverTo("here");
opts.callback((_err, m) => {
  if (m) {
    m.ack();
  }
});

const sub = await js.subscribe(subj, opts);
const sub2 = await js.subscribe(subj, opts);
```

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
  - `nak(millis?)` - like ack, but tells the server you failed to process it,
    and it should be resent. If a number is specified, the message will be
    resent after the specified value. The additional argument only supported on
    server versions 2.7.1 or greater
  - `working()` - informs the server that you are still working on the message
    and thus prevent receiving the message again as a redelivery.
  - `term()` - specifies that you failed to process the message and instructs
    the server to not send it again (to any consumer).

If you implement a standard NATS subscription to process your JetStream
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
standard NATS `Msg` that contains headers to inform the client of various
things. When using iterators, any control message is handled, so your iterator
won't see them. When specifying callbacks to process your
`JetStreamSubscription` or `JetStreamPullSubscription`, this is not possible.
Protocol messages will be handled behind the scenes, but your callback will
still be invoked.

### Flow Control

JetStream added a new consumer option to control whether flow control messages
are dispatched to a client. Flow control messages enable JetStream to
dynamically attempt to determine an optimal delivery message rate for the
client. This feature can help prevent the slow consumer issues when the number
of messages in a stream are large.

You don't need to do anything special to make use of the flow control feature,
except for setting `flow_control` property on your consumer configuration.

If you are creating plain NATS subscriptions, you'll need to handle these flow
control messages yourself. To identify them, simply test to see if the message
has headers, and if the `code` of the header is equal to `100`. If it has a
reply subject, simply respond to it. The server will then adjust its delivery
rate as necessary. Note that there's also the handy function
`isFlowControlMsg(Msg)` that can perform this check for you. Note that
`msg.respond()` is a noop if there's no reply subject (in that case it is not a
flow control message but a heartbeat).

Here's a snippet:

```typescript
let data = 0;
let fc = 0;
// note this is a plan NATS subscription!
const sub = nc.subscribe("my.messages", {
  callback: (err, msg) => {
    // simply checking if has headers and code === 100
    if (isFlowControlMsg(msg)) {
      fc++;
      msg.respond();
      return;
    }
    // do something with the message
    data++;
    const m = toJsMsg(msg);
    m.ack();
    if (data === N) {
      console.log(`processed ${data} msgs and ${fc} flow control messages`);
      sub.drain();
    }
  },
});

// create a consumer that delivers to the subscription
await jsm.consumers.add(stream, {
  ack_policy: AckPolicy.Explicit,
  deliver_subject: "my.messages",
  flow_control: true,
});
```

### Heartbeats

Since JetStream is available through NATS it is possible for your network
topology to not be directly connected to the JetStream server providing you with
messages. In those cases, it is possible for a JetStream server to go away, and
for the client to not notice it. This would mean your client would sit idle
thinking there are no messages, when in reality the JetStream service may have
restarted elsewhere.

By creating a consumer that enables heartbeats, you can request JetStream to
send you heartbeat messages every so often. This way your client can reconcile
if the lack of messages means that you should be restarting your consumer.

Currently, the library doesn't provide a notification for missed heartbeats, but
this is not too difficult to do:

```typescript
let missed = 0;
// this is a plain nats subscription
const sub = nc.subscribe("my.messages", {
  callback: (err, msg) => {
    // if we got a message, we simply reset
    missed = 0;
    // simply checking if has headers and code === 100 and a description === "Idle Heartbeat"
    if (isHeartbeatMsg(msg)) {
      // the heartbeat has additional information:
      const lastSeq = msg.headers.get(JsHeaders.LastStreamSeqHdr);
      const consSeq = msg.headers.get(JsHeaders.LastConsumerSeqHdr);
      console.log(
        `alive - last stream seq: ${lastSeq} - last consumer seq: ${consSeq}`,
      );
      return;
    }
    // do something with the message
    const m = toJsMsg(msg);
    m.ack();
  },
});

setInterval(() => {
  missed++;
  if (missed > 3) {
    console.error("JetStream stopped sending heartbeats!");
  }
}, 30000);

// create a consumer that delivers to the subscription
await jsm.consumers.add(stream, {
  ack_policy: AckPolicy.Explicit,
  deliver_subject: "my.messages",
  idle_heartbeat: nanos(10000),
});

await sub.closed;
```

#### JetStream Ordered Consumers

An Ordered Consumers is a specialized push consumer that puts together flow
control, heartbeats, and additional logic to handle message gaps. Ordered
consumers cannot operate on a queue and cannot be durable.

As the ordered consumer processes messages, it enforces that messages are
presented to the client with the correct sequence. If a gap is detected, the
consumer is recreated at the expected sequence.

Most consumer options are rejected, as the ordered consumer manages its
configuration in a very specific way.

To create an ordered consumer (assuming a stream that captures `my.messages`):

```typescript
const subj = "my.messages";
const opts = consumerOpts();
opts.orderedConsumer();
const sub = await js.subscribe(subj, opts);
```

Use callbacks or iterators as desired to process the messages.
`sub.unsubscribe()` or break out of an iterator to stop processing messages.

#### JetStream Materialized Views

JetStream clients can use streams to store and access data. The materialized
views present a different _API_ to interact with the data stored in a stream.
First materialized view for JetStream is the _KV_ view. The _KV_ view implements
a Key-Value API on top of JetStream. Clients can store and retrieve values by
Keys:

```typescript
const sc = StringCodec();
const js = nc.jetstream();
// create the named KV or bind to it if it exists:
const kv = await js.views.kv("testing", { history: 5 });

// create an entry - this is similar to a put, but will fail if the
// key exists
await kv.create("hello.world", sc.encode("hi"));

// Values in KV are stored as KvEntries:
// {
//   bucket: string,
//   key: string,
//   value: Uint8Array,
//   created: Date,
//   revision: number,
//   delta?: number,
//   operation: "PUT"|"DEL"|"PURGE"
// }
// The operation property specifies whether the value was
// updated (PUT), deleted (DEL) or purged (PURGE).

// you can monitor values modification in a KV by watching.
// You can watch specific key subset or everything.
// Watches start with the latest value for each key in the
// set of keys being watched - in this case all keys
const watch = await kv.watch();
(async () => {
  for await (const e of watch) {
    // do something with the change
  }
})().then();

// update the entry
await kv.put("hello.world", sc.encode("world"));
// retrieve the KvEntry storing the value
// returns null if the value is not found
const e = await kv.get("hello.world");
assert(e);
// initial value of "hi" was overwritten above
assertEquals(sc.decode(e.value), "world");

const buf: string[] = [];
const keys = await kv.keys();
await (async () => {
  for await (const k of keys) {
    buf.push(k);
  }
})();
assertEquals(buf.length, 1);
assertEquals(buf[0], "hello.world");

let h = await kv.history({ key: "hello.world" });
await (async () => {
  for await (const e of h) {
    // do something with the historical value
    // you can test e.operation for "PUT", "DEL", or "PURGE"
    // to know if the entry is a marker for a value set
    // or for a deletion or purge.
  }
})();

// deletes the key - the delete is recorded
await kv.delete("hello.world");

// purge is like delete, but all history values
// are dropped and only the purge remains.
await kv.purge("hello.world");

// stop the watch operation above
watch.stop();

// danger: destroys all values in the KV!
await kv.destroy();
```

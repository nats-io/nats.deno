# Services Framework

The Services Framework introduces a higher-level API for implementing services
with NATS. NATS has always been a strong technology on which to build services,
as they are easy to write, are location and DNS independent and can be scaled up
or down by simply adding or removing instances of the service.

The Services Framework further streamlines their development by providing
observability and standardization. The Service Framework allows your services to
be discovered, queried for status and schema information without additional
work.

## Creating a Service

```typescript
const service = await nc.services.add({
  name: "max",
  version: "0.0.1",
  description: "returns max number in a request",
  endpoint: {
    subject: "max",
    handler: (err, msg) => {
      msg?.respond();
    },
  },
});
```

If you omit the handler, the service is actually an iterator for service
messages. To process messages incoming to the service:

```typescript
for await (const r of service) {
  r.respond();
}
```

For those paying attention, this looks suspiciously like a regular subscription.
And it is. The only difference is that we are collecting additional _metadata_
that allows the service framework to provide some monitoring and discovery for
free.

To invoke the service, it is a simple NATS request:

```typescript
const response = await nc.request("max", JSONCodec().encode([1, 2, 3]));
```

## Discovery and Monitoring

When we started the service you see above, the framework automatically assigned
it an unique `ID`. The `name` and `ID` identify particular instance of the
service. If you start a second instance, that instance will also have the same
`name` but will sport a different `ID`.

To discover services that are running, create a monitoring client:

```typescript
const m = nc.services.client();

// you can ping, request info, stats, and schema information
// All the operations return iterators describing the services found.
for await (const s of await m.ping()) {
  console.log(s.id);
}
await m.stats();
await m.info();
await m.schema();
```

Additional filtering is possible, and they are valid for all the operations:

```typescript
// get the stats services that have the name "max"
await m.stats("max");
// or target a specific instance:
await m.stats("max", id);
```

For a more elaborate first example see:
[simple example here](examples/services/01_services.ts)

## Multiple Endpoints

More complex services will have more than one endpoint. For example a calculator
service may have endpoints for `sum`, `average`, `max`, etc. This type of
service is also possible with the service api, see
[multiple endpoints](examples/services/02_multiple_endpoints.ts)

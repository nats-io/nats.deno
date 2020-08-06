# TODO

- [X] Refactor core to share with nats.ws as a separate module
- [X] Remove callback from flush()
- [X] Add timeout from subscription as option
- [X] Subscription timeout, reject the iterator
- [X] Changed subscribe signature to just return the subjection (no promise)
- [X] Changed drain/sub.drain to return Promise<void>
- [X] Remove argument from flush()
- [X] Binary apis changed to be Uint8Array
- [X] Subscriptions as iterators
- [X] Stale connection
- [X] Remove encoders from client, changing payload signatures to Uint8Arrays only.
- [ ] Package nuidjs as its own project
- [ ] Move nats-base-client to its own project
- [ ] Transport send batching
- [ ] Transport certificate authentication

## BUGS


## Features
- [X] Transform all urls to something deno can handle
- [X] Implement reconnect
- [X] TLS support
- [X] NKey signing support


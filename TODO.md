# TODO

- [ ] Refactor core to share with nats.ws as a separate module
- [X] Remove callback from flush()
- [ ] Add timeout from subscription as option
- [ ] Subscription timeouts not notified via callback to the subscription
- [X] Changed subscribe signature to just return the subjection (no promise)
- [X] Changed drain/sub.drain to return Promise<void>
- [X] Remove argument from flush()
- [X] Binary apis changed to be Uint8Array
- [X] Subscriptions as iterators



## BUGS


## Features
- [X] Transform all urls to something deno can handle
- [X] Implement reconnect
- [X] TLS support
- [ ] NKey signing support


import { DenoTransport } from "./deno_transport.ts";
import {
  NatsConnection,
  ConnectionOptions,
  setTransportFactory,
  Transport,
} from "../nats-base-client/mod.ts";

export function connect(opts: ConnectionOptions = {}): Promise<NatsConnection> {
  setTransportFactory((): Transport => {
    return new DenoTransport();
  });

  return NatsConnection.connect(opts);
}

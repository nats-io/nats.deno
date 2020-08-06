import { DenoTransport } from "./deno_transport.ts";
import {
  NatsConnection,
  ConnectionOptions,
  setTransportFactory,
  Transport,
  NatsConnectionImpl,
} from "../nats-base-client/internal_mod.ts";

export function connect(opts: ConnectionOptions = {}): Promise<NatsConnection> {
  setTransportFactory((): Transport => {
    return new DenoTransport();
  });

  return NatsConnectionImpl.connect(opts);
}

import {
  JetStreamClient,
  JetStreamManager,
  JetStreamManagerOptions,
} from "./types.ts";
import {
  ErrorCode,
  NatsConnection,
  NatsError,
} from "../nats-base-client/core.ts";
import { JetStreamManagerImpl } from "./jsm.ts";
import { JetStreamClientImpl } from "./jsclient.ts";

export function jetstream(
  nc: NatsConnection,
  opts: JetStreamManagerOptions = {},
): JetStreamClient {
  return new JetStreamClientImpl(nc, opts);
}

export async function jetstreamManager(
  nc: NatsConnection,
  opts: JetStreamManagerOptions = {},
): Promise<JetStreamManager> {
  const adm = new JetStreamManagerImpl(nc, opts);
  if (opts.checkAPI !== false) {
    try {
      await adm.getAccountInfo();
    } catch (err) {
      const ne = err as NatsError;
      if (ne.code === ErrorCode.NoResponders) {
        ne.code = ErrorCode.JetStreamNotEnabled;
      }
      throw ne;
    }
  }
  return adm;
}

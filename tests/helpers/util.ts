import {
  deferred,
  Msg,
  Subscription,
  timeout,
} from "../../nats-base-client/internal_mod.ts";

export function consume(sub: Subscription, ms: number = 1000): Promise<Msg[]> {
  const to = timeout<Msg[]>(ms);
  const d = deferred<Msg[]>();
  const msgs: Msg[] = [];
  (async () => {
    for await (const m of sub) {
      msgs.push(m);
    }
    to.cancel();
    d.resolve(msgs);
  })().catch((err) => {
    d.reject(err);
  });

  return Promise.race([to, d]);
}

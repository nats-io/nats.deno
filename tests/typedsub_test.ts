/*
 * Copyright 2021 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  assert,
  assertEquals,
  assertRejects,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { assertErrorCode, assertThrowsErrorCode } from "./helpers/asserts.ts";
import {
  createInbox,
  deferred,
  ErrorCode,
  Msg,
  NatsError,
  StringCodec,
  TypedSubscription,
  TypedSubscriptionOptions,
} from "../nats-base-client/internal_mod.ts";
import { checkFn } from "../nats-base-client/typedsub.ts";
import { cleanup, setup } from "./jstest_util.ts";

Deno.test("typedsub - rejects no adapter", async () => {
  const { nc, ns } = await setup();
  assertThrowsErrorCode(() => {
    new TypedSubscription<string>(
      nc,
      "hello",
      {} as TypedSubscriptionOptions<string>,
    );
  }, ErrorCode.ApiError);
  await cleanup(ns, nc);
});

Deno.test("typedsub - iterator gets data", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();

  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const text = "the fox jumped over the fence";
  const out = text.split(" ");
  tso.max = out.length;

  const sa = new TypedSubscription<string>(nc, subj, tso);
  assertEquals(sa.getMax(), out.length);

  const d: string[] = [];
  const done = (async () => {
    for await (const s of sa) {
      d.push(s);
    }
  })();

  assertEquals(sa.getSubject(), subj);
  out.forEach((v) => nc.publish(subj, sc.encode(v)));
  await done;
  assertEquals(sa.getProcessed(), out.length);
  assertEquals(sa.getPending(), 0);
  assertEquals(d, out);
  await cleanup(ns, nc);
});

Deno.test("typedsub - callback gets data", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();
  const sc = StringCodec();

  const tso = {} as TypedSubscriptionOptions<string>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const d: string[] = [];
  tso.callback = (_err, data) => {
    d.push(data ?? "BAD");
  };

  new TypedSubscription<string>(nc, subj, tso);
  const text = "the fox jumped over the fence";
  const out = text.split(" ");
  out.forEach((v) => nc.publish(subj, sc.encode(v)));
  await nc.flush();
  assertEquals(d, out);
  await cleanup(ns, nc);
});

Deno.test("typedsub - dispatched", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();
  const tso = {} as TypedSubscriptionOptions<Msg>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, Msg | null] => {
    return [err, msg];
  };

  tso.dispatchedFn = (msg) => {
    if (msg && msg.reply) {
      msg.respond(msg.data);
    }
  };

  const sub = new TypedSubscription<Msg>(nc, subj, tso);
  (async () => {
    for await (const _m of sub) {
      // nothing
    }
  })().catch();

  const d = StringCodec().encode("hello");
  const resp = await nc.request(subj, d);
  assertEquals(resp.data, d);
  await cleanup(ns, nc);
});

Deno.test("typedsub - cleanup on unsub", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();
  const tso = {} as TypedSubscriptionOptions<Msg>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, Msg | null] => {
    return [err, msg];
  };

  const d = deferred<void>();
  tso.cleanupFn = () => {
    d.resolve();
  };

  const sub = new TypedSubscription<Msg>(nc, subj, tso);
  sub.unsubscribe();
  await d;
  await cleanup(ns, nc);
});

Deno.test("typedsub - cleanup on close", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();
  const tso = {} as TypedSubscriptionOptions<Msg>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, Msg | null] => {
    return [err, msg];
  };

  tso.callback = () => {};

  const d = deferred<void>();
  tso.cleanupFn = () => {
    d.resolve();
  };

  new TypedSubscription<Msg>(nc, subj, tso);
  await cleanup(ns, nc);
  await d;
});

Deno.test("typedsub - checkFn", () => {
  assertThrowsErrorCode(() => {
    checkFn(undefined, "t", true);
  }, ErrorCode.ApiError);

  assertThrowsErrorCode(() => {
    checkFn("what", "t", false);
  }, ErrorCode.ApiError);
});

Deno.test("typedsub - unsubscribe", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();

  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const sa = new TypedSubscription<string>(nc, subj, tso);
  const done = (async () => {
    for await (const _s of sa) {
      // nothing
    }
  })();
  sa.unsubscribe();
  await done;
  assertEquals(sa.isClosed(), true);
  await cleanup(ns, nc);
});

Deno.test("typedsub - drain", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();

  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const sa = new TypedSubscription<string>(nc, subj, tso);
  (async () => {
    for await (const _s of sa) {
      // nothing
    }
  })().then();
  await sa.drain();
  assertEquals(sa.isClosed(), true);
  await cleanup(ns, nc);
});

Deno.test("typedsub - timeout", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();

  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.timeout = 500;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const sa = new TypedSubscription<string>(nc, subj, tso);
  assert(sa.sub.timer !== undefined);
  await assertRejects(
    async () => {
      for await (const _s of sa) {
        // nothing
      }
    },
    NatsError,
    ErrorCode.Timeout,
    undefined,
  );

  await cleanup(ns, nc);
});

Deno.test("typedsub - timeout on callback", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();

  const d = deferred<NatsError | null>();
  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.timeout = 500;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };
  tso.callback = (err) => {
    d.resolve(err);
  };
  const sa = new TypedSubscription<string>(nc, subj, tso);
  const err = await d;
  assert(err !== null);
  assertErrorCode(err, ErrorCode.Timeout);
  assertEquals(sa.isClosed(), true);

  await cleanup(ns, nc);
});

Deno.test("typedsub - timeout cleared on message", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();

  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.max = 1;
  tso.timeout = 1000;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };

  const sa = new TypedSubscription<string>(nc, subj, tso);
  assert(sa.sub.timer !== undefined);
  const done = (async () => {
    for await (const _s of sa) {
      // nothing
    }
  })();
  nc.publish(subj);
  await nc.flush();
  await done;
  assertEquals(sa.sub.timer, undefined);
  assertEquals(sa.getProcessed(), 1);

  await cleanup(ns, nc);
});

Deno.test("typedsub - timeout callback cleared on message", async () => {
  const { nc, ns } = await setup();
  const subj = createInbox();

  const d = deferred<string | null>();
  const sc = StringCodec();
  const tso = {} as TypedSubscriptionOptions<string>;
  tso.timeout = 500;
  tso.max = 1;
  tso.adapter = (
    err,
    msg,
  ): [NatsError | null, string | null] => {
    if (err) {
      return [err, null];
    }
    return [err, sc.decode(msg.data)];
  };
  tso.callback = (err, msg) => {
    err ? d.reject(err) : d.resolve(msg);
  };
  const sa = new TypedSubscription<string>(nc, subj, tso);
  nc.publish(subj, sc.encode("hello"));
  const m = await d;
  assertEquals(m, "hello");
  assertEquals(sa.sub.timer, undefined);
  await cleanup(ns, nc);
});

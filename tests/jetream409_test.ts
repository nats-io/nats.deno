import {
  AckPolicy,
  JetStreamClient,
  PullOptions,
} from "../nats-base-client/types.ts";
import {
  Js409Errors,
  nanos,
  setMaxWaitingToFail,
} from "../nats-base-client/jsutil.ts";
import {
  consumerOpts,
  deferred,
  NatsError,
  StringCodec,
} from "../nats-base-client/mod.ts";
import { assertRejects } from "https://deno.land/std@0.152.0/testing/asserts.ts";
import { assertStringIncludes } from "https://deno.land/std@0.152.0/testing/asserts.ts";
import {
  cleanup,
  initStream,
  jetstreamServerConf,
  setup,
} from "./jstest_util.ts";
import { notCompatible } from "./helpers/mod.ts";

type testArgs = {
  js: JetStreamClient;
  stream: string;
  durable: string;
  opts: PullOptions;
  expected: Js409Errors;
};

async function expectFetchError(args: testArgs) {
  const { js, stream, durable, opts, expected } = args;
  const i = js.fetch(stream, durable, opts);
  await assertRejects(
    async () => {
      for await (const _m of i) {
        //nothing
      }
    },
    Error,
    expected,
  );
}

async function expectPullSubscribeIteratorError(args: testArgs) {
  const { js, stream, durable, opts, expected } = args;
  const co = consumerOpts();
  co.bind(stream, durable);
  const sub = await js.pullSubscribe(">", co);
  sub.pull(opts);

  await assertRejects(
    async () => {
      for await (const _m of sub) {
        // nothing
      }
    },
    Error,
    expected,
  );
}

async function expectPullSubscribeCallbackError(
  args: testArgs,
) {
  const { js, stream, durable, opts, expected } = args;

  const d = deferred<NatsError | null>();
  const co = consumerOpts();
  co.bind(stream, durable);
  co.callback((err) => {
    d.resolve(err);
  });
  const sub = await js.pullSubscribe(">", co);
  sub.pull(opts);
  const ne = await d;
  assertStringIncludes(ne?.message || "", expected);
}

Deno.test("409 - max_batch", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();

  const sc = StringCodec();
  const js = nc.jetstream();
  for (let i = 0; i < 10; i++) {
    await js.publish(subj, sc.encode("hello"));
  }

  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
    max_batch: 1,
  });

  const opts = { batch: 10, expires: 1000 } as PullOptions;
  const to = {
    js,
    stream,
    durable: "a",
    opts,
    expected: Js409Errors.MaxBatchExceeded,
  };

  await expectFetchError(to);
  await expectPullSubscribeIteratorError(to);
  await expectPullSubscribeCallbackError(to);

  await cleanup(ns, nc);
});

Deno.test("409 - max_expires", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();

  const sc = StringCodec();
  const js = nc.jetstream();
  for (let i = 0; i < 10; i++) {
    await js.publish(subj, sc.encode("hello"));
  }

  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
    max_expires: nanos(1000),
  });

  const opts = { batch: 1, expires: 5000 } as PullOptions;
  const to = {
    js,
    stream,
    durable: "a",
    opts,
    expected: Js409Errors.MaxExpiresExceeded,
  };

  await expectFetchError(to);
  await expectPullSubscribeIteratorError(to);
  await expectPullSubscribeCallbackError(to);

  await cleanup(ns, nc);
});

Deno.test("409 - max_bytes", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.8.3")) {
    return;
  }
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();

  const sc = StringCodec();
  const js = nc.jetstream();
  for (let i = 0; i < 10; i++) {
    await js.publish(subj, sc.encode("hello"));
  }

  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
    max_bytes: 10,
  });

  const opts = { max_bytes: 1024, expires: 5000 } as PullOptions;
  const to = {
    js,
    stream,
    durable: "a",
    opts,
    expected: Js409Errors.MaxBytesExceeded,
  };

  await expectFetchError(to);
  await expectPullSubscribeIteratorError(to);
  await expectPullSubscribeCallbackError(to);

  await cleanup(ns, nc);
});

Deno.test("409 - max msg size", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  if (await notCompatible(ns, nc, "2.9.0")) {
    return;
  }
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();

  const sc = StringCodec();
  const js = nc.jetstream();
  for (let i = 0; i < 10; i++) {
    await js.publish(subj, sc.encode("hello"));
  }

  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
  });

  const opts = { max_bytes: 2, expires: 5000 } as PullOptions;
  const to = {
    js,
    stream,
    durable: "a",
    opts,
    expected: Js409Errors.MaxMessageSizeExceeded,
  };

  await expectFetchError(to);
  await expectPullSubscribeIteratorError(to);
  await expectPullSubscribeCallbackError(to);

  await cleanup(ns, nc);
});

Deno.test("409 - max waiting", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();

  const sc = StringCodec();
  const js = nc.jetstream();
  for (let i = 0; i < 10; i++) {
    await js.publish(subj, sc.encode("hello"));
  }

  await jsm.consumers.add(stream, {
    durable_name: "a",
    ack_policy: AckPolicy.Explicit,
    max_waiting: 1,
  });

  const opts = { expires: 1000 } as PullOptions;
  const to = {
    js,
    stream,
    durable: "a",
    opts,
    expected: Js409Errors.MaxWaitingExceeded,
  };

  const iter = js.fetch(stream, "a", { batch: 1000, expires: 5000 });
  (async () => {
    for await (const _m of iter) {
      // nothing
    }
  })().then();

  setMaxWaitingToFail(true);

  await expectFetchError(to);
  await expectPullSubscribeIteratorError(to);
  await expectPullSubscribeCallbackError(to);

  await cleanup(ns, nc);
});

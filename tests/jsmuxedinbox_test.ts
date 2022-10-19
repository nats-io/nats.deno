/*
 * Copyright 2022 The NATS Authors
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
import { cleanup, setup } from "./jstest_util.ts";
import { JsMuxedInbox } from "../nats-base-client/jsmuxedInbox.ts";
import { deferred, Empty } from "../nats-base-client/mod.ts";
import { assert, fail } from "https://deno.land/std@0.152.0/testing/asserts.ts";

Deno.test("jsmuxedinbox - basics", async () => {
  const { ns, nc } = await setup();
  const noResponders = deferred();
  const muxed = deferred<string>();
  const nonMuxed = deferred<string>();
  type cookie = { n: number };

  nc.subscribe("q", {
    callback: (err, msg) => {
      msg.respond();
    },
  });

  const mux = new JsMuxedInbox<cookie>(nc, (cookie, err, msg): void => {
    if (err) {
      fail(err.message);
      return;
    }
    if (msg?.headers?.code === 503) {
      assert(msg.subject.endsWith(".bad"));
      noResponders.resolve();
      return;
    }
    if (cookie?.id === "") {
      nonMuxed.resolve(msg?.subject);
      assert(msg?.subject.endsWith(".a"));
      return;
    }
    muxed.resolve(msg?.subject);
  });

  // publish to any subject not registered
  nc.publish("foo", Empty, { reply: `${mux.prefix}.bad` });
  nc.publish("q", Empty, { reply: `${mux.prefix}.a` });
  mux.request("q", { n: 1 });

  await muxed;
  await nonMuxed;
  await noResponders;
  await cleanup(ns, nc);
});

Deno.test("jsmuxedinbox - sub close", async () => {
  const { ns, nc } = await setup();
  const mux = new JsMuxedInbox<unknown>(nc, (): void => {});
  setTimeout(() => {
    mux.sub.unsubscribe();
  });
  await mux.done;
  await cleanup(ns, nc);
});

Deno.test("jsmuxedinbox - close", async () => {
  const { ns, nc } = await setup();
  const mux = new JsMuxedInbox<unknown>(nc, (): void => {});
  setTimeout(() => {
    mux.close();
  });
  await mux.done;
  await cleanup(ns, nc);
});

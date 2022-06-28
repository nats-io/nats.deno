import { IdleHeartbeat } from "../nats-base-client/idleheartbeat.ts";
import {
  assert,
  assertEquals,
  fail,
} from "https://deno.land/std@0.138.0/testing/asserts.ts";

Deno.test("idleheartbeat - basic", async () => {
  const h = new IdleHeartbeat(250, 1);
  let count = 0;
  const timer = setInterval(() => {
    count++;
    h.ok();
    if (count === 8) {
      clearInterval(timer);
      h.cancel();
    }
  }, 100);
  const v = await h.done;
  assertEquals(v, 0);
  assert(h.count > 0);
});

Deno.test("idleheartbeat - timeout", async () => {
  const h = new IdleHeartbeat(250, 1);
  h.done.catch((err) => {
    fail(`promise failed ${err.message}`);
  });

  const v = await h.done;
  assertEquals(v, 1);
});

Deno.test("idleheartbeat - timeout maxOut", async () => {
  const h = new IdleHeartbeat(250, 5);
  h.done.catch((err) => {
    fail(`promise failed ${err.message}`);
  });

  const v = await h.done;
  assertEquals(v, 5);
});

Deno.test("idleheartbeat - timeout recover", async () => {
  const h = new IdleHeartbeat(250, 5);
  h.done.catch((err) => {
    fail(`promise failed ${err.message}`);
  });

  const interval = setInterval(() => {
    h.ok();
  }, 1000);

  setTimeout(() => {
    h.cancel();
    clearInterval(interval);
  }, 1650);

  const v = await h.done;
  assertEquals(v, 0);
  assertEquals(h.missed, 2);
});

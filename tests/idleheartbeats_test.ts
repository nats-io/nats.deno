import { IdleHeartbeat } from "../nats-base-client/idleheartbeat.ts";
import { assertEquals } from "https://deno.land/std@0.138.0/testing/asserts.ts";
import { deferred } from "../nats-base-client/util.ts";

Deno.test("idleheartbeat - basic", async () => {
  const d = deferred<number>();
  const h = new IdleHeartbeat(250, () => {
    d.reject(new Error("didn't expect to notify"));
    return true;
  });
  let count = 0;
  const timer = setInterval(() => {
    count++;
    h.work();
    if (count === 8) {
      clearInterval(timer);
      h.cancel();
      d.resolve();
    }
  }, 100);

  await d;
});

Deno.test("idleheartbeat - timeout", async () => {
  const d = deferred<number>();
  new IdleHeartbeat(250, (v: number): boolean => {
    d.resolve(v);
    return true;
  }, 1);
  assertEquals(await d, 1);
});

Deno.test("idleheartbeat - timeout maxOut", async () => {
  const d = deferred<number>();
  new IdleHeartbeat(250, (v: number): boolean => {
    d.resolve(v);
    return true;
  }, 5);
  assertEquals(await d, 5);
});

Deno.test("idleheartbeat - timeout recover", async () => {
  const d = deferred<void>();
  const h = new IdleHeartbeat(250, (_v: number): boolean => {
    d.reject(new Error("didn't expect to fail"));
    return true;
  }, 5);

  const interval = setInterval(() => {
    h.work();
  }, 1000);

  setTimeout(() => {
    h.cancel();
    d.resolve();
    clearInterval(interval);
  }, 1650);

  await d;
  assertEquals(h.missed, 2);
});

import {
  compare,
  Feature,
  Features,
  parseSemVer,
} from "../nats-base-client/semver.ts";
import {
  assert,
  assertEquals,
  assertFalse,
} from "https://deno.land/std@0.152.0/testing/asserts.ts";

Deno.test("semver", () => {
  const pt: { a: string; b: string; r: number }[] = [
    { a: "1.0.0", b: "1.0.0", r: 0 },
    { a: "1.0.1", b: "1.0.0", r: 1 },
    { a: "1.1.0", b: "1.0.1", r: 1 },
    { a: "1.1.1", b: "1.1.0", r: 1 },
    { a: "1.1.1", b: "1.1.1", r: 0 },
    { a: "1.1.100", b: "1.1.099", r: 1 },
    { a: "2.0.0", b: "1.500.500", r: 1 },
  ];

  pt.forEach((t) => {
    assertEquals(
      compare(parseSemVer(t.a), parseSemVer(t.b)),
      t.r,
      `compare(${t.a}, ${t.b})`,
    );
  });
});

Deno.test("semver - feature basics", () => {
  // sanity version check
  const f = new Features(parseSemVer("4.0.0"));
  assert(f.require("4.0.0"));
  assertFalse(f.require("5.0.0"));

  // change the version to 2.8.3
  f.update(parseSemVer("2.8.3"));
  let info = f.get(Feature.JS_PULL_MAX_BYTES);
  assertEquals(info.ok, true);
  assertEquals(info.min, "2.8.3");
  assert(f.supports(Feature.JS_PULL_MAX_BYTES));
  assertFalse(f.isDisabled(Feature.JS_PULL_MAX_BYTES));
  // disable the feature
  f.disable(Feature.JS_PULL_MAX_BYTES);
  assert(f.isDisabled(Feature.JS_PULL_MAX_BYTES));
  assertFalse(f.supports(Feature.JS_PULL_MAX_BYTES));
  info = f.get(Feature.JS_PULL_MAX_BYTES);
  assertEquals(info.ok, false);
  assertEquals(info.min, "unknown");

  // remove all disablements
  f.resetDisabled();
  assert(f.supports(Feature.JS_PULL_MAX_BYTES));

  f.update(parseSemVer("2.8.2"));
  assertFalse(f.supports(Feature.JS_PULL_MAX_BYTES));
});

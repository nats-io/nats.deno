import { compare, parseSemVer } from "../nats-base-client/semver.ts";
import { assertEquals } from "https://deno.land/std@0.152.0/testing/asserts.ts";

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

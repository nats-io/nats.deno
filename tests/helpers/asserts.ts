import { assert, fail } from "https://deno.land/std@0.61.0/testing/asserts.ts";

export function assertErrorCode(err: Error, ...codes: string[]) {
  const { code } = err as { code?: string };
  assert(code);

  const ok = codes.find((c) => {
    return code.indexOf(c) !== -1;
  });
  assert(ok);
}

export function assertThrowsErrorCode(fn: () => any, ...codes: string[]) {
  try {
    fn();
    fail("failed to throw error");
  } catch (err) {
    assertErrorCode(err, ...codes);
  }
}

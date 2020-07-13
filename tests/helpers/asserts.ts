import { AssertionError } from "https://deno.land/std@0.61.0/testing/asserts.ts";
import { NatsError } from "../../src/mod.ts";

export function assertErrorCode(err: Error, ...code: string[]) {
  if (!(Object.getPrototypeOf(err) === NatsError.prototype)) {
    throw new AssertionError(
      `Expected error to be instance of NatsError but got "${err.name}"`,
    );
  }
  const ne = err as NatsError;

  if (code) {
    if (code.indexOf(ne.code) === -1) {
      throw new AssertionError(
        `Expected error message to include "${
          code.join(" ")
        }", but got "${ne.code}"`,
      );
    }
  }
}

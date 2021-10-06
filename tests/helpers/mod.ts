import { NatsServer } from "./launcher.ts";
import { NatsConnection } from "../../nats-base-client/types.ts";
import { cleanup } from "../jstest_util.ts";

export { check } from "./check.ts";
export { Lock } from "./lock.ts";
import { red, yellow } from "https://deno.land/std@0.95.0/fmt/colors.ts";
export { Connection, TestServer } from "./test_server.ts";
export {
  assertBetween,
  assertErrorCode,
  assertThrowsAsyncErrorCode,
  assertThrowsErrorCode,
} from "./asserts.ts";
export { NatsServer, ServerSignals } from "./launcher.ts";

export type SemVer = { major: number; minor: number; micro: number };
export function parseSemVer(
  s: string,
): SemVer {
  const m = s.match(/(\d+).(\d+).(\d+)/);
  if (m) {
    return {
      major: parseInt(m[1]),
      minor: parseInt(m[2]),
      micro: parseInt(m[3]),
    };
  }
  throw new Error(`${s} is not a semver value`);
}
export function compare(a: SemVer, b: SemVer): number {
  if (a.major < b.major) return -1;
  if (a.major > b.major) return 1;
  if (a.minor < b.minor) return -1;
  if (a.minor > b.minor) return 1;
  if (a.micro < b.micro) return -1;
  if (a.micro > b.micro) return 1;
  return 0;
}

export function disabled(reason: string): void {
  const m = new TextEncoder().encode(red(`skipping: ${reason} `));
  Deno.stdout.writeSync(m);
}

export async function notCompatible(
  ns: NatsServer,
  nc: NatsConnection,
  version?: string,
): Promise<boolean> {
  version = version ?? "2.3.3";
  const varz = await ns.varz() as unknown as Record<string, string>;
  const sv = parseSemVer(varz.version);
  if (compare(sv, parseSemVer(version)) < 0) {
    const m = new TextEncoder().encode(yellow(
      `skipping test as server (${varz.version}) doesn't implement required feature from ${version} `,
    ));
    await Deno.stdout.write(m);
    await cleanup(ns, nc);
    return true;
  }
  return false;
}

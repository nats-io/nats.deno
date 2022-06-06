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

export enum Feature {
  JS_PULL_MAX_BYTES = "js_pull_max_bytes",
}

type FeatureVersion = {
  ok: boolean;
  min: string;
};

export class Features {
  server: SemVer;
  features: Map<Feature, FeatureVersion>;
  constructor(v: SemVer) {
    this.features = new Map<Feature, FeatureVersion>();
    this.server = v;

    this.set(Feature.JS_PULL_MAX_BYTES, "2.8.3");
  }

  set(f: Feature, requires: string) {
    this.features.set(f, {
      min: requires,
      ok: compare(this.server, parseSemVer(requires)) >= 0,
    });
  }

  get(f: Feature): FeatureVersion {
    return this.features.get(f) || { min: "unknown", ok: false };
  }

  supports(f: Feature): boolean {
    return this.get(f).ok;
  }

  require(v: SemVer | string): boolean {
    if (typeof v === "string") {
      v = parseSemVer(v);
    }
    return compare(this.server, v) >= 0;
  }
}

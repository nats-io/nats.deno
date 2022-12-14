export type SemVer = { major: number; minor: number; micro: number };
export function parseSemVer(
  s = "",
): SemVer {
  const m = s.match(/(\d+).(\d+).(\d+)/);
  if (m) {
    return {
      major: parseInt(m[1]),
      minor: parseInt(m[2]),
      micro: parseInt(m[3]),
    };
  }
  throw new Error(`'${s}' is not a semver value`);
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
  JS_KV = "js_kv",
  JS_OBJECTSTORE = "js_objectstore",
  JS_PULL_MAX_BYTES = "js_pull_max_bytes",
  JS_NEW_CONSUMER_CREATE_API = "js_new_consumer_create",
  JS_ALLOW_DIRECT = "js_allow_direct",
}

type FeatureVersion = {
  ok: boolean;
  min: string;
};

export class Features {
  server!: SemVer;
  features: Map<Feature, FeatureVersion>;
  disabled: Feature[];
  constructor(v: SemVer) {
    this.features = new Map<Feature, FeatureVersion>();
    this.disabled = [];
    this.update(v);
  }

  /**
   * Removes all disabled entries
   */
  resetDisabled() {
    this.disabled.length = 0;
    this.update(this.server);
  }

  /**
   * Disables a particular feature.
   * @param f
   */
  disable(f: Feature) {
    this.disabled.push(f);
    this.update(this.server);
  }

  isDisabled(f: Feature) {
    return this.disabled.indexOf(f) !== -1;
  }

  update(v: SemVer | string) {
    if (typeof v === "string") {
      v = parseSemVer(v);
    }
    this.server = v;
    this.set(Feature.JS_KV, "2.6.2");
    this.set(Feature.JS_OBJECTSTORE, "2.6.3");
    this.set(Feature.JS_PULL_MAX_BYTES, "2.8.3");
    this.set(Feature.JS_NEW_CONSUMER_CREATE_API, "2.9.0");
    this.set(Feature.JS_ALLOW_DIRECT, "2.9.0");

    this.disabled.forEach((f) => {
      this.features.delete(f);
    });
  }

  /**
   * Register a feature that requires a particular server version.
   * @param f
   * @param requires
   */
  set(f: Feature, requires: string) {
    this.features.set(f, {
      min: requires,
      ok: compare(this.server, parseSemVer(requires)) >= 0,
    });
  }

  /**
   * Returns whether the feature is available and the min server
   * version that supports it.
   * @param f
   */
  get(f: Feature): FeatureVersion {
    return this.features.get(f) || { min: "unknown", ok: false };
  }

  /**
   * Returns true if the feature is supported
   * @param f
   */
  supports(f: Feature): boolean {
    return this.get(f)?.ok || false;
  }

  /**
   * Returns true if the server is at least the specified version
   * @param v
   */
  require(v: SemVer | string): boolean {
    if (typeof v === "string") {
      v = parseSemVer(v);
    }
    return compare(this.server, v) >= 0;
  }
}

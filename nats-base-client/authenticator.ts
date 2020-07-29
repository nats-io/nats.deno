import {
  fromSeed,
  encode,
} from "https://raw.githubusercontent.com/nats-io/nkeys.js/main/modules/esm/mod.ts";
import { ConnectionOptions } from "./types.ts";
import { ErrorCode, NatsError } from "./mod.ts";

export type NoAuth = void;

export interface TokenAuth {
  auth_token: string;
}

export interface UserPass {
  user: string;
  pass?: string;
}

export interface NKeyAuth {
  nkey: string;
  sig: string;
}

export interface JwtAuth {
  jwt: string;
  nkey?: string;
  sig?: string;
}

type Auth = NoAuth | TokenAuth | UserPass | NKeyAuth | JwtAuth;

/**
 * Authenticator is an interface that returns credentials
 */
export interface Authenticator {
  (nonce?: string): Auth;
}

export function buildAuthenticator(
  opts: ConnectionOptions,
): Authenticator {
  // jwtAuthenticator is created by the user, since it
  // will require possibly reading files which
  // some of the clients are simply unable to do
  if (opts.authenticator) {
    return opts.authenticator;
  }
  if (opts.token) {
    return tokenFn(opts.token);
  }
  if (opts.user) {
    return passFn(opts.user, opts.pass);
  }
  return noAuthFn();
}

export function noAuthFn(): Authenticator {
  return (): NoAuth => {
    return;
  };
}

/**
 * Returns a user/pass authenticator
 * @param { string }user
 * @param {string } pass
 * @return {UserPass}
 */
function passFn(user: string, pass?: string): Authenticator {
  return (): UserPass => {
    return { user, pass };
  };
}

/**
 * Returns a token authenticator
 * @param {string } token
 * @return {TokenAuth}
 */
function tokenFn(token: string): Authenticator {
  return (): TokenAuth => {
    return { auth_token: token };
  };
}

/**
 * Returns an nkey authenticator that returns a public key
 * @param {Uint8Array | (() => Uint8Array)} seed
 * @return {NKeyAuth}
 */
export function nkeyAuthenticator(
  seed?: Uint8Array | (() => Uint8Array),
): Authenticator {
  return (nonce?: string): NKeyAuth => {
    seed = typeof seed === "function" ? seed() : seed;
    const kp = seed ? fromSeed(seed) : undefined;
    const nkey = kp ? kp.getPublicKey() : "";
    const challenge = new TextEncoder().encode(nonce || "");
    const sigBytes = kp !== undefined && nonce ? kp.sign(challenge) : undefined;
    const sig = sigBytes ? encode(sigBytes) : "";
    return { nkey, sig };
  };
}

/**
 * Returns a jwt authenticator. If a seed is provided, the public
 * key, and signature are calculated. Note if a signature is provided
 * the returned value should be a base64 encoded string.
 *
 * @return {JwtAuth}
 * @param ajwt
 * @param seed
 */
export function jwtAuthenticator(
  ajwt: string | (() => string),
  seed?: Uint8Array | (() => Uint8Array),
): Authenticator {
  return (
    nonce?: string,
  ): JwtAuth => {
    const jwt = typeof ajwt === "function" ? ajwt() : ajwt;
    const fn = nkeyAuthenticator(seed);
    const { nkey, sig } = fn(nonce) as NKeyAuth;
    return { jwt, nkey, sig };
  };
}

/**
 * Returns a jwt authenticator configured from the specified creds file contents.
 * @param creds
 * @returns {JwtAuth}
 */
export function credsAuthenticator(creds: Uint8Array): Authenticator {
  const CREDS =
    /\s*(?:(?:[-]{3,}[^\n]*[-]{3,}\n)(.+)(?:\n\s*[-]{3,}[^\n]*[-]{3,}\n))/ig;
  const s = new TextDecoder().decode(creds);
  // get the JWT
  let m = CREDS.exec(s);
  if (!m) {
    throw NatsError.errorForCode(ErrorCode.BAD_CREDS);
  }
  const jwt = m[1].trim();
  // get the nkey
  m = CREDS.exec(s);
  if (!m) {
    throw NatsError.errorForCode(ErrorCode.BAD_CREDS);
  }
  const seed = new TextEncoder().encode(m[1].trim());
  return jwtAuthenticator(jwt, seed);
}

/*
 * Copyright 2020-2022 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { nkeys } from "./nkeys.ts";
import type { ConnectionOptions } from "./types.ts";
import { TD, TE } from "./encoders.ts";
import { ErrorCode, NatsError } from "./error.ts";

/**
 * @type {}
 */
export type NoAuth = void;

/**
 * @type {auth_token: string} the user token
 */
export interface TokenAuth {
  "auth_token": string;
}

/**
 * @type {user: string, pass?: string} the username and
 * optional password if the server requires.
 */
export interface UserPass {
  user: string;
  pass?: string;
}

/**
 * @type {nkey: string, sig: string} the public nkey for the user,
 * and a base64 encoded string for the calculated signature of the
 * challenge nonce.
 */
export interface NKeyAuth {
  nkey: string;
  sig: string;
}

/**
 * @type {jwt: string, nkey?: string, sig?: string} the user JWT,
 * and if not a bearer token also the public nkey for the user,
 * and a base64 encoded string for the calculated signature of the
 * challenge nonce.
 */
export interface JwtAuth {
  jwt: string;
  nkey?: string;
  sig?: string;
}

/**
 * @type NoAuth|TokenAuth|UserPass|NKeyAuth|JwtAuth
 */
export type Auth = NoAuth | TokenAuth | UserPass | NKeyAuth | JwtAuth;

/**
 * Authenticator is an interface that returns credentials.
 * @type function(nonce?: string) => Auth
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
    return tokenAuthenticator(opts.token);
  }
  if (opts.user) {
    return usernamePasswordAuthenticator(opts.user, opts.pass);
  }
  return noAuthFn();
}

export function noAuthFn(): Authenticator {
  return (): NoAuth => {
    return;
  };
}

/**
 * Returns a user/pass authenticator for the specified user and optional password
 * @param { string }user
 * @param {string } pass
 * @return {UserPass}
 */
export function usernamePasswordAuthenticator(
  user: string,
  pass?: string,
): Authenticator {
  return (): UserPass => {
    return { user, pass };
  };
}

/**
 * Returns a token authenticator for the specified token
 * @param { string } token
 * @return {TokenAuth}
 */
export function tokenAuthenticator(token: string): Authenticator {
  return (): TokenAuth => {
    return { auth_token: token };
  };
}

/**
 * Returns an Authenticator that returns a NKeyAuth based that uses the
 * specified seed or function returning a seed.
 * @param {Uint8Array | (() => Uint8Array)} seed - the nkey seed
 * @return {NKeyAuth}
 */
export function nkeyAuthenticator(
  seed?: Uint8Array | (() => Uint8Array),
): Authenticator {
  return (nonce?: string): NKeyAuth => {
    seed = typeof seed === "function" ? seed() : seed;
    const kp = seed ? nkeys.fromSeed(seed) : undefined;
    const nkey = kp ? kp.getPublicKey() : "";
    const challenge = TE.encode(nonce || "");
    const sigBytes = kp !== undefined && nonce ? kp.sign(challenge) : undefined;
    const sig = sigBytes ? nkeys.encode(sigBytes) : "";
    return { nkey, sig };
  };
}

/**
 * Returns an Authenticator function that returns a JwtAuth.
 * If a seed is provided, the public key, and signature are
 * calculated.
 *
 * @param {string | ()=>string} ajwt - the jwt
 * @param {Uint8Array | ()=> Uint8Array } seed - the optional nkey seed
 * @return {Authenticator}
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
 * Returns an Authenticator function that returns a JwtAuth.
 * This is a convenience Authenticator that parses the
 * specifid creds and delegates to the jwtAuthenticator.
 * @param {Uint8Array} creds - the contents of a creds file
 * @returns {JwtAuth}
 */
export function credsAuthenticator(creds: Uint8Array): Authenticator {
  const CREDS =
    /\s*(?:(?:[-]{3,}[^\n]*[-]{3,}\n)(.+)(?:\n\s*[-]{3,}[^\n]*[-]{3,}\n))/ig;
  const s = TD.decode(creds);
  // get the JWT
  let m = CREDS.exec(s);
  if (!m) {
    throw NatsError.errorForCode(ErrorCode.BadCreds);
  }
  const jwt = m[1].trim();
  // get the nkey
  m = CREDS.exec(s);
  if (!m) {
    throw NatsError.errorForCode(ErrorCode.BadCreds);
  }
  const seed = TE.encode(m[1].trim());
  return jwtAuthenticator(jwt, seed);
}

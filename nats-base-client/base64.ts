/*
 * Copyright 2021-2023 The NATS Authors
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
export class Base64Codec {
  static encode(bytes: string | Uint8Array): string {
    if (typeof bytes === "string") {
      return btoa(bytes);
    }
    const a = Array.from(bytes);
    return btoa(String.fromCharCode(...a));
  }

  static decode(s: string, binary = false): Uint8Array | string {
    const bin = atob(s);
    if (!binary) {
      return bin;
    }
    return Uint8Array.from(bin, (c) => c.charCodeAt(0));
  }
}

export class Base64UrlCodec {
  static encode(bytes: string | Uint8Array): string {
    return Base64UrlCodec.toB64URLEncoding(Base64Codec.encode(bytes));
  }

  static decode(s: string, binary = false): Uint8Array | string {
    return Base64Codec.decode(Base64UrlCodec.fromB64URLEncoding(s), binary);
  }

  static toB64URLEncoding(b64str: string): string {
    return b64str
      .replace(/=/g, "")
      .replace(/\+/g, "-")
      .replace(/\//g, "_");
  }

  static fromB64URLEncoding(b64str: string): string {
    // pads are % 4, but not necessary on decoding
    return b64str
      .replace(/_/g, "/")
      .replace(/-/g, "+");
  }
}

export class Base64UrlPaddedCodec {
  static encode(bytes: string | Uint8Array): string {
    return Base64UrlPaddedCodec.toB64URLEncoding(Base64Codec.encode(bytes));
  }

  static decode(s: string, binary = false): Uint8Array | string {
    return Base64UrlPaddedCodec.decode(
      Base64UrlPaddedCodec.fromB64URLEncoding(s),
      binary,
    );
  }

  static toB64URLEncoding(b64str: string): string {
    return b64str
      .replace(/\+/g, "-")
      .replace(/\//g, "_");
  }

  static fromB64URLEncoding(b64str: string): string {
    // pads are % 4, but not necessary on decoding
    return b64str
      .replace(/_/g, "/")
      .replace(/-/g, "+");
  }
}

/*
 * Copyright 2020 The NATS Authors
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

// Heavily inspired by Golang https://golang.org/src/net/http/header.go

export class MsgHeaders {
  static HEADER = "NATS/1.0";
  static valid: boolean[];
  errorStatus?: number;
  headers: Map<string, string[]> = new Map();

  static init() {
    if (!MsgHeaders.valid) {
      MsgHeaders.valid = new Array(127);
      for (let i = 0; i < MsgHeaders.valid.length; i++) {
        MsgHeaders.valid[i] = false;
      }
      const alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
      const lowerAlpha = alpha.toLowerCase();
      const numbers = "0123456789";
      const symbols = "!#$%&\'*+-.`|~";
      const chars = [alpha, lowerAlpha, numbers, symbols];
      chars.map((alphabet) => {
        for (let j = 0; j < alphabet.length; j++) {
          const c = alphabet.charCodeAt(j);
          MsgHeaders.valid[c] = true;
        }
      });
    }
  }

  constructor() {}

  size(): number {
    let count = 0;
    for (const [_, v] of this.headers.entries()) {
      count += v.length;
    }
    return count;
  }

  equals(mh: MsgHeaders) {
    if (
      mh && this.headers.size === mh.headers.size &&
      this.errorStatus === mh.errorStatus
    ) {
      for (const [k, v] of this.headers) {
        const a = mh.values(k);
        if (v.length !== a?.length) {
          return false;
        }
        const vv = [...v].sort();
        const aa = [...a].sort();
        for (let i = 0; i < vv.length; i++) {
          if (vv[i] !== aa[i]) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  static decode(a: Uint8Array): MsgHeaders {
    const mh = new MsgHeaders();
    const s = new TextDecoder().decode(a);
    const lines = s.split("\r\n");
    const h = lines[0];
    if (h !== MsgHeaders.HEADER) {
      const str = h.replace(MsgHeaders.HEADER, "");
      mh.errorStatus = parseInt(str, 10);
    } else {
      lines.slice(1).map((s) => {
        if (s) {
          const [k, v] = s.split(":");
          mh.add(k, v);
        }
      });
    }
    return mh;
  }

  toString(): string {
    if (this.headers.size === 0) {
      return "";
    }
    let s = MsgHeaders.HEADER;
    for (const [k, v] of this.headers) {
      for (let i = 0; i < v.length; i++) {
        s = `${s}\r\n${k}:${v[i]}`;
      }
    }
    return `${s}\r\n\r\n`;
  }

  encode(): Uint8Array {
    return new TextEncoder().encode(this.toString());
  }

  static canonicalMIMEHeaderKey(k: string): string {
    const A = 65;
    const Z = 90;
    const a = 97;
    const z = 122;
    const dash = 45;
    const toLower = a - A;

    let upper = true;
    const buf: number[] = new Array(k.length);
    for (let i = 0; i < k.length; i++) {
      let c = k.charCodeAt(i);
      if (!MsgHeaders.valid[c]) {
        throw new Error(`'${c}' is not a valid character for a header key`);
      }

      if (upper && a <= c && c <= z) {
        c -= toLower;
      } else if (!upper && A <= c && c <= Z) {
        c += toLower;
      }
      buf[i] = c;
      upper = c == dash;
    }
    return String.fromCharCode(...buf);
  }

  get(k: string): string {
    const key = MsgHeaders.canonicalMIMEHeaderKey(k);
    const a = this.headers.get(key);
    return a ? a[0] : "";
  }

  set(k: string, v: string): void {
    const key = MsgHeaders.canonicalMIMEHeaderKey(k);
    this.headers.set(key, [v]);
  }

  add(k: string, v: string): void {
    const key = MsgHeaders.canonicalMIMEHeaderKey(k);
    let a = this.headers.get(key);
    if (!a) {
      a = [];
      this.headers.set(key, a);
    }
    a.push(v);
  }

  values(k: string): string[] {
    const key = MsgHeaders.canonicalMIMEHeaderKey(k);
    return this.headers.get(key) || [];
  }

  del(k: string): void {
    const key = MsgHeaders.canonicalMIMEHeaderKey(k);
    this.headers.delete(key);
  }
}

MsgHeaders.init();

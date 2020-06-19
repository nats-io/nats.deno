/*
 * Copyright 2018 The NATS Authors
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

export class DataBuffer {
  buffers: Uint8Array[] = [];
  byteLength: number = 0;

  static concat(...bufs: Uint8Array[]): Uint8Array {
    let max = 0;
    for (let i = 0; i < bufs.length; i++) {
      max += bufs[i].length;
    }
    let out = new Uint8Array(max);
    let index = 0;
    for (let i = 0; i < bufs.length; i++) {
      out.set(bufs[i], index);
      index += bufs[i].length;
    }
    return out;
  }

  static fromAscii(m: string): Uint8Array {
    if (!m) {
      m = "";
    }
    return new TextEncoder().encode(m);
  }

  static toAscii(a: Uint8Array): string {
    return new TextDecoder().decode(a);
  }

  pack(): void {
    if (this.buffers.length > 1) {
      let v = this.buffers.splice(0, this.buffers.length);
      this.buffers.push(DataBuffer.concat(...v));
    }
  }

  drain(n?: number): Uint8Array {
    if (this.buffers.length) {
      this.pack();
      let v = this.buffers.pop();
      if (v) {
        let max = this.byteLength;
        if (n === undefined || n > max) {
          n = max;
        }
        let d = v.slice(0, n);
        if (max > n) {
          this.buffers.push(v.slice(n));
        }
        this.byteLength = max - n;
        return d;
      }
    }
    return new Uint8Array(0);
  }

  fill(a: Uint8Array): void {
    if (a) {
      this.buffers.push(a);
      this.byteLength += a.length;
    }
  }

  peek(): Uint8Array {
    if (this.buffers.length) {
      this.pack();
      return this.buffers[0];
    }
    return new Uint8Array(0);
  }

  size(): number {
    return this.byteLength;
  }

  length(): number {
    return this.buffers.length;
  }
}

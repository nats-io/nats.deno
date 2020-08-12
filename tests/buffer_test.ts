// Copyright 2018-2020 the Deno authors. All rights reserved. MIT license.

// This code has been ported almost directly from Go's src/bytes/buffer_test.go
// Copyright 2009 The Go Authors. All rights reserved. BSD license.
// https://github.com/golang/go/blob/master/LICENSE

// This code removes all Deno specific functionality to enable its use
// in a browser environment

import {
  assertEquals,
  assert,
  assertThrows,
} from "https://deno.land/std@0.63.0/testing/asserts.ts";
import {
  Buffer,
  MAX_SIZE,
  readAll,
  writeAll,
} from "../nats-base-client/internal_mod.ts";

// N controls how many iterations of certain checks are performed.
const N = 100;
let testBytes: Uint8Array | null;
let testString: string | null;

const ignoreMaxSizeTests = true;

function init(): void {
  if (testBytes == null) {
    testBytes = new Uint8Array(N);
    for (let i = 0; i < N; i++) {
      testBytes[i] = "a".charCodeAt(0) + (i % 26);
    }
    const decoder = new TextDecoder();
    testString = decoder.decode(testBytes);
  }
}

function check(buf: Buffer, s: string): void {
  const bytes = buf.bytes();
  assertEquals(buf.length, bytes.byteLength);
  const decoder = new TextDecoder();
  const bytesStr = decoder.decode(bytes);
  assertEquals(bytesStr, s);
  assertEquals(buf.length, s.length);
}

// Fill buf through n writes of byte slice fub.
// The initial contents of buf corresponds to the string s;
// the result is the final contents of buf returned as a string.
function fillBytes(
  buf: Buffer,
  s: string,
  n: number,
  fub: Uint8Array,
): string {
  check(buf, s);
  for (; n > 0; n--) {
    const m = buf.write(fub);
    assertEquals(m, fub.byteLength);
    const decoder = new TextDecoder();
    s += decoder.decode(fub);
    check(buf, s);
  }
  return s;
}

// Empty buf through repeated reads into fub.
// The initial contents of buf corresponds to the string s.
function empty(
  buf: Buffer,
  s: string,
  fub: Uint8Array,
): void {
  check(buf, s);
  while (true) {
    const r = buf.read(fub);
    if (r === null) {
      break;
    }
    s = s.slice(r);
    check(buf, s);
  }
  check(buf, "");
}

function repeat(c: string, bytes: number): Uint8Array {
  assertEquals(c.length, 1);
  const ui8 = new Uint8Array(bytes);
  ui8.fill(c.charCodeAt(0));
  return ui8;
}

Deno.test("buffer - new buffer", () => {
  init();
  assert(testBytes);
  assert(testString);
  const buf = new Buffer(testBytes.buffer as ArrayBuffer);
  check(buf, testString);
});

Deno.test("buffer - basic operations", () => {
  init();
  assert(testBytes);
  assert(testString);
  const buf = new Buffer();
  for (let i = 0; i < 5; i++) {
    check(buf, "");

    buf.reset();
    check(buf, "");

    buf.truncate(0);
    check(buf, "");

    let n = buf.write(testBytes.subarray(0, 1));
    assertEquals(n, 1);
    check(buf, "a");

    n = buf.write(testBytes.subarray(1, 2));
    assertEquals(n, 1);
    check(buf, "ab");

    n = buf.write(testBytes.subarray(2, 26));
    assertEquals(n, 24);
    check(buf, testString.slice(0, 26));

    buf.truncate(26);
    check(buf, testString.slice(0, 26));

    buf.truncate(20);
    check(buf, testString.slice(0, 20));

    empty(buf, testString.slice(0, 20), new Uint8Array(5));
    empty(buf, "", new Uint8Array(100));
  }
});

Deno.test("buffer - read/write byte", () => {
  init();
  assert(testBytes);
  assert(testString);
  const buf = new Buffer();
  buf.writeByte("a".charCodeAt(0));
  const a = String.fromCharCode(buf.readByte()!);
  assertEquals(a, "a");
});

Deno.test("buffer - write string", () => {
  const buf = new Buffer();
  const s = "MSG a 1 b 6\r\nfoobar";
  buf.writeString(s);
  const rs = new TextDecoder().decode(buf.bytes());
  assertEquals(rs, s);
});

Deno.test("buffer - write empty string", () => {
  const buf = new Buffer();
  buf.writeString("");
  assertEquals(buf.length, 0);
  assertEquals(buf.capacity, 0);
});

Deno.test("buffer - read empty at EOF", () => {
  // check that EOF of 'buf' is not reached (even though it's empty) if
  // results are written to buffer that has 0 length (ie. it can't store any data)
  const buf = new Buffer();
  const zeroLengthTmp = new Uint8Array(0);
  const result = buf.read(zeroLengthTmp);
  assertEquals(result, 0);
});

Deno.test("buffer - large byte writes", () => {
  init();
  const buf = new Buffer();
  const limit = 9;
  for (let i = 3; i < limit; i += 3) {
    const s = fillBytes(buf, "", 5, testBytes!);
    empty(buf, s, new Uint8Array(Math.floor(testString!.length / i)));
  }
  check(buf, "");
});

Deno.test("buffer - too large byte writes", () => {
  init();
  const tmp = new Uint8Array(72);
  const growLen = Number.MAX_VALUE;
  const xBytes = repeat("x", 0);
  const buf = new Buffer(xBytes.buffer as ArrayBuffer);
  buf.read(tmp);

  assertThrows(
    () => {
      buf.grow(growLen);
    },
    Error,
    "grown beyond the maximum size",
  );
});

Deno.test("buffer - grow write max buffer", () => {
  const bufSize = 16 * 1024;
  const capacities = [MAX_SIZE, MAX_SIZE - 1];
  for (const capacity of capacities) {
    let written = 0;
    const buf = new Buffer();
    const writes = Math.floor(capacity / bufSize);
    for (let i = 0; i < writes; i++) {
      written += buf.write(repeat("x", bufSize));
    }

    if (written < capacity) {
      written += buf.write(repeat("x", capacity - written));
    }

    assertEquals(written, capacity);
  }
});

Deno.test("buffer - grow read close max buffer plus 1", () => {
  const reader = new Buffer(new ArrayBuffer(MAX_SIZE + 1));
  const buf = new Buffer();

  assertThrows(
    () => {
      buf.readFrom(reader);
    },
    Error,
    "grown beyond the maximum size",
  );
});

Deno.test("buffer - grow read close to max buffer", () => {
  const capacities = [MAX_SIZE, MAX_SIZE - 1];
  for (const capacity of capacities) {
    const reader = new Buffer(new ArrayBuffer(capacity));
    const buf = new Buffer();
    buf.readFrom(reader);

    assertEquals(buf.length, capacity);
  }
});

Deno.test("buffer - read close to max buffer with initial grow", () => {
  const capacities = [MAX_SIZE, MAX_SIZE - 1, MAX_SIZE - 512];
  for (const capacity of capacities) {
    const reader = new Buffer(new ArrayBuffer(capacity));
    const buf = new Buffer();
    buf.grow(MAX_SIZE);
    buf.readFrom(reader);
    assertEquals(buf.length, capacity);
  }
});

Deno.test("buffer - large byte reads", () => {
  init();
  assert(testBytes);
  assert(testString);
  const buf = new Buffer();
  for (let i = 3; i < 30; i += 3) {
    const n = Math.floor(testBytes.byteLength / i);
    const s = fillBytes(buf, "", 5, testBytes.subarray(0, n));
    empty(buf, s, new Uint8Array(testString.length));
  }
  check(buf, "");
});

Deno.test("buffer - cap with pre allocated slice", () => {
  const buf = new Buffer(new ArrayBuffer(10));
  assertEquals(buf.capacity, 10);
});

Deno.test("buffer - read from sync", () => {
  init();
  assert(testBytes);
  assert(testString);
  const buf = new Buffer();
  for (let i = 3; i < 30; i += 3) {
    const s = fillBytes(
      buf,
      "",
      5,
      testBytes.subarray(0, Math.floor(testBytes.byteLength / i)),
    );
    const b = new Buffer();
    b.readFrom(buf);
    const fub = new Uint8Array(testString.length);
    empty(b, s, fub);
  }
  assertThrows(function () {
    new Buffer().readFrom(null!);
  });
});

Deno.test("buffer - test grow", () => {
  const tmp = new Uint8Array(72);
  for (const startLen of [0, 100, 1000, 10000, 100000]) {
    const xBytes = repeat("x", startLen);
    for (const growLen of [0, 100, 1000, 10000, 100000]) {
      const buf = new Buffer(xBytes.buffer as ArrayBuffer);
      // If we read, this affects buf.off, which is good to test.
      const nread = (buf.read(tmp)) ?? 0;
      buf.grow(growLen);
      const yBytes = repeat("y", growLen);
      buf.write(yBytes);
      // Check that buffer has correct data.
      assertEquals(
        buf.bytes().subarray(0, startLen - nread),
        xBytes.subarray(nread),
      );
      assertEquals(
        buf.bytes().subarray(startLen - nread, startLen - nread + growLen),
        yBytes,
      );
    }
  }
});

Deno.test("buffer - read all", () => {
  init();
  assert(testBytes);
  const reader = new Buffer(testBytes.buffer as ArrayBuffer);
  const actualBytes = readAll(reader);
  assertEquals(testBytes.byteLength, actualBytes.byteLength);
  for (let i = 0; i < testBytes.length; ++i) {
    assertEquals(testBytes[i], actualBytes[i]);
  }
});

Deno.test("buffer - write all", () => {
  init();
  assert(testBytes);
  const writer = new Buffer();
  writeAll(writer, testBytes);
  const actualBytes = writer.bytes();
  assertEquals(testBytes.byteLength, actualBytes.byteLength);
  for (let i = 0; i < testBytes.length; ++i) {
    assertEquals(testBytes[i], actualBytes[i]);
  }
});

Deno.test("buffer - bytes array buffer length", () => {
  // defaults to copy
  const args = [{}, { copy: undefined }, undefined, { copy: true }];
  for (const arg of args) {
    const bufSize = 64 * 1024;
    const bytes = new TextEncoder().encode("a".repeat(bufSize));
    const reader = new Buffer();
    writeAll(reader, bytes);

    const writer = new Buffer();
    writer.readFrom(reader);
    const actualBytes = writer.bytes(arg);

    assertEquals(actualBytes.byteLength, bufSize);
    assert(actualBytes.buffer !== writer.bytes(arg).buffer);
    assertEquals(actualBytes.byteLength, actualBytes.buffer.byteLength);
  }
});

Deno.test("buffer - bytes copy false", () => {
  const bufSize = 64 * 1024;
  const bytes = new TextEncoder().encode("a".repeat(bufSize));
  const reader = new Buffer();
  writeAll(reader, bytes);

  const writer = new Buffer();
  writer.readFrom(reader);
  const actualBytes = writer.bytes({ copy: false });

  assertEquals(actualBytes.byteLength, bufSize);
  assertEquals(actualBytes.buffer, writer.bytes({ copy: false }).buffer);
  assert(actualBytes.buffer.byteLength > actualBytes.byteLength);
});

Deno.test("buffer - bytes copy false grow exact bytes", () => {
  const bufSize = 64 * 1024;
  const bytes = new TextEncoder().encode("a".repeat(bufSize));
  const reader = new Buffer();
  writeAll(reader, bytes);

  const writer = new Buffer();
  writer.grow(bufSize);
  writer.readFrom(reader);
  const actualBytes = writer.bytes({ copy: false });

  assertEquals(actualBytes.byteLength, bufSize);
  assertEquals(actualBytes.buffer.byteLength, actualBytes.byteLength);
});

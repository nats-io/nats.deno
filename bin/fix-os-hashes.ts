/*
 * Copyright 2025 The NATS Authors
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
import { parse } from "https://deno.land/std@0.221.0/flags/mod.ts";
import { ObjectStoreImpl, ServerObjectInfo } from "../jetstream/objectstore.ts";
import {
  connect,
  ConnectionOptions,
  credsAuthenticator,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/src/mod.ts";
import { Base64UrlPaddedCodec } from "../nats-base-client/base64.ts";
import {
  SHA256 as BAD_SHA256,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/refs/tags/v1.29.1/nats-base-client/sha256.js";
import { consumerOpts } from "../jetstream/mod.ts";
import { sha256 } from "https://raw.githubusercontent.com/nats-io/nats.deno/refs/tags/v1.29.1/nats-base-client/sha256.js";
import { checkSha256, parseSha256 } from "../jetstream/sha_digest.parser.ts";

const argv = parse(
  Deno.args,
  {
    alias: {
      "s": ["server"],
      "f": ["creds"],
      "b": ["bucket"],
    },
    default: {
      s: "127.0.0.1:4222",
      c: 1,
      i: 0,
    },
    boolean: ["check"],
    string: ["server", "creds", "bucket"],
  },
);

const copts = { servers: argv.s } as ConnectionOptions;

if (argv.h || argv.help) {
  console.log(
    "Usage: fix-os [-s server] [--creds=/path/file.creds] [--check] --bucket=name",
  );
  console.log(
    "\nThis tool fixes metadata entries in an object store that were written",
  );
  console.log(
    "with hashes that were calculated incorrectly due to a bug in the sha256 library.",
  );
  console.log("Please backup your object stores before using this tool.");

  Deno.exit(1);
}

if (argv.creds) {
  const data = await Deno.readFile(argv.creds);
  copts.authenticator = credsAuthenticator(data);
}

if (!argv.bucket) {
  console.log("--bucket is required");
  Deno.exit(1);
}

const nc = await connect(copts);

const js = nc.jetstream();
const jsm = await nc.jetstreamManager();
const lister = jsm.streams.listObjectStores();
let found = false;
const streamName = `OBJ_${argv.bucket}`;
for await (const oss of lister) {
  if (oss.streamInfo.config.name === streamName) {
    found = true;
    break;
  }
}
if (!found) {
  console.log(`bucket '${argv.bucket}' was not found`);
  Deno.exit(1);
}
const os = await js.views.os(argv.bucket) as ObjectStoreImpl;
await fixDigests(os);

async function fixDigests(os: ObjectStoreImpl): Promise<void> {
  let fixes = 0;
  const entries = await os.list();
  for (const entry of entries) {
    if (!entry.digest.startsWith("SHA-256=")) {
      console.error(
        `ignoring entry ${entry.name} - unknown objectstore digest:`,
        entry.digest,
      );
      continue;
    }
    // plain digest string
    const digest = entry.digest.substring(8);
    const parsedDigest = parseSha256(digest);
    if (parsedDigest === null) {
      console.error(
        `ignoring entry ${entry.name} - unable to parse digest:`,
        digest,
      );
      continue;
    }

    const badSha = new BAD_SHA256();
    const sha = sha256.create();
    let badHash = new Uint8Array(0);
    let hash = new Uint8Array(0);

    const oc = consumerOpts();
    oc.orderedConsumer();

    const subj = `$O.${os.name}.C.${entry.nuid}`;
    let needsFixing = false;

    const sub = await js.subscribe(subj, oc);
    for await (const m of sub) {
      if (m.data.length > 0) {
        badSha.update(m.data);
        sha.update(m.data);
      }
      if (m.info.pending === 0) {
        badHash = badSha.digest();
        hash = sha.digest();
        break;
      }
    }
    sub.unsubscribe();

    if (checkSha256(parsedDigest, badHash)) {
      // this one could be bad
      if (!checkSha256(badHash, hash)) {
        console.log(
          `[WARN] entry ${entry.name} has a bad hash: ${
            Base64UrlPaddedCodec.encode(badHash)
          } - should be ${Base64UrlPaddedCodec.encode(hash)}`,
        );
        needsFixing = true;
        fixes++;
      }
    }

    if (argv.check) {
      continue;
    }
    if (needsFixing) {
      const metaSubject = os._metaSubject(entry.name);
      const m = await os.jsm.streams.getMessage(os.stream, {
        last_by_subj: metaSubject,
      });
      const info = m.json<ServerObjectInfo>();
      const digest = Base64UrlPaddedCodec.encode(hash);
      info.digest = `SHA-256=${digest}`;
      try {
        await js.publish(metaSubject, JSON.stringify(info));
      } catch (err) {
        console.error(`[ERR] failed to update ${metaSubject}: ${err.message}`);
        continue;
      }
      try {
        const seq = m.seq;
        await jsm.streams.deleteMessage(os.stream, seq);
      } catch (err) {
        console.error(
          `[WARN] failed to delete bad entry ${metaSubject}: ${err.message} - new entry was added`,
        );
      }
    }
  }

  const verb = argv.check ? "are" : "were";
  console.log(
    `${fixes} digest fixes ${verb} required on bucket ${argv.bucket}`,
  );
}

await nc.drain();

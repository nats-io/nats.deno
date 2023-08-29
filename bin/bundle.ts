/*
 * Copyright 2023 The NATS Authors
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
import { cli } from "https://deno.land/x/cobra@v0.0.9/mod.ts";
import { bundle } from "https://deno.land/x/emit@0.26.0/mod.ts";

const root = cli({
  use: "bundle javascript/typescript",
  run: async (_cmd, _args, flags) => {
    const src = flags.value<string>("src");
    const out = flags.value<string>("out");
    const type = flags.value<boolean>("module") ? "module" : "classic";
    try {
      const r = URL.canParse(src)
        ? await bundle(new URL(src), { type })
        : await bundle(src, { type });
      await Deno.writeTextFile(out, r.code);
      console.log(`wrote ${out}`);
      return 0;
    } catch (err) {
      console.log(`failed to bundle: ${err.message}`);
      console.log(err.stack);
      return 1;
    }
  },
});

root.addFlag({
  name: "src",
  type: "string",
  usage: "input module source path",
  required: true,
});
root.addFlag({
  name: "out",
  type: "string",
  usage: "output bundle path",
  required: true,
});
root.addFlag({
  name: "module",
  type: "boolean",
  usage: "output esm module (default)",
  default: true,
});

Deno.exit(await root.execute(Deno.args));

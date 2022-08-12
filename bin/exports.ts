import { parse } from "https://deno.land/std@0.152.0/flags/mod.ts";
import {
  basename,
  extname,
  join,
  resolve,
} from "https://deno.land/std@0.152.0/path/mod.ts";

const argv = parse(
  Deno.args,
  {},
);

// resolve the specified directories to fq
const dirs = (argv._ as string[]).map((n) => {
  return resolve(n);
});

if (!dirs.length || argv.h || argv.help || dirs.length > 1) {
  console.log(
    `deno run --allow-all exports dir`,
  );
  Deno.exit(1);
}

// collect a list of all the files
const files: string[] = [];
for (const d of dirs) {
  for await (const fn of Deno.readDir(d)) {
    const n = basename(fn.name);
    if (n === "mod.ts" || n === "internal_mod.ts") {
      continue;
    }
    const ext = extname(fn.name);
    if (ext === ".ts" || ext === ".js") {
      files.push(join(d, fn.name));
    }
  }
}

type Export = {
  fn: string;
  all: boolean;
  classes: string[];
  enums: string[];
  functions: string[];
  interfaces: string[];
  types: string[];
  vars: string[];
};

type Exports = Export[];
const exports: Exports = [];

for (const fn of files) {
  const data = await Deno.readFile(fn);
  const txt = new TextDecoder().decode(data);
  const matches = txt.matchAll(
    /export\s+(\*|function|class|type|interface|enum|const|var)\s+(\w+)/g,
  );
  if (!matches) {
    continue;
  }
  const e = {
    fn: fn,
    all: false,
    classes: [],
    enums: [],
    functions: [],
    interfaces: [],
    types: [],
    vars: [],
  } as Export;
  exports.push(e);

  for (const m of matches) {
    switch (m[1]) {
      case "*":
        e.all = true;
        break;
      case "function":
        e.functions.push(m[2]);
        break;
      case "type":
        e.types.push(m[2]);
        break;
      case "interface":
        e.interfaces.push(m[2]);
        break;
      case "enum":
        e.enums.push(m[2]);
        break;
      case "class":
        e.classes.push(m[2]);
        break;
      case "const":
      case "var":
        e.vars.push(m[2]);
        break;
      default:
        // ignore
    }
  }
}

exports.sort((e1, e2) => {
  const a = e1.fn;
  const b = e2.fn;
  return a < b ? -1 : (a > b ? 1 : 0);
});

const ordered = exports.filter((e) => {
  return basename(e.fn) !== "types.ts";
}) || [];

const types = exports.find((e) => {
  return basename(e.fn) === "types.ts";
});
if (types) {
  ordered.unshift(types);
}

for (const e of ordered) {
  e.fn = `./${basename(e.fn)}`;
  const types = [];
  const other = [];
  types.push(...e.types);
  types.push(...e.interfaces);

  other.push(...e.enums);
  other.push(...e.classes);
  other.push(...e.functions);
  other.push(...e.vars);

  if (types.length) {
    console.log(`export type { ${types.join(", ")} } from "${e.fn}"`);
  }
  if (e.all) {
    console.log(`export * from "${e.fn}"`);
  }
  if (other.length) {
    console.log(`export { ${other.join(", ")} } from "${e.fn}"`);
  }
}

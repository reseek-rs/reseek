import { existsSync, readdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";

import type { Plugin } from "vite";

import type { Bundled } from "./collect.ts";

/** The modules the bundler writes itself, by the package that writes each */
const WRITTEN: Record<string, string[]> = {
  "\0vite/preload-helper.js": ["vite"],
  "\0rolldown/runtime.js": ["vite", "rolldown"]
};

/**
 * Packages whose code reaches the bundle through none of its modules or assets, each as the
 * chain of packages it is installed under: tailwind writes its base styles and theme into the
 * stylesheet it compiles
 */
const INLINED = [["tailwindcss"]];

/** Packages whose manifest names no license, by `name@version`, with the license their file holds */
const UNNAMED: Record<string, string> = {
  "runed@0.23.4": "MIT",
  "runed@0.25.0": "MIT",
  "svelte-toolbelt@0.7.1": "MIT",
  "svelte-toolbelt@0.10.7": "MIT"
};

const LICENSE_FILE = /^(licen[cs]e|copying|notice)([._-].*)?$/i;

type Manifest = { name?: string; version?: string; license?: unknown };

/** The directory of the package a file is part of: the nearest one with a named manifest */
const packageOf = (file: string): string => {
  for (let dir = dirname(file); ; dir = dirname(dir)) {
    const manifest = join(dir, "package.json");
    if (existsSync(manifest)) {
      const { name, version } = JSON.parse(readFileSync(manifest, "utf8")) as Manifest;
      if (name && version) return dir;
    }
    if (dirname(dir) === dir) throw new Error(`${file} is in no package`);
  }
};

/** Where `name` is installed for the code in `from`, as node finds it */
const installed = (name: string, from: string): string => {
  for (let dir = from; ; dir = dirname(dir)) {
    const candidate = join(dir, "node_modules", name);
    if (existsSync(join(candidate, "package.json"))) return realpathSync(candidate);
    if (dirname(dir) === dir) throw new Error(`${name} is not installed for ${from}`);
  }
};

const chain = (names: string[], root: string) =>
  names.reduce((from, name) => installed(name, from), root);

const describe = (dir: string): Bundled => {
  const manifest = JSON.parse(readFileSync(join(dir, "package.json"), "utf8")) as Manifest;
  const { name = "", version = "" } = manifest;
  const id = `${name}@${version}`;
  const license = typeof manifest.license === "string" ? manifest.license : UNNAMED[id];
  if (!license) throw new Error(`${id} names no license; add the one its files hold to UNNAMED`);
  const files = readdirSync(dir)
    .filter((file) => LICENSE_FILE.test(file))
    .toSorted();
  if (!files.length) throw new Error(`${id} ships no license file`);
  return { name, version, license, texts: files.map((f) => readFileSync(join(dir, f), "utf8")) };
};

/**
 * Writes to `out` the npm packages whose code the browser gets: those of the client build's
 * modules that kept any code, of its assets, and of the code bundled without either
 */
export const bundledPackages = (out: string): Plugin => {
  let root = "";
  return {
    name: "reseek:bundled-packages",
    apply: "build",
    applyToEnvironment: (environment) => environment.name === "client",
    configResolved(config) {
      root = config.root;
    },
    generateBundle(_options, bundle) {
      const files: string[] = [];
      const dirs = new Set<string>();
      for (const output of Object.values(bundle)) {
        if (output.type === "asset") {
          files.push(...output.originalFileNames.map((file) => resolve(root, file)));
          continue;
        }
        for (const [id, module] of Object.entries(output.modules)) {
          if (module.renderedLength === 0) continue;
          const writer = WRITTEN[id];
          if (writer) dirs.add(chain(writer, root));
          else if (isAbsolute(id)) files.push(id.replace(/\?.*$/, ""));
          else
            throw new Error(`the bundle holds ${JSON.stringify(id)}; add its package to WRITTEN`);
        }
      }
      for (const file of files) {
        if (file.includes("/node_modules/")) dirs.add(packageOf(realpathSync(file)));
      }
      for (const names of INLINED) dirs.add(chain(names, root));
      const packages = [...dirs].map(describe);
      writeFileSync(out, `${JSON.stringify(packages)}\n`);
    }
  };
};

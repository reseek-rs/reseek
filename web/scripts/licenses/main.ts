// Prints third-party.json from what `just licenses` gathered: cargo-about's JSON, the crates
// `cargo tree` lists for the two programs, and the npm packages the UI build bundles
import { readFileSync } from "node:fs";

import { type Bundled, type CargoAbout, collect, linkedCrates } from "./collect.ts";

const [cargoAbout, cargoTree, bundled] = process.argv.slice(2);
if (!cargoAbout || !cargoTree || !bundled) {
  throw new Error("usage: main.ts <cargo-about json> <cargo tree output> <bundled npm json>");
}
const notices = collect(
  JSON.parse(readFileSync(cargoAbout, "utf8")) as CargoAbout,
  linkedCrates(readFileSync(cargoTree, "utf8")),
  JSON.parse(readFileSync(bundled, "utf8")) as Bundled[]
);
process.stdout.write(`${JSON.stringify(notices, null, 2)}\n`);

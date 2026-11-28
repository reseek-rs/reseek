import type { NoticeGroup, Notices } from "../../src/lib/licenses/notices.ts";

/** What `cargo about generate --format json` writes, the parts read here */
export type CargoAbout = {
  licenses: {
    id: string;
    text: string;
    used_by: { crate: { name: string; version: string; source: string | null } }[];
  }[];
};

/** A package the UI bundles, with the license its manifest names and the texts it ships */
export type Bundled = { name: string; version: string; license: string; texts: string[] };

const CRATES_IO =
  /^(registry\+https:\/\/github\.com\/rust-lang\/crates\.io-index|sparse\+https:\/\/index\.crates\.io\/)$/;

/**
 * The crates in what `cargo tree --prefix none --format '{p}'` prints, as `name@version`. A crate
 * listed with a path is the workspace's own, under reseek's license, so it is left out
 */
export const linkedCrates = (tree: string): Set<string> => {
  const crates = new Set<string>();
  for (const line of tree.split("\n")) {
    const found = /^(\S+) v(\S+)(?: \(([^)]*)\))?(?: \(\*\))?$/.exec(line.trim());
    if (!found) continue;
    const [, name, version, source] = found;
    if (source?.startsWith("/")) continue;
    crates.add(`${name}@${version}`);
  }
  return crates;
};

/** A text as it is compared and kept: unix line ends, no blank lines around it */
const tidy = (text: string) =>
  text
    .replaceAll("\r\n", "\n")
    .replace(/^\s*\n/, "")
    .trimEnd();

/** Orders numbers in a name or a version by their value: 0.9 before 0.10 */
const sortKey = (text: string) => text.replace(/\d+/g, (digits) => digits.padStart(12, "0"));

/**
 * The notices of the crates in `linked`, from what cargo-about found for them, and of the
 * bundled npm packages, grouped by license. A text many packages share is kept once. Throws for a
 * linked crate cargo-about has no license for, or one from another registry than crates.io
 */
export const collect = (cargo: CargoAbout, linked: Set<string>, npm: Bundled[]): Notices => {
  type Entry = { name: string; version: string; registry: string; texts: Set<string> };
  const groups = new Map<string, Map<string, Entry>>();
  const add = (id: string, name: string, version: string, registry: string, text: string) => {
    let group = groups.get(id);
    if (!group) groups.set(id, (group = new Map()));
    const key = sortKey(`${name}\0${version}\0${registry}`);
    let entry = group.get(key);
    if (!entry) group.set(key, (entry = { name, version, registry, texts: new Set() }));
    entry.texts.add(tidy(text));
  };

  const found = new Set<string>();
  for (const license of cargo.licenses) {
    for (const { crate } of license.used_by) {
      const key = `${crate.name}@${crate.version}`;
      if (!linked.has(key)) continue;
      if (!crate.source || !CRATES_IO.test(crate.source)) {
        throw new Error(`${key} comes from ${crate.source ?? "no registry"}, not crates.io`);
      }
      found.add(key);
      add(license.id, crate.name, crate.version, "crates.io", license.text);
    }
  }
  const missing = [...linked].filter((key) => !found.has(key));
  if (missing.length) throw new Error(`cargo-about found no license for ${missing.join(", ")}`);

  for (const p of npm) {
    for (const text of p.texts) add(p.license, p.name, p.version, "npm", text);
  }

  const texts: string[] = [];
  const places = new Map<string, number>();
  const place = (text: string) => {
    let at = places.get(text);
    if (at === undefined) {
      at = texts.push(text) - 1;
      places.set(text, at);
    }
    return at;
  };
  const licenses: NoticeGroup[] = [...groups]
    .toSorted(([a, x], [b, y]) => y.size - x.size || (a < b ? -1 : a > b ? 1 : 0))
    .map(([id, group]) => ({
      id,
      packages: [...group]
        .toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([, { name, version, registry, texts: own }]) => ({
          name,
          version,
          registry,
          texts: [...own].map(place)
        }))
    }));
  return { texts, licenses };
};

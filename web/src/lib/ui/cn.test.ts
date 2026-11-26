import { describe, expect, it } from "vitest";

import { rule } from "#lib/testing/contrast.js";

import { cn } from "./cn";

// a scale of the theme, a utility on it, and a class of tailwind's own it conflicts with
const SCALES = [
  ["spacing", "h", "h-11"],
  ["radius", "rounded", "rounded-sm"],
  ["shadow", "shadow", "shadow-sm"],
  ["container", "max-w", "max-w-sm"],
  ["animate", "animate", "animate-spin"],
  ["ease", "ease", "ease-in"]
] as const;

describe("cn", () => {
  it("lets a later class win over every token of the theme", () => {
    const theme = rule("@theme");
    for (const [scale, utility, other] of SCALES) {
      const names = [...theme.matchAll(new RegExp(`--${scale}-([a-z0-9-]+):`, "g"))].map(
        (match) => match[1]
      );
      expect(names, scale).not.toEqual([]);
      for (const name of names) {
        expect(cn(`${utility}-${name}`, other), `${scale} ${name}`).toBe(other);
      }
    }
  });
});

import { describe, expect, it } from "vitest";

import { contrast, luminance, rows, rule, shown, token } from "#lib/testing/contrast.js";
import { DOTS } from "#lib/ui/dots.js";

// the colour tokens of app.css
describe("status text", () => {
  it("reads at 4.5:1 on every surface text sits on, in both themes", () => {
    for (const block of [rule("@theme"), rule(".dark")]) {
      for (const text of ["success-text", "warning-text", "danger"]) {
        const fg = luminance(shown(token(block, text)));
        for (const under of ["surface", "page", "surface-muted", "overlay", "hover"]) {
          const ratio = contrast(fg, luminance(shown(token(block, under))));
          expect(ratio, `${text} on ${under}`).toBeGreaterThanOrEqual(4.5);
        }
      }
    }
  });
});

const THEMES = [
  ["light", rule("@theme")],
  ["dark", rule(".dark")]
] as const;

/** the luminance of every surface and row an icon or a dot sits on in a theme's `block` */
const grounds = (block: string) => {
  // press is a control's tint inside a hovered line, and a sidebar link's while pressed
  const under: Record<string, number> = { ...rows(block) };
  for (const name of [
    "page",
    "surface-muted",
    "overlay",
    "press",
    "warning-soft",
    "success-soft"
  ]) {
    under[name] = luminance(shown(token(block, name)));
  }
  return under;
};

/** checks every token of `tokens` reaches 3:1 on every ground, in both themes */
const reachesThree = (tokens: string[]) => {
  for (const [theme, block] of THEMES) {
    for (const name of tokens) {
      const fg = luminance(shown(token(block, name)));
      for (const [ground, bg] of Object.entries(grounds(block))) {
        expect(contrast(fg, bg), `${theme} ${name} on ${ground}`).toBeGreaterThanOrEqual(3);
      }
    }
  }
};

describe("status icons", () => {
  it("reach 3:1 on every surface and row an icon sits on, in both themes", () => {
    reachesThree(["success-icon", "warning-icon", "danger"]);
  });
});

describe("status dots", () => {
  it("reach 3:1 on every surface and row a dot sits on, in both themes", () => {
    reachesThree(Object.values(DOTS).map((fill) => fill.replace(/^bg-/, "")));
  });
});

import { flushSync, mount, unmount } from "svelte";
import { afterEach, describe, expect, it } from "vitest";

import type { Report } from "#lib/analysis/analysis.js";
import { box } from "#lib/testing/state.svelte.js";

import Judgment from "./Judgment.svelte";

const report = (override: Report["override"]) => ({ override }) as Report;

describe("the judgment form", () => {
  let component: ReturnType<typeof mount> | null = null;
  afterEach(() => {
    if (component) unmount(component);
    component = null;
    document.body.innerHTML = "";
  });

  it("keeps an edit through a report read again and takes a judgment kept since", () => {
    const shown = box(report(null));
    component = mount(Judgment, {
      target: document.body,
      props: {
        target: { kind: "file", root: "downloads", path: "a.flac" },
        get report() {
          return shown.value;
        },
        onsaved: () => {}
      }
    });
    flushSync();
    const fake = [...document.querySelectorAll<HTMLElement>("[role=radio]")].find(
      (r) => r.textContent?.trim() === "Fake"
    )!;
    fake.click();
    flushSync();
    const textarea = document.querySelector("textarea")!;
    textarea.value = "clipped at 16 kHz";
    textarea.dispatchEvent(new Event("input", { bubbles: true }));
    flushSync();

    // another file's analysis finishing reads this one's report again, unchanged
    shown.value = report(null);
    flushSync();
    expect(textarea.value).toBe("clipped at 16 kHz");
    expect(fake.getAttribute("aria-checked")).toBe("true");

    shown.value = report({
      verdict: "legit",
      note: "heard it",
      at: "2026-11-20T10:00:00Z",
      by: "local:admin"
    });
    flushSync();
    expect(textarea.value).toBe("heard it");
    expect(fake.getAttribute("aria-checked")).toBe("false");
  });
});

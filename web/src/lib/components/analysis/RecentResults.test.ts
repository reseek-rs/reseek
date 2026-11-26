import { mount, unmount } from "svelte";
import { afterEach, describe, expect, it, vi } from "vitest";

import { jobs } from "#lib/analysis/jobs.svelte.js";
import { flush } from "#lib/testing/flush.js";
import { fakeNetwork } from "#lib/testing/network.js";

import RecentResults from "./RecentResults.svelte";

const report = (name: string) => ({
  path: `/music/${name}`,
  root: "downloads",
  relativePath: name,
  stale: false,
  verdict: "consistent",
  analyzedAt: "2026-11-20T10:00:00Z"
});

describe("the recent results", () => {
  let component: ReturnType<typeof mount> | null = null;
  afterEach(() => {
    if (component) unmount(component);
    component = null;
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("show the latest read when an earlier one answers after it", async () => {
    const answers: ((names: string[]) => void)[] = [];
    fakeNetwork((url) => {
      if (url.pathname.endsWith("/api/v0/analysis") && !url.searchParams.has("offset")) {
        return new Promise((done) => answers.push((names) => done(names.map((n) => report(n)))));
      }
      return url.pathname.endsWith("/jobs") ? { jobs: [], backlog: 0 } : [];
    });
    component = mount(RecentResults, { target: document.body });
    await flush();
    answers.shift()!(["a.flac"]);
    await flush();
    jobs.finished++;
    await flush();
    jobs.finished++;
    await flush();
    const [older, newer] = answers;
    newer(["c.flac", "b.flac", "a.flac"]);
    await flush();
    older(["b.flac", "a.flac"]);
    await flush();
    expect(
      [...document.querySelectorAll("li button")].map((b) => b.getAttribute("aria-label"))
    ).toEqual(["Open in Analysis: c.flac", "Open in Analysis: b.flac", "Open in Analysis: a.flac"]);
  });
});

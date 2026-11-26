import { mount, unmount } from "svelte";
import { afterEach, describe, expect, it, vi } from "vitest";

import { live } from "#lib/live/live.svelte.js";
import { transfer } from "#lib/testing/fixtures.js";
import { flush } from "#lib/testing/flush.js";
import { connected } from "#lib/testing/live.js";

import ProfileTransfers from "./ProfileTransfers.svelte";

vi.mock("$app/navigation", () => ({ goto: vi.fn() }));

describe("a user's transfers", () => {
  afterEach(() => {
    live.stop();
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("marks a listed transfer removed when it is removed", async () => {
    const t = transfer({ state: "Completed, Succeeded", endedAt: "2026-10-26T10:05:00Z" });
    const net = connected((url) =>
      url.pathname.endsWith("/users/alice/transfers") ? { transfers: [t], total: 1 } : undefined
    );
    const component = mount(ProfileTransfers, {
      target: document.body,
      props: { username: "alice" }
    });
    await flush();
    const row = () => document.querySelector("[role=rowgroup] [role=row]")!;
    expect(row().classList.contains("opacity-40")).toBe(false);
    net.frame("transfers.removed", { id: t.id, username: "alice", direction: "Download" });
    await flush();
    expect(row().classList.contains("opacity-40")).toBe(true);
    unmount(component);
  });
});

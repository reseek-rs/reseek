import { flushSync, mount, unmount } from "svelte";
import { afterEach, describe, expect, it, vi } from "vitest";

import { live } from "#lib/live/live.svelte.js";
import { session } from "#lib/session.svelte.js";
import { userSummary } from "#lib/testing/fixtures.js";
import { flush } from "#lib/testing/flush.js";
import { connected } from "#lib/testing/live.js";
import { fakeNetwork } from "#lib/testing/network.js";

import BlockedList from "./BlockedList.svelte";

describe("importing blocks", () => {
  afterEach(() => {
    session.principal = null;
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  const button = (text: string) =>
    [...document.querySelectorAll("button")].find((b) => b.textContent?.trim() === text)!;

  it("sends once however often Import is pressed, and opens again without the last error", async () => {
    session.principal = { name: "me", role: "Administrator" } as never;
    let release!: () => void;
    const held = new Promise<void>((done) => (release = done));
    const net = fakeNetwork(async (url, method) => {
      if (method === "GET")
        return url.pathname.endsWith("/users/blocked") ? { users: [], rules: null } : undefined;
      await held;
      return { status: 400, body: { error: "not a block list" } };
    });
    const component = mount(BlockedList, { target: document.body });
    await flush();
    button("Import").click();
    flushSync();
    const box = document.querySelector<HTMLTextAreaElement>("[aria-label='Blocks to import']")!;
    box.value = "alice";
    box.dispatchEvent(new Event("input", { bubbles: true }));
    flushSync();
    const send = () =>
      [...document.querySelectorAll<HTMLElement>("[role=dialog] button")].find(
        (b) => b.textContent?.trim() === "Import"
      )!;
    send().click();
    send().click();
    release();
    await flush();
    expect(net.requests.filter((r) => r.method === "POST")).toHaveLength(1);
    expect(document.body.textContent).toContain("not a block list");

    button("Cancel").click();
    await flush();
    button("Import").click();
    await flush();
    expect(document.body.textContent).not.toContain("not a block list");
    unmount(component);
  });
});

describe("the blocked list", () => {
  afterEach(() => {
    live.stop();
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("reads a block that changed only once nothing holds the list", async () => {
    const bob = {
      username: "bob",
      addresses: [],
      reason: "",
      scopes: ["browse"],
      source: "api",
      createdAt: "2026-10-26T10:00:00Z"
    };
    const net = connected((url) =>
      url.pathname.endsWith("/users/blocked") ? { users: [bob], rules: null } : undefined
    );
    const component = mount(BlockedList, { target: document.body });
    await flush();
    const reads = () => net.requests.filter((r) => r.url.pathname.endsWith("/blocked")).length;
    const before = reads();

    // the pointer pressed in the list holds it, as for a selection
    const rows = document.querySelector("[role=rowgroup]")!;
    rows.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    net.frame("users.updated", {
      ...userSummary("alice"),
      block: { reason: "", scopes: [] }
    } as never);
    await flush();
    expect(reads()).toBe(before);
    window.dispatchEvent(new PointerEvent("pointerup"));
    await flush();
    expect(reads()).toBe(before + 1);
    unmount(component);
  });
});

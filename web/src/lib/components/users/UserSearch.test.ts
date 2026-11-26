import { flushSync, mount, unmount } from "svelte";
import { afterEach, describe, expect, it, vi } from "vitest";

import { flush } from "#lib/testing/flush.js";
import { fakeNetwork } from "#lib/testing/network.js";
import { TYPING_PAUSE } from "#lib/ui/keys.js";

import UserSearch from "./UserSearch.svelte";

vi.mock("$app/navigation", () => ({ goto: vi.fn() }));

describe("the user search box", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("asks again for a text whose answer came after the text moved on", async () => {
    let release!: () => void;
    const held = new Promise<void>((done) => (release = done));
    const net = fakeNetwork(async (url) => {
      if (!url.pathname.endsWith("/api/v0/users")) return undefined;
      if (url.searchParams.get("q") === "al") await held;
      return { users: [], total: 0 };
    });
    const component = mount(UserSearch, { target: document.body, props: { onsearch: () => {} } });
    const box = document.querySelector<HTMLInputElement>("input")!;
    const type = (text: string) => {
      box.value = text;
      box.dispatchEvent(new Event("input", { bubbles: true }));
      flushSync();
    };
    const asked = (q: string) =>
      net.requests.filter((r) => r.url.searchParams.get("q") === q).length;

    type("al");
    await vi.waitFor(() => expect(asked("al")).toBe(1));
    type("ali");
    release();
    await flush();
    type("al");
    await new Promise((done) => setTimeout(done, TYPING_PAUSE * 2));
    await flush();
    expect(asked("al")).toBe(2);
    unmount(component);
  });
});

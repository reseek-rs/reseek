import { mount, unmount } from "svelte";
import { afterEach, describe, expect, it } from "vitest";

import { app } from "#lib/app.svelte.js";
import { live } from "#lib/live/live.svelte.js";
import { flush } from "#lib/testing/flush.js";
import { connected } from "#lib/testing/live.js";

import PublicFeed from "./PublicFeed.svelte";

describe("the public room feed", () => {
  afterEach(() => {
    live.stop();
    app.state = null;
    document.body.innerHTML = "";
  });

  it("follows the feed only while it is open, and filters what came", async () => {
    app.state = { server: { isConnected: true } } as never;
    const net = connected(() => undefined, "Administrator");
    const feed = mount(PublicFeed, { target: document.body, props: {} });
    await flush();
    expect(live.topics).toContain("public_rooms");
    expect(document.body.textContent).toContain("Waiting for messages");

    const said = (roomName: string, username: string, message: string) =>
      net.frame("rooms.public_message", {
        roomName,
        username,
        message,
        receivedAt: "2026-11-05T10:00:00Z"
      });
    said("indie", "alice", "anyone have the new rip?");
    said("jazz", "bob", "coltrane all night");
    await flush();
    const lines = () =>
      [...document.querySelectorAll('[role="log"] > div')].map((l) => l.textContent);
    expect(lines()).toHaveLength(2);
    expect(lines()[0]).toContain("#indie");

    const box = document.querySelector<HTMLInputElement>('input[aria-label="Filter messages"]')!;
    box.value = "JAZZ";
    box.dispatchEvent(new Event("input", { bubbles: true }));
    await flush();
    expect(lines()).toHaveLength(1);
    expect(lines()[0]).toContain("coltrane");

    unmount(feed);
    expect(live.topics).not.toContain("public_rooms");
  });

  it("stays where it is while a name's card in it is open, and follows the newest after", async () => {
    app.state = { server: { isConnected: true }, user: { username: "me" } } as never;
    const net = connected(() => undefined, "Administrator");
    const feed = mount(PublicFeed, { target: document.body, props: {} });
    await flush();
    let said = 0;
    const say = async () => {
      said += 1;
      net.frame("rooms.public_message", {
        roomName: "indie",
        username: "alice",
        message: `message ${said}`,
        receivedAt: "2026-11-05T10:00:00Z"
      });
      await flush();
    };
    await say();
    const log = document.querySelector<HTMLElement>('[role="log"]')!;
    // the times the feed was taken to its newest message
    let followed = 0;
    Object.defineProperty(log, "scrollTop", {
      get: () => 0,
      set: () => {
        followed += 1;
      }
    });
    log
      .querySelector("a[href]:not([href*='rooms'])")!
      .dispatchEvent(new KeyboardEvent("keydown", { key: " ", bubbles: true, cancelable: true }));
    await flush();
    expect(document.querySelector('[role="dialog"][aria-label="Profile of alice"]')).not.toBeNull();
    await say();
    expect(followed).toBe(0);
    document
      .querySelector('[role="dialog"]')!
      .dispatchEvent(
        new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true })
      );
    await flush();
    await say();
    expect(followed).toBeGreaterThan(0);
    unmount(feed);
  });

  it("counts the messages that come while scrolled up, and no change of the filter", async () => {
    app.state = { server: { isConnected: true } } as never;
    const net = connected(() => undefined, "Administrator");
    const feed = mount(PublicFeed, { target: document.body, props: {} });
    await flush();
    const say = async (message: string) => {
      net.frame("rooms.public_message", {
        roomName: "indie",
        username: "alice",
        message,
        receivedAt: "2026-11-05T10:00:00Z"
      });
      await flush();
    };
    await say("one");
    await say("two");
    // jsdom lays nothing out: the log is taller than its view, which is at its top
    const log = document.querySelector<HTMLElement>('[role="log"]')!;
    Object.defineProperty(log, "scrollHeight", { value: 1000 });
    log.dispatchEvent(new Event("scroll"));
    await flush();
    const button = () => document.querySelector("section > div > button")?.textContent?.trim();
    expect(button()).toBe("Jump to latest");
    await say("three");
    expect(button()).toBe("1 new");

    const filter = document.querySelector<HTMLInputElement>('input[aria-label="Filter messages"]')!;
    for (const text of ["two", ""]) {
      filter.value = text;
      filter.dispatchEvent(new Event("input", { bubbles: true }));
      await flush();
      expect(button()).toBe("1 new");
    }
    unmount(feed);
  });
});

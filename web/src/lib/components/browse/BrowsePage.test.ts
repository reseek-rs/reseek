import { flushSync, mount, unmount } from "svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { Schemas } from "#lib/api/client.js";
import { app } from "#lib/app.svelte.js";
import { browser } from "#lib/browse/browse.svelte.js";
import { live } from "#lib/live/live.svelte.js";
import { session } from "#lib/session.svelte.js";
import { sharedFile } from "#lib/testing/fixtures.js";
import { flushFaked } from "#lib/testing/flush.js";
import { connected } from "#lib/testing/live.js";
import { fakeNetwork } from "#lib/testing/network.js";
import { box } from "#lib/testing/state.svelte.js";

import BrowsePage from "./BrowsePage.svelte";

const progress = (percentComplete: number): Schemas["BrowseProgress"] => ({
  username: "alice",
  percentComplete,
  bytesTransferred: percentComplete,
  bytesRemaining: 100 - percentComplete,
  size: 100
});

describe("browse progress", () => {
  let component: ReturnType<typeof mount> | null = null;

  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    if (component) unmount(component);
    live.stop();
    browser.clear();
    session.principal = null;
    app.state = null;
    vi.useRealTimers();
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("updates the same line as frames arrive, then shows the tree", async () => {
    let answer!: (listing: Schemas["BrowseResponse"]) => void;
    const listing = new Promise<Schemas["BrowseResponse"]>((resolve) => (answer = resolve));
    const net = connected((url) =>
      url.pathname.endsWith("/users/alice/browse") ? listing : undefined
    );
    component = mount(BrowsePage, {
      target: document.body,
      props: { user: "alice", path: null }
    });
    await flushFaked();
    const line = () => document.querySelector("[role=status] p")!;
    expect(line().textContent).toContain("Loading shares... 0%");
    const before = line();

    net.frame("users.browse_progress", progress(40));
    await flushFaked();
    expect(line()).toBe(before);
    expect(line().textContent).toContain("Loading shares... 40%");
    // another user's browse doesn't move this one
    net.frame("users.browse_progress", { ...progress(90), username: "bob" });
    await flushFaked();
    expect(line().textContent).toContain("Loading shares... 40%");

    answer({
      directories: [
        { name: "Music", fileCount: 0, files: [] },
        { name: "Music\\Album", fileCount: 0, files: [] }
      ],
      directoryCount: 2,
      lockedDirectories: [],
      lockedDirectoryCount: 0
    });
    await flushFaked();
    // the first folder is open, a hint that folders open
    const rows = [...document.querySelectorAll("[role=treegrid] [role=rowgroup] [data-line]")];
    expect(rows.map((r) => r.getAttribute("data-line"))).toEqual([
      "folder:Music",
      "folder:Music\\Album"
    ]);
  });

  it("ticks a folder over the files under it, not those of a locked folder", async () => {
    fakeNetwork((url) =>
      url.pathname.endsWith("/users/alice/browse")
        ? {
            directories: [
              { name: "Music", fileCount: 1, files: [sharedFile("1.flac")] },
              { name: "Music\\CD2", fileCount: 1, files: [sharedFile("2.flac")] },
              { name: "Video", fileCount: 1, files: [sharedFile("x.mkv")] }
            ],
            directoryCount: 3,
            lockedDirectories: [
              { name: "Music\\Locked", fileCount: 1, files: [sharedFile("3.flac")] }
            ],
            lockedDirectoryCount: 1
          }
        : undefined
    );
    session.principal = { name: "me", role: "Administrator" } as never;
    app.state = { server: { isConnected: true }, user: { username: "me" } } as never;
    component = mount(BrowsePage, {
      target: document.body,
      props: { user: "alice", path: null }
    });
    await flushFaked();
    await flushFaked();
    const tick = (name: string) =>
      document.querySelector<HTMLElement>(
        `[role=checkbox][aria-label="Select every file in ${name}"]`
      )!;
    tick("Music").click();
    flushSync();
    expect([...browser.selected].toSorted()).toEqual(["Music\\1.flac", "Music\\CD2\\2.flac"]);
    tick("Video").click();
    flushSync();
    expect(document.querySelector("[aria-label='Selected files']")!.textContent).toContain(
      "3 files"
    );
  });

  it("downloads a folder whose line is ticked in it, and some of its files loose", async () => {
    const net = fakeNetwork((url) => {
      if (url.pathname.endsWith("/transfers/downloads/jobs")) return { id: "j", groups: [] };
      if (!url.pathname.endsWith("/users/alice/browse")) return undefined;
      return {
        directories: [
          { name: "Album", fileCount: 2, files: [sharedFile("1.flac"), sharedFile("2.flac")] }
        ],
        directoryCount: 1,
        lockedDirectories: [],
        lockedDirectoryCount: 0
      };
    });
    session.principal = { name: "me", role: "Administrator" } as never;
    app.state = { server: { isConnected: true }, user: { username: "me" } } as never;
    component = mount(BrowsePage, {
      target: document.body,
      props: { user: "alice", path: null }
    });
    await flushFaked();
    await flushFaked();
    /** ticks `label`, downloads the selection and gives the folder the job was asked in */
    const download = async (label: string) => {
      document.querySelector<HTMLElement>(`[aria-label="${label}"]`)!.click();
      flushSync();
      [...document.querySelectorAll("button")]
        .find((b) => b.textContent?.trim() === "Download")!
        .click();
      await flushFaked();
      const jobs = net.requests.filter((r) => r.url.pathname.endsWith("/transfers/downloads/jobs"));
      const body = jobs.at(-1)!.body as { groups: { folder: string | null }[] };
      return body.groups.map((g) => g.folder);
    };
    document.querySelector<HTMLElement>("[aria-label='Open Album']")!.click();
    flushSync();
    expect(await download("Select 1.flac")).toEqual([null]);
    expect(await download("Select every file in Album")).toEqual(["Album"]);
  });

  it("downloads a ticked folder from its own line, however few folders the ones above hold", async () => {
    const dir = (name: string, files: string[] = []) => ({
      name,
      fileCount: files.length,
      files: files.map((f) => sharedFile(f))
    });
    const listings: Record<string, ReturnType<typeof dir>[]> = {
      // the album alone under every folder above it, to the share's root
      alice: [
        dir("@@abcde"),
        dir("@@abcde\\Music"),
        dir("@@abcde\\Music\\Artist"),
        dir("@@abcde\\Music\\Artist\\Album", ["1.flac", "2.flac"])
      ],
      // the artist beside another folder, holding only the album
      bob: [
        dir("@@abcde"),
        dir("@@abcde\\Artist"),
        dir("@@abcde\\Artist\\Album", ["1.flac", "2.flac"]),
        dir("@@abcde\\Other", ["x.flac"])
      ]
    };
    const net = fakeNetwork((url) => {
      if (url.pathname.endsWith("/transfers/downloads/jobs")) return { id: "j", groups: [] };
      const user = url.pathname.match(/users\/(\w+)\/browse$/)?.[1];
      if (!user) return undefined;
      return {
        directories: listings[user],
        directoryCount: listings[user].length,
        lockedDirectories: [],
        lockedDirectoryCount: 0
      };
    });
    session.principal = { name: "me", role: "Administrator" } as never;
    app.state = { server: { isConnected: true }, user: { username: "me" } } as never;
    /** opens every folder line, ticks the line of `name`, downloads and clears the selection */
    const download = async (name: string) => {
      for (;;) {
        const closed = document.querySelector<HTMLElement>("[aria-label^='Open ']");
        if (!closed) break;
        closed.click();
        flushSync();
      }
      document.querySelector<HTMLElement>(`[aria-label="Select every file in ${name}"]`)!.click();
      flushSync();
      [...document.querySelectorAll("button")]
        .find((b) => b.textContent?.trim() === "Download")!
        .click();
      await flushFaked();
      [...document.querySelectorAll("button")]
        .find((b) => b.textContent?.trim() === "Clear")!
        .click();
      flushSync();
      const jobs = net.requests.filter((r) => r.url.pathname.endsWith("/transfers/downloads/jobs"));
      const body = jobs.at(-1)!.body as { groups: { folder: string | null }[] };
      return body.groups.map((g) => g.folder);
    };
    for (const user of ["alice", "bob"]) {
      if (component) unmount(component);
      browser.clear();
      component = mount(BrowsePage, { target: document.body, props: { user, path: null } });
      await flushFaked();
      await flushFaked();
      const artist = user === "alice" ? "@@abcde\\Music\\Artist" : "@@abcde\\Artist";
      // as each line's own Download button sends it
      expect(await download("Album"), user).toEqual([`${artist}\\Album`]);
      expect(await download("Artist"), user).toEqual([artist]);
    }
  });

  it("forgets a folder line's tick once anything unticks a file under it", async () => {
    const net = fakeNetwork((url) => {
      if (url.pathname.endsWith("/transfers/downloads/jobs")) return { id: "j", groups: [] };
      if (!url.pathname.endsWith("/users/alice/browse")) return undefined;
      const dir = (name: string, files: string[] = []) => ({
        name,
        fileCount: files.length,
        files: files.map((f) => sharedFile(f))
      });
      const directories = [
        dir("@@abcde"),
        dir("@@abcde\\Artist"),
        dir("@@abcde\\Artist\\Album", ["1.flac", "2.flac"]),
        dir("@@abcde\\Other", ["x.flac"])
      ];
      return {
        directories,
        directoryCount: directories.length,
        lockedDirectories: [],
        lockedDirectoryCount: 0
      };
    });
    session.principal = { name: "me", role: "Administrator" } as never;
    app.state = { server: { isConnected: true }, user: { username: "me" } } as never;
    component = mount(BrowsePage, { target: document.body, props: { user: "alice", path: null } });
    await flushFaked();
    await flushFaked();
    for (;;) {
      const closed = document.querySelector<HTMLElement>("[aria-label^='Open ']");
      if (!closed) break;
      closed.click();
      flushSync();
    }
    const click = (label: string) => {
      document.querySelector<HTMLElement>(`[aria-label="${label}"]`)!.click();
      flushSync();
    };
    const button = (text: string) =>
      [...document.querySelectorAll("button")].find((b) => b.textContent?.trim() === text)!;
    /** downloads the selection, clears it and gives the folder the job was asked in */
    const download = async () => {
      button("Download").click();
      await flushFaked();
      button("Clear").click();
      flushSync();
      const jobs = net.requests.filter((r) => r.url.pathname.endsWith("/transfers/downloads/jobs"));
      const body = jobs.at(-1)!.body as { groups: { folder: string | null }[] };
      return body.groups.map((g) => g.folder);
    };
    const album = "@@abcde\\Artist\\Album";

    // the artist's line, then the album's after a Clear
    click("Select every file in Artist");
    expect(await download()).toEqual(["@@abcde\\Artist"]);
    click("Select every file in Album");
    expect(await download(), "after a Clear").toEqual([album]);

    // the album unticked by its line and ticked again
    click("Select every file in Artist");
    click("Select every file in Album");
    click("Select every file in Album");
    expect(await download(), "the album's line unticked").toEqual([album]);

    // the album's files unticked one by one and ticked again
    click("Select every file in Artist");
    for (const file of ["1.flac", "2.flac", "1.flac", "2.flac"]) click(`Select ${file}`);
    expect(await download(), "the files unticked").toEqual([album]);

    // everything unticked through Select all, then the album
    click("Select every file in Artist");
    click("Select all");
    click("Select all");
    expect(browser.selected.size, "Select all unticks").toBe(0);
    click("Select every file in Album");
    expect(await download(), "Select all").toEqual([album]);
  });

  it("leaves the next user's tree where it is when the one before answers late", async () => {
    let answerAlice!: (listing: Schemas["BrowseResponse"]) => void;
    const folders = (...names: string[]): Schemas["BrowseResponse"] => ({
      directories: names.map((name) => ({ name, fileCount: 0, files: [] })),
      directoryCount: names.length,
      lockedDirectories: [],
      lockedDirectoryCount: 0
    });
    fakeNetwork((url) => {
      if (url.pathname.endsWith("/users/alice/browse")) {
        return new Promise((resolve) => (answerAlice = resolve));
      }
      if (url.pathname.endsWith("/users/bob/browse")) {
        return folders("Music", "Music\\Album", "Other", "Other\\Deep");
      }
      return undefined;
    });
    const address = box<{ user: string; path: string | null }>({
      user: "alice",
      path: "Other\\Deep"
    });
    component = mount(BrowsePage, {
      target: document.body,
      props: {
        get user() {
          return address.value.user;
        },
        get path() {
          return address.value.path;
        }
      }
    });
    await flushFaked();
    address.value = { user: "bob", path: null };
    await flushFaked();
    await flushFaked();
    const lines = () =>
      [...document.querySelectorAll("[role=treegrid] [role=rowgroup] [data-line]")].map((r) =>
        r.getAttribute("data-line")
      );
    expect(lines()).toEqual(["folder:Music", "folder:Music\\Album", "folder:Other"]);

    answerAlice(folders("Other", "Other\\Deep"));
    await flushFaked();
    await flushFaked();
    expect(browser.username).toBe("bob");
    expect(lines()).toEqual(["folder:Music", "folder:Music\\Album", "folder:Other"]);
  });
});

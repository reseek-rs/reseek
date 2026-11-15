import { afterEach, describe, expect, it, vi } from "vitest";

import type { Schemas } from "#lib/api/client.js";
import { fakeNetwork } from "#lib/testing/network.js";

import {
  downloadFolderPlan,
  downloadPlan,
  enqueueJob,
  enqueueLoose,
  enqueueSelection
} from "./downloads";

const file = (filename: string, size = 10): Schemas["File"] => ({
  filename,
  size,
  code: 1,
  extension: "",
  isLocked: false,
  attributeCount: 0,
  attributes: []
});

const plan = (
  files: string[],
  topOf: (path: string) => string,
  separator: "\\" | "/" = "\\",
  whole?: (folder: string) => boolean,
  ticked?: (folder: string) => boolean
) =>
  downloadPlan(
    files.map((f) => file(f)),
    topOf,
    separator,
    whole,
    ticked
  ).map((g): [string | null, string[]] => [g.folder, g.files.map((f) => f.filename)]);

const folder = (name: string, files: string[]) =>
  downloadFolderPlan(
    name,
    files.map((f) => file(f)),
    "\\"
  ).map((g): [string | null, string[]] => [g.folder, g.files.map((f) => f.filename)]);

describe("downloading a selection", () => {
  it("sends it in the deepest folder holding it when it has folders below, its own files first", () => {
    expect(
      plan(
        [
          "C:\\Music\\Album\\Disc 1\\Extras\\x.mp3",
          "C:\\Music\\Album\\Disc 1\\01.mp3",
          "C:\\Music\\Album\\cover.jpg",
          "C:\\Music\\Album\\Disc 1\\Extras\\y.mp3"
        ],
        () => "C:"
      )
    ).toEqual([
      [
        "C:\\Music\\Album",
        [
          "C:\\Music\\Album\\cover.jpg",
          "C:\\Music\\Album\\Disc 1\\Extras\\x.mp3",
          "C:\\Music\\Album\\Disc 1\\Extras\\y.mp3",
          "C:\\Music\\Album\\Disc 1\\01.mp3"
        ]
      ]
    ]);
    // the files of one folder, with nothing below it, go where the pattern puts them
    expect(plan(["top/sub/a", "top/sub/b"], () => "top", "/")).toEqual([
      [null, ["top/sub/a", "top/sub/b"]]
    ]);
  });

  it("sends a folder ticked whole in it though it holds only files", () => {
    const album = ["@@a\\Music\\Album\\1.flac", "@@a\\Music\\Album\\2.flac"];
    const whole = (folder: string) => folder === "@@a\\Music\\Album";
    expect(plan(album, () => "@@a\\Music", "\\", whole)).toEqual([["@@a\\Music\\Album", album]]);
    // some of its files, or a folder the tree does not show ticked, go where the pattern puts them
    expect(
      plan(
        album.slice(0, 1),
        () => "@@a\\Music",
        "\\",
        () => false
      )
    ).toEqual([[null, album.slice(0, 1)]]);
    // the other tops of the selection follow, as they do once anything is nested
    const top = (path: string) => path.split("\\").slice(0, 2).join("\\");
    expect(plan([...album, "@@a\\Video\\x.mkv"], top, "\\", whole)).toEqual([
      ["@@a\\Music\\Album", album],
      ["@@a\\Video", ["@@a\\Video\\x.mkv"]]
    ]);
  });

  it("sends a folder from the line that was ticked, though the ones above hold only it", () => {
    const album = ["@@a\\Music\\Artist\\Album\\1.flac", "@@a\\Music\\Artist\\Album\\2.flac"];
    // every folder above the album holds only it, so each reads ticked whole
    const chain = ["@@a", "@@a\\Music", "@@a\\Music\\Artist", "@@a\\Music\\Artist\\Album"];
    const whole = (folder: string) => chain.includes(folder);
    const sent = (line: string) =>
      plan(
        album,
        () => "@@a",
        "\\",
        whole,
        (folder) => folder === line
      );
    // as each line's own Download button sends it
    for (const line of chain.slice(1)) {
      expect(sent(line), line).toEqual([[line, album]]);
      expect(folder(line, album), line).toEqual([[line, album]]);
    }
    expect(plan(album, () => "@@a", "\\", whole)).toEqual([["@@a\\Music\\Artist\\Album", album]]);
  });

  it("sends every top's files in their folder once anything in the selection is nested", () => {
    const top = (path: string) => path.split("\\").slice(0, 2).join("\\");
    expect(
      plan(["@@a\\Music\\A\\1.flac", "@@a\\Video\\x.mkv", "@@a\\Music\\A\\CD2\\2.flac"], top)
    ).toEqual([
      ["@@a\\Music\\A", ["@@a\\Music\\A\\1.flac", "@@a\\Music\\A\\CD2\\2.flac"]],
      ["@@a\\Video", ["@@a\\Video\\x.mkv"]]
    ]);
    // with nothing nested anywhere, every top's files go where the pattern puts them
    expect(plan(["@@a\\Music\\A\\1.flac", "@@a\\Video\\x.mkv"], top)).toEqual([
      [null, ["@@a\\Music\\A\\1.flac"]],
      [null, ["@@a\\Video\\x.mkv"]]
    ]);
  });

  it("sends files that are in no folder in none", () => {
    expect(plan(["a.flac", "b.flac"], () => "")).toEqual([[null, ["a.flac", "b.flac"]]]);
    // a file at a peer's root is below no folder but the root
    expect(plan(["\\x.flac"], () => "\\")).toEqual([[null, ["\\x.flac"]]]);
    expect(plan(["/x.flac"], () => "/", "/")).toEqual([[null, ["/x.flac"]]]);
    expect(plan(["/x.flac", "/home/y.flac"], () => "/", "/")).toEqual([
      [null, ["/x.flac", "/home/y.flac"]]
    ]);
  });
});

describe("downloading search results of several users", () => {
  afterEach(() => vi.unstubAllGlobals());

  /** The groups an enqueue of `picked` sends, as user, folder, search and files */
  const sent = async (
    picked: { username: string; file: Schemas["File"] }[],
    whole?: (username: string, folder: string) => boolean
  ) => {
    const net = fakeNetwork(() => ({ id: "j", groups: [] }));
    await enqueueSelection(picked, "s1", whole);
    const body = net.requests[0].body as {
      groups: {
        username: string;
        folder: string | null;
        searchId: string;
        files: { filename: string }[];
      }[];
    };
    return body.groups.map((g) => [
      g.username,
      g.folder,
      g.searchId,
      g.files.map((f) => f.filename)
    ]);
  };

  it("sends each user's selection as the browse page plans it, a share a top", async () => {
    expect(
      await sent([
        { username: "alice", file: file("@@a\\Music\\Rock\\A\\01.flac") },
        { username: "bob", file: file("/home/bob/Album/01.flac") },
        { username: "alice", file: file("@@a\\Music\\Jazz\\B\\01.flac") }
      ])
    ).toEqual([
      // alice's files are in folders below the deepest holding them, so they keep their tree
      [
        "alice",
        "@@a\\Music",
        "s1",
        ["@@a\\Music\\Rock\\A\\01.flac", "@@a\\Music\\Jazz\\B\\01.flac"]
      ],
      ["bob", null, "s1", ["/home/bob/Album/01.flac"]]
    ]);
  });

  it("sends a folder ticked whole in it, and loose files where the pattern puts them", async () => {
    const album = ["@@a\\Music\\A\\1.flac", "@@a\\Music\\A\\2.flac"];
    const picked = album.map((f) => ({ username: "alice", file: file(f) }));
    const whole = (username: string, folder: string) =>
      username === "alice" && folder === "@@a\\Music\\A";
    expect(await sent(picked, whole)).toEqual([["alice", "@@a\\Music\\A", "s1", album]]);
    expect(await sent(picked)).toEqual([["alice", null, "s1", album]]);
  });

  it("sends files asked for loose in no folder, so each goes where the pattern puts it", async () => {
    const net = fakeNetwork(() => ({ id: "j", groups: [] }));
    await enqueueLoose(
      [
        { username: "alice", file: file("@@a\\Music\\Rock\\A\\01.flac") },
        { username: "alice", file: file("@@a\\Music\\Jazz\\B\\01.flac") }
      ],
      "s1"
    );
    const body = net.requests[0].body as { groups: { folder: string | null }[] };
    expect(body.groups.map((g) => g.folder)).toEqual([null]);
  });
});

describe("downloading a folder", () => {
  it("sends a peer's root as the folder of the files below it", () => {
    const root = (name: string, files: string[], separator: "\\" | "/") =>
      downloadFolderPlan(
        name,
        files.map((f) => file(f)),
        separator
      ).map((g) => g.folder);
    expect(root("/", ["/x.flac", "/home/y.flac"], "/")).toEqual(["/"]);
    expect(root("\\", ["\\x.flac"], "\\")).toEqual(["\\"]);
  });

  it("sends every file in the folder, its own files first, each folder's below together", () => {
    expect(
      folder("@@a\\Artist", [
        "@@a\\Artist\\Album\\CD1\\01.flac",
        "@@a\\Artist\\Album\\cover.jpg",
        "@@a\\Artist\\notes.txt",
        "@@a\\Artist\\Album\\CD1\\02.flac"
      ])
    ).toEqual([
      [
        "@@a\\Artist",
        [
          "@@a\\Artist\\notes.txt",
          "@@a\\Artist\\Album\\CD1\\01.flac",
          "@@a\\Artist\\Album\\CD1\\02.flac",
          "@@a\\Artist\\Album\\cover.jpg"
        ]
      ]
    ]);
  });
});

describe("enqueueing a job", () => {
  afterEach(() => vi.unstubAllGlobals());

  /** Enqueues a job whose groups the daemon answers with `answers`, in order */
  const outcome = (answers: Omit<Schemas["EnqueuedDownloadGroup"], "username" | "enqueued">[]) => {
    fakeNetwork(() => ({
      id: "j",
      groups: answers.map((a) => ({ username: "", enqueued: [], ...a }))
    }));
    return enqueueJob(
      answers.map((_, i) => ({
        username: `user${i}`,
        files: [
          { filename: `${i}\\a.flac`, size: 1 },
          { filename: `${i}\\b.flac`, size: 1 }
        ]
      }))
    );
  };
  const offline = { failures: [], error: { error: "User x appears to be offline" } };

  it("counts a group refused whole as every one of its files not queued", async () => {
    const refusedOne = { message: "Skipped: Already in progress", filename: "1\\a.flac" };
    expect(await outcome([offline, { failures: [refusedOne], error: null }])).toEqual({
      kind: "partial",
      text: "Queued 1 of 4 files. Could not queue 2 files from user0: User x appears to be offline; 1\\a.flac: Skipped: Already in progress"
    });
    expect(await outcome([offline, offline])).toEqual({
      kind: "failed",
      text: "User x appears to be offline"
    });
    expect(await outcome([{ failures: [], error: null }])).toEqual({
      kind: "done",
      text: "2 files queued"
    });
  });
});

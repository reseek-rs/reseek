import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { Schemas } from "#lib/api/client.js";
import { parseFilters } from "#lib/files/filters.js";
import { live } from "#lib/live/live.svelte.js";
import { sharedFile } from "#lib/testing/fixtures.js";
import { flushFaked } from "#lib/testing/flush.js";
import { connected } from "#lib/testing/live.js";

import { pipeline } from "./results";
import { Results, STREAM_MS } from "./results.svelte";

const ID = "6f1c9a52-0d1e-4c43-9d55-3a1f0b6e7a10";

const response = (username: string): Schemas["SearchResponse"] => ({
  username,
  country: null,
  token: 1,
  hasFreeUploadSlot: true,
  uploadSpeed: 0,
  queueLength: 0,
  fileCount: 0,
  files: [],
  lockedFileCount: 0,
  lockedFiles: []
});

/** The hello of a stream that follows the search */
const following = {
  principal: { name: "me", role: "ReadOnly" as const },
  resumed: true,
  epoch: "first",
  serverTime: "2026-10-26T10:00:00Z",
  topics: ["search", `search:${ID}`],
  version: { current: "0.1.0", full: "0.1.0", isCanary: false, isDevelopment: true }
};

describe("a running search's results", () => {
  let results: Results | null = null;

  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    results?.close();
    results = null;
    live.stop();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  const shown = () => results!.results.map((r) => r.key);

  it("show what the search took before the stream followed it, each response once", async () => {
    let taken = [response("alice")];
    const net = connected((url) =>
      url.pathname.endsWith(`/searches/${ID}/responses`) ? taken : undefined
    );
    results = new Results(ID);
    await results.catchUp();
    expect(shown(), "taken before the page opened").toEqual(["0:alice"]);

    // bob comes before the stream follows the search and is never streamed; carol comes after
    // and is both streamed and read
    taken = [response("alice"), response("bob"), response("carol")];
    net.frame("search.response", { searchId: ID, response: response("carol") });
    net.frame("hello", following);
    await flushFaked();
    await vi.advanceTimersByTimeAsync(STREAM_MS);
    expect(shown()).toEqual(["0:alice", "0:bob", "0:carol"]);

    net.frame("search.response", { searchId: ID, response: response("bob") });
    net.frame("search.response", { searchId: ID, response: response("dave") });
    await flushFaked();
    await vi.advanceTimersByTimeAsync(STREAM_MS);
    expect(shown()).toEqual(["0:alice", "0:bob", "0:carol", "0:dave"]);
  });

  it("keep a later read's answer over an earlier one answered after it", async () => {
    const answers: ((body: unknown) => void)[] = [];
    connected((url) =>
      url.pathname.endsWith(`/searches/${ID}/responses`)
        ? new Promise((resolve) => answers.push(resolve))
        : undefined
    );
    results = new Results(ID);
    const first = results.catchUp();
    const second = results.catchUp();
    await flushFaked();
    answers[1]([response("alice"), response("bob")]);
    await second;
    answers[0]([response("alice")]);
    await first;
    expect(shown()).toEqual(["0:alice", "0:bob"]);
  });
});

describe("the results' order and filters", () => {
  const queued = (username: string, queueLength: number | null) => ({
    key: `user:${username}`,
    response: {
      ...response(username),
      queueLength,
      fileCount: 1,
      files: [sharedFile(`${username}\\a.flac`)]
    }
  });
  const shown = (options: { maxQueue?: number | null; sort?: "queueLength" | null }) =>
    pipeline([queued("alice", 3), queued("bob", null), queued("carol", 1)], {
      hidden: () => false,
      filters: parseFilters(""),
      hideLocked: false,
      hideNoFreeSlots: false,
      sort: null,
      ...options
    }).map((r) => r.response.username);

  it("sorts a queue the user doesn't say as the longest", () => {
    expect(shown({ sort: "queueLength" })).toEqual(["carol", "alice", "bob"]);
  });

  it("keeps a queue the user doesn't say under the longest queue allowed", () => {
    expect(shown({ maxQueue: 2 })).toEqual(["bob", "carol"]);
  });
});

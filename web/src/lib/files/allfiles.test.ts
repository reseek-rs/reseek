import { afterEach, describe, expect, it, vi } from "vitest";

import { toasts } from "#lib/feedback/toast.svelte.js";
import { sharedFile } from "#lib/testing/fixtures.js";
import { flush } from "#lib/testing/flush.js";
import { fakeNetwork } from "#lib/testing/network.js";

import { AllFiles } from "./allfiles.svelte";

describe("the all files dialog", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    toasts.splice(0);
  });

  it("shows the folder asked for last, whichever answer comes first", async () => {
    const answers = new Map<string, (reply: unknown) => void>();
    fakeNetwork(
      (_url, _method, body) =>
        new Promise((resolve) => answers.set((body as { directory: string }).directory, resolve))
    );
    const dialog = new AllFiles();
    const first = dialog.show("alice", "@@a\\A");
    await vi.waitFor(() => expect(answers.has("@@a\\A")).toBe(true));
    const second = dialog.show("alice", "@@a\\B");
    await vi.waitFor(() => expect(answers.has("@@a\\B")).toBe(true));

    answers.get("@@a\\B")!([{ name: "@@a\\B", files: [sharedFile("2.flac")] }]);
    await second;
    expect(dialog.shown?.folder).toBe("@@a\\B");
    expect(dialog.asking).toBeNull();

    // the overtaken ask fails late, and neither replaces the dialog nor toasts
    answers.get("@@a\\A")!({ status: 503, body: { error: "unreachable" } });
    await first;
    await flush();
    expect(dialog.shown?.folder).toBe("@@a\\B");
    expect(toasts).toEqual([]);
  });
});

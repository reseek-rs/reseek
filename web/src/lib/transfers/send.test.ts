import { describe, expect, it } from "vitest";

import { sentText } from "./send.svelte";

/** `text` as the line sets a name apart from the sentence around it */
const named = (text: string) => `\u2068${text}\u2069`;

const ONE = "@@a\\Music\\Album\\01 One.flac";
const TWO = "@@a\\Music\\Album\\02 Two.flac";

describe("what sending files came to", () => {
  it("says which files will be offered", () => {
    expect(sentText("bob", { queued: [ONE], alreadySending: [], failed: [] })).toBe(
      `${named("01 One.flac")} will be offered to ${named("bob")}`
    );
    expect(sentText("bob", { queued: [ONE, TWO], alreadySending: [], failed: [] })).toBe(
      `2 files will be offered to ${named("bob")}`
    );
  });

  it("says apart from those which files are already being sent", () => {
    expect(sentText("bob", { queued: [], alreadySending: [ONE], failed: [] })).toBe(
      `${named("01 One.flac")} is already being sent to ${named("bob")}`
    );
    expect(sentText("bob", { queued: [], alreadySending: [ONE, TWO], failed: [] })).toBe(
      `2 files are already being sent to ${named("bob")}`
    );
    expect(sentText("bob", { queued: [TWO], alreadySending: [ONE], failed: [] })).toBe(
      `${named("02 Two.flac")} will be offered to ${named("bob")}. ` +
        `${named("01 One.flac")} is already being sent`
    );
  });

  it("names why the first file not sent was refused", () => {
    const failed = [{ filename: TWO, message: "File not shared." }];
    expect(sentText("bob", { queued: [], alreadySending: [], failed })).toBe(
      `Nothing was sent to ${named("bob")}. 1 file not sent: File not shared.`
    );
    expect(sentText("bob", { queued: [], alreadySending: [ONE], failed })).toBe(
      `${named("01 One.flac")} is already being sent to ${named("bob")}. ` +
        "1 file not sent: File not shared."
    );
  });
});

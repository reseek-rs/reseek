import { flushSync, mount, unmount } from "svelte";
import { afterEach, describe, expect, it } from "vitest";

import ProfileView from "./ProfileView.svelte";

describe("a profile as a peer is sent it", () => {
  let component: ReturnType<typeof mount> | null = null;
  afterEach(() => {
    if (component) unmount(component);
    component = null;
    document.body.innerHTML = "";
  });

  const show = (description: string, uploadSlots: number | null = 3, queueLength = uploadSlots) => {
    component = mount(ProfileView, {
      target: document.body,
      props: {
        name: "alice",
        pictureUrl: null,
        description,
        uploadSlots,
        freeUploadSlot: false,
        queueLength
      }
    });
  };

  it("folds a description of a megabyte until the rest is asked for", () => {
    show("x".repeat(1_000_000));
    const text = document.querySelector("p")!;
    expect(text.textContent!.length).toBeLessThan(5000);
    const more = [...document.querySelectorAll("button")].find(
      (b) => b.textContent === "Show more"
    )!;
    more.click();
    flushSync();
    expect(text.textContent).toHaveLength(1_000_000);
  });

  it("folds a description of many short lines", () => {
    show("line\n".repeat(500));
    expect(document.querySelector("p")!.textContent!.split("\n").length).toBeLessThan(60);
  });

  it("writes its figures with the digits grouped, and a dash for one the peer does not say", () => {
    show("hi", 2_147_483_647, null);
    expect(document.querySelector("dl")!.textContent).toMatch(/Upload slots\s*2,147,483,647/);
    expect(document.querySelector("dl")!.textContent).toMatch(/Queue length\s*—/);
  });
});

import { flushSync, mount, unmount } from "svelte";
import { afterEach, expect, it } from "vitest";

import { box } from "#lib/testing/state.svelte.js";

import UserCard from "./UserCard.svelte";

afterEach(() => (document.body.innerHTML = ""));

it("shows the first three lines of a draft's description, and follows the draft", () => {
  const draft: { description: string; src: string | null; pictureUrl: string | null } = {
    description: "one\ntwo\nthree\nfour",
    src: null,
    pictureUrl: null
  };
  const props = box({ name: "alice", user: undefined, onblock: () => {}, draft }).value;
  const card = mount(UserCard, { target: document.body, props });
  flushSync();
  expect(document.querySelector("p")?.textContent).toBe("one\ntwo\nthree");

  // a picture picked in the editor makes a new draft with the same text
  props.draft = { ...props.draft, pictureUrl: "blob:picture" };
  flushSync();
  expect(document.querySelector("p")?.textContent).toBe("one\ntwo\nthree");

  props.draft = { ...props.draft, description: "five" };
  flushSync();
  expect(document.querySelector("p")?.textContent).toBe("five");
  unmount(card);
});

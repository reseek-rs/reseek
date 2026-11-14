import { afterEach, expect, it } from "vitest";

import { focusByKeys } from "./keys";

// jsdom works out :focus-visible from the last key or mouse event the window saw, which a test
// leaves behind for the next one; these two run in this order
afterEach(() => (document.body.innerHTML = ""));

const button = () => document.body.appendChild(document.createElement("button"));

it("shows focus the keys move", () => {
  const target = button();
  focusByKeys(target);
  expect(target.matches(":focus-visible")).toBe(true);
});

it("starts as a press would leave it, whatever the test before it pressed", () => {
  const target = button();
  target.focus();
  expect(target.matches(":focus-visible")).toBe(false);
});

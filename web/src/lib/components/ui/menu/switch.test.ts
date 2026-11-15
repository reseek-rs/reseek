import { afterEach, expect, it, vi } from "vitest";

import { ROW_MENU, switchOnPress } from "./switch";

const hitTest = document.elementFromPoint;
afterEach(() => {
  document.elementFromPoint = hitTest;
  document.body.innerHTML = "";
  document.body.style.pointerEvents = "";
  vi.restoreAllMocks();
});

const press = { clientX: 10, clientY: 20 } as PointerEvent;

it("looks past the open menu's hold on the page, and leaves the page held", async () => {
  const more = document.body.appendChild(document.createElement("button"));
  more.setAttribute(ROW_MENU, "");
  const opened = vi.fn();
  more.addEventListener("click", opened);
  document.body.style.pointerEvents = "none";
  // jsdom lays nothing out, so what is at the press is told; and while the test looks, the page
  // takes presses
  let held: string | null = null;
  document.elementFromPoint = () => {
    held = document.body.style.pointerEvents;
    return more.appendChild(document.createElement("svg"));
  };
  switchOnPress(press, null);
  expect(held).toBe("");
  expect(document.body.style.pointerEvents).toBe("none");
  await vi.waitFor(() => expect(opened).toHaveBeenCalledOnce());
});

import { tick } from "svelte";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ROW_MENU, switchOnPress } from "./switch";

const hitTest = document.elementFromPoint;
afterEach(() => {
  document.elementFromPoint = hitTest;
  document.body.innerHTML = "";
  document.body.style.pointerEvents = "";
  vi.restoreAllMocks();
});

/** a tap's click, which bits-ui hands on once the tap is released */
const tap = { type: "click", clientX: 10, clientY: 20 } as PointerEvent;

/** A row's menu button at (10, 20), and nothing else anywhere, with a listener on its clicks */
const rowMenu = () => {
  const more = document.body.appendChild(document.createElement("button"));
  more.setAttribute(ROW_MENU, "");
  const opened = vi.fn();
  more.addEventListener("click", opened);
  document.elementFromPoint = (x, y) => (x === 10 && y === 20 ? more : document.body);
  return opened;
};

const pointer = (type: string, clientX: number, clientY: number) =>
  document.body.dispatchEvent(new PointerEvent(type, { bubbles: true, clientX, clientY }));

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
  switchOnPress(tap, null);
  expect(held).toBe("");
  expect(document.body.style.pointerEvents).toBe("none");
  await vi.waitFor(() => expect(opened).toHaveBeenCalledOnce());
});

describe("a mouse press on another row's menu button", () => {
  const press = () => {
    pointer("pointerdown", 10, 20);
    return new PointerEvent("pointerdown", { clientX: 10, clientY: 20 });
  };

  it("opens its menu on a release over the button", async () => {
    const opened = rowMenu();
    switchOnPress(press(), null);
    await tick();
    expect(opened).not.toHaveBeenCalled();
    pointer("pointerup", 10, 20);
    await vi.waitFor(() => expect(opened).toHaveBeenCalledOnce());
  });

  it("opens its menu when released before the press is handed on", async () => {
    const opened = rowMenu();
    const pressed = press();
    pointer("pointerup", 10, 20);
    switchOnPress(pressed, null);
    await vi.waitFor(() => expect(opened).toHaveBeenCalledOnce());
  });

  it("opens nothing when dragged off and released elsewhere", async () => {
    const opened = rowMenu();
    switchOnPress(press(), null);
    pointer("pointerup", 300, 400);
    // a later release over the button is another press's
    pointer("pointerup", 10, 20);
    await tick();
    expect(opened).not.toHaveBeenCalled();
  });
});

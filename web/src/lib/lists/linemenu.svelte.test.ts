import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { LONG_PRESS_MS } from "#lib/users/hover.svelte.js";

import { LineMenu } from "./linemenu.svelte";

type Line = { id: string };

const LINES: Line[] = [{ id: "a" }, { id: "b" }];

/** A menu on a container holding lines `a` and `b`, with its pointer listeners added */
const lines = async () => {
  const container = document.body.appendChild(document.createElement("div"));
  const elements = LINES.map((line) => {
    const element = container.appendChild(document.createElement("div"));
    element.dataset.line = line.id;
    return element;
  });
  let menu!: LineMenu<Line>;
  const stop = $effect.root(() => {
    menu = new LineMenu<Line>({
      lines: () => LINES,
      key: (line) => line.id,
      enabled: () => true,
      anchor: () => null
    });
  });
  const detach = menu.attach(container);
  // svelte's on() adds pointer listeners a microtask later
  await Promise.resolve();
  return {
    menu,
    elements,
    stop: () => {
      detach();
      stop();
    }
  };
};

const touch = (element: Element, type: string, pointerId: number) =>
  element.dispatchEvent(new PointerEvent(type, { bubbles: true, pointerId, pointerType: "touch" }));

describe("a long press", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    document.body.innerHTML = "";
    vi.useRealTimers();
  });

  it("opens the menu of the line held", async () => {
    const { menu, elements, stop } = await lines();
    touch(elements[0], "pointerdown", 1);
    vi.advanceTimersByTime(LONG_PRESS_MS);
    expect(menu.line?.id).toBe("a");
    stop();
  });

  it("opens nothing once a second finger came down and both lifted", async () => {
    const { menu, elements, stop } = await lines();
    touch(elements[0], "pointerdown", 1);
    touch(elements[1], "pointerdown", 2);
    touch(elements[0], "pointercancel", 1);
    touch(elements[1], "pointercancel", 2);
    vi.advanceTimersByTime(LONG_PRESS_MS * 2);
    expect(menu.line).toBeNull();
    stop();
  });
});

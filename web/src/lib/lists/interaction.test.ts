import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { freezesWhileUsed, holdListOf, SELECTION_IDLE_MS } from "./interaction.svelte";
import { Keyed } from "./keyed.svelte";

type Row = { id: string };

/** A list of rows `a` and `b` drawn in an element of its own, which a selection holds */
const list = () => {
  const rows = new Keyed<Row>((row) => row.id);
  rows.reconcile([{ id: "a" }, { id: "b" }]);
  const element = document.body.appendChild(document.createElement("div"));
  element.textContent = "a b";
  const detach = freezesWhileUsed(rows.freeze)(element);
  return { rows, element, detach };
};

/** Selects the text of `element`, as a drag or a double click does */
const select = (element: Element) => {
  document.getSelection()!.selectAllChildren(element);
  document.dispatchEvent(new Event("selectionchange"));
};

const keys = (rows: Keyed<Row>) => rows.rows.map((row) => row.id);

describe("a text selection", () => {
  const detached: (() => void)[] = [];
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    for (const detach of detached.splice(0)) detach();
    document.getSelection()!.removeAllRanges();
    document.body.innerHTML = "";
    vi.useRealTimers();
  });

  it("holds back removals in its own list only", () => {
    const chat = list();
    const other = list();
    detached.push(chat.detach, other.detach);
    select(chat.element);
    chat.rows.remove("a");
    other.rows.remove("a");
    expect(keys(chat.rows)).toEqual(["a", "b"]);
    expect(chat.rows.removed.has("a")).toBe(true);
    expect(keys(other.rows)).toEqual(["b"]);

    document.getSelection()!.removeAllRanges();
    document.dispatchEvent(new Event("selectionchange"));
    expect(keys(chat.rows)).toEqual(["b"]);
  });

  it("lets its list catch up after a while with no pointer or key activity", () => {
    const chat = list();
    detached.push(chat.detach);
    select(chat.element);
    chat.rows.remove("a");

    // the user moving the pointer or typing keeps it held
    vi.advanceTimersByTime(SELECTION_IDLE_MS - 1_000);
    window.dispatchEvent(new PointerEvent("pointermove"));
    vi.advanceTimersByTime(SELECTION_IDLE_MS - 1_000);
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Shift" }));
    vi.advanceTimersByTime(SELECTION_IDLE_MS - 1_000);
    expect(keys(chat.rows)).toEqual(["a", "b"]);

    // then nothing for the whole while: the removal waits no longer, though the text is selected
    vi.advanceTimersByTime(1_000);
    expect(keys(chat.rows)).toEqual(["b"]);
    expect(chat.rows.freeze.frozen).toBe(false);

    // the user back at the page, with the text still selected, holds the list again
    window.dispatchEvent(new PointerEvent("pointermove"));
    expect(chat.rows.freeze.frozen).toBe(true);
    chat.rows.remove("b");
    expect(keys(chat.rows)).toEqual(["b"]);
  });

  it("holds removals in its list only while the pointer is pressed in it", () => {
    const chat = list();
    const other = list();
    detached.push(chat.detach, other.detach);
    chat.element.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    chat.rows.remove("a");
    other.rows.remove("a");
    expect(keys(chat.rows)).toEqual(["a", "b"]);
    expect(keys(other.rows)).toEqual(["b"]);
    window.dispatchEvent(new PointerEvent("pointerup"));
    expect(keys(chat.rows)).toEqual(["b"]);
  });

  it("stops watching for activity once the selection leaves its list", () => {
    const chat = list();
    detached.push(chat.detach);
    select(chat.element);
    document.getSelection()!.removeAllRanges();
    document.dispatchEvent(new Event("selectionchange"));
    window.dispatchEvent(new PointerEvent("pointermove"));
    expect(chat.rows.freeze.frozen).toBe(false);
  });
});

describe("a card or a menu open in a list", () => {
  afterEach(() => (document.body.innerHTML = ""));

  it("holds the nearest list it is in, and no other", () => {
    const outer = list();
    const inner = list();
    outer.element.append(inner.element);
    const line = inner.element.appendChild(document.createElement("span"));
    const release = holdListOf(line)!;
    expect(inner.rows.freeze.frozen).toBe(true);
    expect(outer.rows.freeze.frozen).toBe(false);
    inner.rows.remove("a");
    expect(keys(inner.rows)).toEqual(["a", "b"]);
    release();
    expect(keys(inner.rows)).toEqual(["b"]);
    expect(holdListOf(document.body.appendChild(document.createElement("p")))).toBeUndefined();
    outer.detach();
    inner.detach();
    // a list no longer drawn is held by nothing
    expect(holdListOf(line)).toBeUndefined();
  });
});

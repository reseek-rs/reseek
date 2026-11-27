import { tick } from "svelte";

/** Marks a row's menu button, which a press while another menu is open goes on to */
export const ROW_MENU = "data-row-menu";

/**
 * The row's menu button a press outside an open menu is on, as the user sees the page: the
 * topmost element at the press once the open menu's hold on the page is lifted. An element over
 * the button, such as a sticky header, is what is pressed, not the button
 */
const rowMenuAt = (e: PointerEvent): HTMLElement | null => {
  // the open menu holds the page with `pointer-events: none` on the body, which hit testing
  // follows; lifted for the one test, with nothing drawn in between
  const body = document.body.style;
  const held = body.pointerEvents;
  body.pointerEvents = "";
  try {
    const at = document.elementFromPoint(e.clientX, e.clientY);
    return at?.closest<HTMLElement>(`[${ROW_MENU}]`) ?? null;
  } finally {
    body.pointerEvents = held;
  }
};

/**
 * How the last mouse or pen press ended, null while it is held. bits-ui hands a mouse or pen
 * press on 10 ms after the `pointerdown`, so a quick click has ended by then
 */
let ended: PointerEvent | null = null;
if (typeof window !== "undefined") {
  const end = (e: PointerEvent) => (ended = e);
  window.addEventListener("pointerdown", () => (ended = null), true);
  window.addEventListener("pointerup", end, true);
  window.addEventListener("pointercancel", end, true);
}

// once this menu has closed: a menu of the same list is this one, which the close would undo
const open = (more: HTMLElement) => void tick().then(() => more.click());

/** Opens `more`'s menu when the press ended in a release over it, as a button's click would */
const openOnRelease = (end: PointerEvent, more: HTMLElement) => {
  if (end.type === "pointerup" && rowMenuAt(end) === more) open(more);
};

/**
 * A press outside an open menu, which closes it: on another row's menu button it opens that
 * row's menu as well, once released over it; anything else only closes. `anchor` is the button
 * this menu hangs from, a press on which only closes it
 */
export const switchOnPress = (e: PointerEvent, anchor: HTMLElement | null | undefined) => {
  const more = rowMenuAt(e);
  if (!more || more === anchor) return;
  // a tap is handed on at its click, after the release
  if (e.type !== "pointerdown") return open(more);
  if (ended) return openOnRelease(ended, more);
  const waited = (end: PointerEvent) => {
    window.removeEventListener("pointerup", waited, true);
    window.removeEventListener("pointercancel", waited, true);
    openOnRelease(end, more);
  };
  window.addEventListener("pointerup", waited, true);
  window.addEventListener("pointercancel", waited, true);
};

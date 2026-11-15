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
 * A press outside an open menu, which closes it: on another row's menu button it opens that
 * row's menu as well; anything else only closes. `anchor` is the button this menu hangs from, a
 * press on which only closes it
 */
export const switchOnPress = (e: PointerEvent, anchor: HTMLElement | null | undefined) => {
  const more = rowMenuAt(e);
  if (!more || more === anchor) return;
  // once this menu has closed: a menu of the same list is this one, which the close would undo
  void tick().then(() => more.click());
};

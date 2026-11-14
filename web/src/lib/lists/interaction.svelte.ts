import { untrack } from "svelte";

/**
 * What holds a list still while the user is in the middle of something they must not move under,
 * and the work waiting for it to end. While it holds, the list takes appends only and keeps
 * removed rows until it ends
 */
export class Freeze {
  #holds = $state(0);
  #thawed: (() => void)[] = [];

  get frozen(): boolean {
    return this.#holds > 0;
  }

  /** Freezes until the returned function is called. Callable from an effect: the count it
   * changes is not one the effect follows */
  hold(): () => void {
    untrack(() => (this.#holds += 1));
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const left = untrack(() => (this.#holds -= 1));
      if (left === 0) for (const run of this.#thawed.splice(0)) run();
    };
  }

  /** Runs `run` once nothing holds, now when nothing does */
  whenThawed(run: () => void) {
    if (this.#holds > 0) this.#thawed.push(run);
    else run();
  }
}

/** The freeze of each list's element, which a name's card or a line's menu inside it holds */
const lists = new WeakMap<Element, Freeze>();

/**
 * For `{@attach}` on a list's element: a name's card or a line's menu open inside it holds
 * `freeze`. `freezesWhileUsed` does this as well
 */
export const heldBy = (freeze: Freeze) => (node: HTMLElement) => {
  lists.set(node, freeze);
  return () => {
    if (lists.get(node) === freeze) lists.delete(node);
  };
};

/**
 * Holds the list `node` is in, the nearest one up from it, until the returned function is called;
 * undefined outside every list. For a card or a menu that opens from inside a list
 */
export const holdListOf = (node: Element | null): (() => void) | undefined => {
  for (let at = node; at; at = at.parentElement) {
    const freeze = lists.get(at);
    if (freeze) return freeze.hold();
  }
  return undefined;
};

/**
 * Keeps a list's order while `freeze` holds it: the lines keep the places they had and new ones
 * go below; once nothing holds it, the order given is taken. Make one per list and give it each
 * fresh order from a `$derived`
 */
export const heldOrder = <T>(key: (item: T) => string, freeze: Freeze) => {
  let shown: T[] = [];
  return (fresh: T[]): T[] => {
    if (!freeze.frozen || !shown.length) return (shown = fresh);
    const place = new Map(shown.map((item, i) => [key(item), i]));
    return (shown = fresh.toSorted(
      (a, b) => (place.get(key(a)) ?? Infinity) - (place.get(key(b)) ?? Infinity)
    ));
  };
};

/** How long a text selection holds its list with no pointer or key activity */
export const SELECTION_IDLE_MS = 10_000;

/** What tells that the user is at the page while a selection holds a list */
const ACTIVITY = ["pointermove", "pointerdown", "keydown", "wheel"] as const;
const QUIET = { capture: true, passive: true } as const;

/**
 * Holds `freeze` while the pointer is pressed in the element or a selection that isn't collapsed
 * lies in it, and while a name's card or a line's menu in it is open (`heldBy`). A selection
 * holds only while the user is at the page: after `SELECTION_IDLE_MS` with no pointer or key
 * activity the list catches up, and their next move holds it again. For `{@attach}` on a list's
 * element
 */
export const freezesWhileUsed = (freeze: Freeze) => (node: HTMLElement) => {
  const listed = heldBy(freeze)(node);
  let pressed: (() => void) | null = null;
  let selecting: (() => void) | null = null;
  let idle: ReturnType<typeof setTimeout> | undefined;
  let lastActive = 0;
  let watching = false;

  const release = () => {
    pressed?.();
    pressed = null;
  };
  const down = () => {
    release();
    pressed = freeze.hold();
  };

  const letGo = () => {
    clearTimeout(idle);
    selecting?.();
    selecting = null;
  };
  const checkIdle = () => {
    const left = lastActive + SELECTION_IDLE_MS - Date.now();
    if (left > 0) idle = setTimeout(checkIdle, left);
    else letGo();
  };
  const active = () => {
    lastActive = Date.now();
    if (selecting) return;
    selecting = freeze.hold();
    idle = setTimeout(checkIdle, SELECTION_IDLE_MS);
  };
  const watch = (on: boolean) => {
    if (on === watching) return;
    watching = on;
    for (const type of ACTIVITY) {
      if (on) window.addEventListener(type, active, QUIET);
      else window.removeEventListener(type, active, QUIET);
    }
  };

  const selection = () => {
    const sel = document.getSelection();
    const inside =
      !!sel &&
      !sel.isCollapsed &&
      sel.rangeCount > 0 &&
      node.contains(sel.getRangeAt(0).commonAncestorContainer);
    watch(inside);
    if (inside) active();
    else letGo();
  };
  node.addEventListener("pointerdown", down);
  window.addEventListener("pointerup", release);
  window.addEventListener("pointercancel", release);
  document.addEventListener("selectionchange", selection);
  return () => {
    listed();
    node.removeEventListener("pointerdown", down);
    window.removeEventListener("pointerup", release);
    window.removeEventListener("pointercancel", release);
    document.removeEventListener("selectionchange", selection);
    watch(false);
    release();
    letGo();
  };
};

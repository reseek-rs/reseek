import { flushSync, mount, unmount } from "svelte";
import { afterEach, describe, expect, it, vi } from "vitest";

import Tip from "#lib/testing/Tip.svelte";

import TooltipHost from "./TooltipHost.svelte";

describe("a tooltip", () => {
  let mounted: ReturnType<typeof mount>[] = [];
  afterEach(() => {
    for (const component of mounted) unmount(component);
    mounted = [];
    document.body.innerHTML = "";
  });

  /** The tab index of a static element with a tooltip, dormant or with its trigger */
  const stop = (props: { text?: string; dense?: boolean; clipped?: boolean }, dormant: boolean) => {
    const target = document.createElement("div");
    document.body.append(target);
    mounted.push(mount(Tip, { target, props: { ...props, dormant } }));
    flushSync();
    return target.querySelector("[role=note]")!.getAttribute("tabindex");
  };

  it("makes a static element the same tab stop dormant as with its trigger", () => {
    for (const props of [{ text: "why" }, { text: "why", dense: true }, {}]) {
      expect(stop(props, true)).toBe(stop(props, false));
    }
    expect(stop({ text: "why" }, true)).toBe("0");
  });

  it("leaves a clipped element out of the tab order, dormant or with its trigger", () => {
    expect(stop({ text: "tip", clipped: true }, true)).toBeNull();
    expect(stop({ text: "tip", clipped: true }, false)).toBeNull();
  });

  it("shows a clipped element's text only while the element cuts it off", async () => {
    vi.useFakeTimers();
    try {
      mounted.push(mount(TooltipHost, { target: document.body }));
      const target = document.body.appendChild(document.createElement("div"));
      mounted.push(mount(Tip, { target, props: { text: "the whole name", clipped: true } }));
      flushSync();
      const note = target.querySelector<HTMLElement>("[role=note]")!;
      const shown = async (width: number) => {
        Object.defineProperty(note, "scrollWidth", { configurable: true, value: width });
        note.dispatchEvent(new PointerEvent("pointerenter", { pointerType: "mouse" }));
        note.dispatchEvent(
          new PointerEvent("pointermove", { pointerType: "mouse", bubbles: true })
        );
        await vi.advanceTimersByTimeAsync(1_000);
        flushSync();
        const tip = document.querySelector("[data-tooltip-content]");
        note.dispatchEvent(new PointerEvent("pointerleave", { pointerType: "mouse" }));
        await vi.advanceTimersByTimeAsync(1_000);
        flushSync();
        return tip?.textContent ?? null;
      };
      // jsdom lays nothing out, so the element is as wide as its text unless told otherwise
      expect(await shown(0)).toBeNull();
      expect(await shown(500)).toBe("the whole name");
    } finally {
      vi.useRealTimers();
    }
  });
});

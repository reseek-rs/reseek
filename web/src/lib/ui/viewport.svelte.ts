import { createSubscriber, MediaQuery } from "svelte/reactivity";

/** Below Tailwind's `md`: a phone, where panels become sheets over the screen */
const narrow = new MediaQuery("max-width: 767px");
const sm = new MediaQuery("min-width: 640px");
const lg = new MediaQuery("min-width: 1024px");
const xxl = new MediaQuery("min-width: 1536px");
const finePointer = new MediaQuery("any-pointer: fine");

/** What the window's width allows, decided here once for every screen */
export const viewport = {
  get phone() {
    return narrow.current;
  },
  /** at least Tailwind's `sm`, for a label that its classes hide from there on */
  get sm() {
    return sm.current;
  },
  /** at least Tailwind's `lg` */
  get lg() {
    return lg.current;
  },
  /** at least Tailwind's `2xl` */
  get xxl() {
    return xxl.current;
  },
  /** a mouse or trackpad is at hand, as it is not on a touch-only phone or tablet */
  get finePointer() {
    return finePointer.current;
  }
};

/** the root font size in pixels while something follows it, null while it is to be read again */
let rootPx: number | null = null;
const readRoot = () => parseFloat(getComputedStyle(document.documentElement).fontSize);

// a probe 1rem tall changes size with the root font size, which nothing else announces. While
// something follows it the size is read once and kept until the probe changes size
const rootSize = createSubscriber((update) => {
  const probe = document.createElement("div");
  probe.setAttribute("aria-hidden", "true");
  probe.style.cssText =
    "position:absolute;top:0;left:0;width:0;height:1rem;visibility:hidden;pointer-events:none";
  document.body.append(probe);
  rootPx = readRoot();
  const observer = new ResizeObserver(() => {
    rootPx = readRoot();
    update();
  });
  observer.observe(probe);
  return () => {
    observer.disconnect();
    probe.remove();
    rootPx = null;
  };
});

/**
 * The pixels in a rem, followed as the text size changes: a virtual list places its lines in
 * pixels and sizes them in rem with this, so a larger text size gets taller lines
 */
export const rem = {
  get px(): number {
    rootSize();
    return rootPx ?? readRoot();
  }
};

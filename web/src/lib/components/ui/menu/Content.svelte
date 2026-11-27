<script lang="ts">
  import { DropdownMenu } from "bits-ui";

  import { cn } from "#lib/ui/cn.js";
  import { safeArea } from "#lib/ui/safearea.svelte.js";

  import { anchorOf } from "./anchor";
  import { switchOnPress } from "./switch";

  let {
    sideOffset = 4,
    class: className,
    onCloseAutoFocus,
    customAnchor,
    ...rest
  }: Omit<DropdownMenu.ContentProps, "onInteractOutside"> = $props();

  // in a LazyRoot there is no trigger of the menu's own: it hangs from the button that opened
  // it, which gets focus back on close
  const anchor = anchorOf();
  /** what the menu hangs from: the caller's anchor, else a LazyRoot's button */
  const hangsFrom = () => customAnchor ?? anchor?.();
</script>

<DropdownMenu.Portal>
  <DropdownMenu.Content
    {sideOffset}
    collisionPadding={safeArea.padding(0)}
    class={cn(
      "z-50 max-h-(--bits-dropdown-menu-content-available-height) max-w-(--bits-dropdown-menu-content-available-width) min-w-32 origin-(--bits-dropdown-menu-content-transform-origin) overflow-y-auto rounded-control border bg-overlay p-1 text-fg shadow-raised outline-none data-[state=closed]:animate-popover-out data-[state=open]:animate-popover-in",
      className
    )}
    customAnchor={hangsFrom()}
    onInteractOutside={(e) => {
      // a press on the button the menu hangs from only closes it
      const from = hangsFrom();
      switchOnPress(e, from instanceof HTMLElement ? from : null);
    }}
    onCloseAutoFocus={(e) => {
      if (onCloseAutoFocus) onCloseAutoFocus(e);
      else if (anchor) {
        e.preventDefault();
        anchor()?.focus({ preventScroll: true });
      }
    }}
    {...rest} />
</DropdownMenu.Portal>

<script lang="ts">
  import { DropdownMenu } from "bits-ui";

  import { cn } from "#lib/ui/cn.js";
  import { safeArea } from "#lib/ui/safearea.svelte.js";

  import { anchorOf } from "./anchor";

  let { sideOffset = 4, class: className, ...rest }: DropdownMenu.ContentProps = $props();

  // in a LazyRoot there is no trigger of the menu's own: it hangs from the button that opened
  // it, which gets focus back on close. A row's menu leaves the page under it taking presses, so
  // a tap on another row's button opens that row's menu rather than only closing this one
  const anchor = anchorOf();
</script>

<DropdownMenu.Portal>
  <DropdownMenu.Content
    {sideOffset}
    collisionPadding={safeArea.padding(0)}
    class={cn(
      "z-50 max-h-(--bits-dropdown-menu-content-available-height) max-w-(--bits-dropdown-menu-content-available-width) min-w-32 origin-(--bits-dropdown-menu-content-transform-origin) overflow-y-auto rounded-control border bg-overlay p-1 text-fg shadow-raised outline-none data-[state=closed]:animate-popover-out data-[state=open]:animate-popover-in",
      className
    )}
    customAnchor={anchor?.()}
    preventScroll={anchor ? false : undefined}
    onCloseAutoFocus={(e) => {
      if (!anchor) return;
      e.preventDefault();
      anchor()?.focus({ preventScroll: true });
    }}
    {...rest} />
</DropdownMenu.Portal>

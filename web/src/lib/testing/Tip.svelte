<script lang="ts">
  import Tooltip from "#lib/components/ui/Tooltip.svelte";

  // a static element with a tooltip, for the Tooltip tests
  let {
    text,
    dense = false,
    dormant = false,
    clipped = false
  }: { text?: string; dense?: boolean; dormant?: boolean; clipped?: boolean } = $props();
</script>

<Tooltip {text} {dense} {dormant} {clipped}>
  {#snippet child({ props })}<span {...props} role="note">tip</span>{/snippet}
</Tooltip>

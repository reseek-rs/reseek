<script lang="ts">
  import ChevronRightIcon from "@lucide/svelte/icons/chevron-right";
  import ExternalLinkIcon from "@lucide/svelte/icons/external-link";
  import ScaleIcon from "@lucide/svelte/icons/scale";
  import { SvelteSet } from "svelte/reactivity";

  import InfoList from "#lib/components/system/InfoList.svelte";
  import Badge from "#lib/components/ui/Badge.svelte";
  import Card from "#lib/components/ui/Card.svelte";
  import EmptyState from "#lib/components/ui/EmptyState.svelte";
  import Input from "#lib/components/ui/Input.svelte";
  import { plural } from "#lib/format.js";
  import type { NoticePackage, Notices } from "#lib/licenses/notices.js";
  import thirdParty from "#lib/licenses/third-party.json";
  import { useCrumbs } from "#lib/shell/crumbs.svelte.js";
  import { LICENSE, REPOSITORY } from "#lib/shell/project.js";

  // reseek's own license and source, then the code it ships from others, by license
  const notices: Notices = thirdParty;

  useCrumbs(() => [{ label: "Licenses" }]);

  const total = new Set(
    notices.licenses.flatMap((g) => g.packages.map((p) => `${p.registry}/${p.name}@${p.version}`))
  ).size;

  let typed = $state("");
  // a name or a license; a license's group shows whole
  const groups = $derived.by(() => {
    const q = typed.trim().toLowerCase();
    if (!q) return notices.licenses;
    return notices.licenses
      .map((g) =>
        g.id.toLowerCase().includes(q)
          ? g
          : { ...g, packages: g.packages.filter((p) => p.name.toLowerCase().includes(q)) }
      )
      .filter((g) => g.packages.length);
  });

  const keyOf = (id: string, p: NoticePackage) => `${id}/${p.registry}/${p.name}@${p.version}`;

  // a text is put on the page only while its package is open
  const opened = new SvelteSet<string>();

  const pageOf = (p: NoticePackage) =>
    p.registry === "npm"
      ? { href: `https://www.npmjs.com/package/${p.name}/v/${p.version}`, label: "On npm" }
      : { href: `https://crates.io/crates/${p.name}/${p.version}`, label: "On crates.io" };
</script>

<Card title="reseek">
  <InfoList>
    <dt>License</dt>
    <dd>
      <a href={LICENSE} target="_blank" rel="noreferrer" class="text-accent hover:underline"
        >AGPL-3.0-only</a>
    </dd>
    <dt>Source</dt>
    <dd>
      <a href={REPOSITORY} target="_blank" rel="noreferrer" class="text-accent hover:underline"
        >{REPOSITORY}</a>
    </dd>
  </InfoList>
</Card>

<section class="flex flex-col gap-section" aria-label="Third-party licenses">
  <div class="flex flex-wrap items-end justify-between gap-inline">
    <div class="flex flex-col gap-0.5">
      <h3 class="text-base leading-tight font-semibold tracking-tight">Third-party code</h3>
      <p class="text-sm text-fg-muted">
        {plural(total, "package", "packages")} reseek is built with
      </p>
    </div>
    <div class="w-full sm:w-64">
      <Input
        bind:value={typed}
        type="search"
        aria-label="Filter packages"
        placeholder="Filter by package or license" />
    </div>
  </div>

  {#each groups as group (group.id)}
    <Card title={group.id} description={plural(group.packages.length, "package", "packages")}>
      <ul data-edge class="-mx-5 divide-y border-y">
        {#each group.packages as p (keyOf(group.id, p))}
          {@const key = keyOf(group.id, p)}
          {@const page = pageOf(p)}
          <li>
            <details
              class="group"
              open={opened.has(key)}
              ontoggle={(e) => (e.currentTarget.open ? opened.add(key) : opened.delete(key))}>
              <summary
                class="flex cursor-pointer list-none items-center gap-1.5 px-5 py-2 text-sm hover:bg-hover [&::-webkit-details-marker]:hidden">
                <ChevronRightIcon
                  class="size-4 shrink-0 text-fg-muted transition-transform duration-(--motion-fast) group-open:rotate-90" />
                <span class="min-w-0 font-medium wrap-anywhere">{p.name}</span>
                <span class="font-mono text-xs text-fg-muted">{p.version}</span>
                {#if p.registry === "npm"}<Badge>npm</Badge>{/if}
              </summary>
              {#if opened.has(key)}
                <div class="flex flex-col gap-3 px-5 pb-3">
                  <a
                    href={page.href}
                    target="_blank"
                    rel="noreferrer"
                    class="flex w-fit items-center gap-1 text-xs text-accent hover:underline"
                    ><ExternalLinkIcon class="size-3" />{page.label}</a>
                  {#each p.texts as text (text)}
                    <pre
                      class="max-h-[50svh] overflow-auto rounded-control bg-surface-muted p-3 font-mono text-xs wrap-anywhere whitespace-pre-wrap">{notices
                        .texts[text]}</pre>
                  {/each}
                </div>
              {/if}
            </details>
          </li>
        {/each}
      </ul>
    </Card>
  {:else}
    <Card>
      <EmptyState icon={ScaleIcon} title="No package or license matches" />
    </Card>
  {/each}
</section>

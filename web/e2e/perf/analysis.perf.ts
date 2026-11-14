import { expect, type Page, test } from "@playwright/test";

import { openSlot, rect, settled } from "../support/analysis";
import { fakeAnalysis, fakeDownloads } from "../support/fakes";
import { visit } from "../support/login";
import { workPerFrame } from "../support/perf";

/** A five-minute download open in slot A, zoomed in four times, its detail in */
const zoomedSlot = async (page: Page) => {
  await fakeDownloads(page);
  await fakeAnalysis(page, { duration: 300 });
  await visit(page, "/downloads");
  const slot = await openSlot(page);
  const detail = page.waitForResponse((r) => r.url().includes("/analysis/spectrogram?tile="));
  for (let i = 0; i < 4; i++) await slot.getByRole("button", { name: "Zoom in" }).click();
  // the detail for the first stretch arrives before the clock starts
  await detail;
  return slot;
};

test("pans a zoomed five-minute file at sixty frames a second", async ({ page }) => {
  const plot = (await zoomedSlot(page)).getByRole("slider", { name: "Cursor" });
  const box = await settled(plot);
  const spec = await rect(plot, "data-plot");
  const y = box.y + spec.height / 2;
  const task = await workPerFrame(page, "panning", async () => {
    await page.mouse.move(box.x + spec.x + spec.width * 0.9, y);
    await page.mouse.down();
    for (let i = 0; i < 120; i++) {
      await page.mouse.move(box.x + spec.x + spec.width * (0.9 - (0.8 * i) / 120), y);
    }
    await page.mouse.up();
  });
  expect(task).toBeLessThan(1000 / 60);
});

test("moves between scales at sixty frames a second on a five-minute file", async ({ page }) => {
  const slot = await zoomedSlot(page);
  await settled(slot.getByRole("slider", { name: "Cursor" }));
  const scales = slot.getByRole("radiogroup", { name: "Frequency scale" });
  const task = await workPerFrame(page, "scale moves", async (counted) => {
    // a new scale as soon as the last is taken, well before its move lands, so every frame is a
    // moving one
    for (let i = 0; !counted(); i++) {
      const scale = scales.getByRole("radio", { name: ["Log", "Mel", "Linear"][i % 3] });
      await scale.click();
      await expect(scale).toHaveAttribute("aria-checked", "true");
    }
  });
  expect(task).toBeLessThan(1000 / 60);
});

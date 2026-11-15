import { expect, type Locator, type Page, test } from "@playwright/test";

import { manyDownloads } from "./support/fakes";
import { visit } from "./support/login";

/**
 * 300 finished downloads, flat, so every row is a file's, and their More buttons; `low` is the
 * first row past `below` pixels down, whose menu opens upward and leaves the first row clear
 */
const rows = async (page: Page, below: number) => {
  await manyDownloads(page, 300);
  await page.addInitScript(() =>
    localStorage.setItem("transfers.view.download", JSON.stringify({ groupBy: "flat" }))
  );
  await visit(page, "/downloads");
  const more = page.getByRole("button", { name: /^More for / });
  await expect(more.first()).toBeVisible();
  const ys = await more.evaluateAll((all) => all.map((b) => b.getBoundingClientRect().y));
  return { more, low: more.nth(ys.findIndex((y) => y > below)) };
};

const open = (page: Page) => page.locator("[role=menu][data-state=open]");

/** The row `more` is in */
const lineOf = (more: Locator) => more.locator("xpath=ancestor::*[@data-line][1]");

/** The size under a row's name on a phone, which is no control */
const sizeOf = (more: Locator) => lineOf(more).locator(".whitespace-nowrap.tabular-nums").first();

/** Waits for the open menu to have flown in, and checks `box` is clear above it */
const clearOfMenu = async (page: Page, box: { y: number; height: number }) => {
  await expect(open(page)).toHaveCount(1);
  await open(page).evaluate((e) => Promise.all(e.getAnimations().map((a) => a.finished)));
  expect(box.y + box.height, "clear of the menu").toBeLessThan((await open(page).boundingBox())!.y);
};

/**
 * Checks the one open menu is the one `more` opened, hanging under it, with focus: the arrows
 * walk its items, and Escape gives focus back to `more`
 */
const opened = async (page: Page, more: Locator) => {
  const button = (await more.boundingBox())!;
  await expect(open(page)).toHaveCount(1);
  const box = (await open(page).boundingBox())!;
  expect(box.y).toBeGreaterThanOrEqual(button.y + button.height);
  expect(box.y).toBeLessThan(button.y + button.height + 20);
  await expect(open(page)).toBeFocused();
  await page.keyboard.press("ArrowDown");
  await expect(open(page).locator("[data-highlighted]")).toHaveCount(1);
  await page.keyboard.press("Escape");
  await expect(open(page)).toHaveCount(0);
  await expect(more).toBeFocused();
};

/**
 * Scrolls the list a little, so the row under the column header is partly hidden by it, and
 * gives back that row's More button, whose middle the header covers
 */
const underHeader = async (page: Page, more: Locator) => {
  const grid = page.getByRole("treegrid", { name: "Downloads" });
  let covered = -1;
  for (let top = 4; covered === -1 && top < 80; top += 4) {
    await grid.evaluate((e, top) => (e.scrollTop = top), top);
    covered = await more.evaluateAll((all) =>
      all.findIndex((b) => {
        const r = b.getBoundingClientRect();
        const at = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2);
        return r.height > 0 && !!at && !b.contains(at) && !!at.closest("[role=treegrid]");
      })
    );
  }
  expect(covered, "a More the header covers").toBeGreaterThanOrEqual(0);
  return more.nth(covered);
};

test.describe("with a mouse", () => {
  // tall enough that a low row's menu opens upward and leaves the first row clear
  test.use({ viewport: { width: 1280, height: 1200 } });

  test("a click on a row's More opens its menu with focus while another's is open", async ({
    page
  }) => {
    const { more, low } = await rows(page, 1000);
    await low.click();
    await clearOfMenu(page, (await more.first().boundingBox())!);
    // press and release at once, inside the closing menu's own wait
    await more.first().click({ force: true });
    await opened(page, more.first());
  });

  test("a click on a row's More opens its menu with focus while a line's is open", async ({
    page
  }) => {
    const { more, low } = await rows(page, 1000);
    await lineOf(low).locator("[role=gridcell]").nth(2).click({ button: "right" });
    await clearOfMenu(page, (await more.first().boundingBox())!);
    await more.first().click({ force: true });
    await opened(page, more.first());
  });

  test("a press on the header over a hidden row's More only closes the menu", async ({ page }) => {
    const { more, low } = await rows(page, 1000);
    const hidden = await underHeader(page, more);
    await low.click();
    await expect(open(page)).toHaveCount(1);
    const box = (await hidden.boundingBox())!;
    await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
    await expect(open(page)).toHaveCount(0);
    await expect(page.getByRole("menu")).toHaveCount(0);
  });
});

test.describe("on a touch screen", () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });

  const tap = async (page: Page, target: Locator) => {
    const box = (await target.boundingBox())!;
    await page.touchscreen.tap(box.x + box.width / 2, box.y + box.height / 2);
  };

  test("a tap on a row's More opens its menu with focus while another's is open", async ({
    page
  }) => {
    const { more, low } = await rows(page, 780);
    await low.tap();
    await clearOfMenu(page, (await more.first().boundingBox())!);
    await tap(page, more.first());
    await opened(page, more.first());
  });

  test("a tap on a row's More opens its menu with focus while a long press's is open", async ({
    page
  }) => {
    // a row whose size, held, is still above the bar at the foot of the screen
    const { more, low } = await rows(page, 700);
    const size = (await sizeOf(low).boundingBox())!;
    const cdp = await page.context().newCDPSession(page);
    const at = [{ x: size.x + size.width / 2, y: size.y + size.height / 2 }];
    await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: at });
    await clearOfMenu(page, (await more.first().boundingBox())!);
    await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
    await tap(page, more.first());
    await opened(page, more.first());
  });

  test("a tap on a row beside an open menu only closes it", async ({ page }) => {
    const { more, low } = await rows(page, 780);
    await low.tap();
    await clearOfMenu(page, (await lineOf(more.first()).boundingBox())!);
    await tap(page, sizeOf(more.first()));
    await expect(open(page)).toHaveCount(0);
    await expect(lineOf(more.first())).not.toHaveAttribute("aria-selected", "true");
    await expect(page.locator("[data-inspector]")).toHaveCount(0);
  });

  test("a tap on the header over a hidden row's More only closes the menu", async ({ page }) => {
    const { more, low } = await rows(page, 780);
    const hidden = await underHeader(page, more);
    await low.tap();
    await expect(open(page)).toHaveCount(1);
    await tap(page, hidden);
    await expect(open(page)).toHaveCount(0);
    await expect(page.getByRole("menu")).toHaveCount(0);
  });
});

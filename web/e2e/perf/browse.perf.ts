import { expect, test } from "@playwright/test";

import { wideBrowse } from "../support/fakes";
import { visit } from "../support/login";
import { workPerFrame } from "../support/perf";

test("flings through a long browse tree at sixty frames a second", async ({ page }) => {
  await wideBrowse(page, 5000);
  await visit(page, "/users/carol/shares");
  const tree = page.getByRole("treegrid", { name: "Shared folders" });
  await expect(tree.locator("[role=rowgroup] [role=row]").first()).toBeVisible();
  await page.getByRole("button", { name: "Expand all" }).click();
  await expect(tree.getByText("1.flac").first()).toBeVisible();
  const task = await workPerFrame(page, "fling", () =>
    // half a screen of new lines every frame, as a fast fling brings them, for more frames than
    // are counted
    tree.evaluate(
      (box) =>
        new Promise<void>((done) => {
          let frames = 0;
          const step = () => {
            box.scrollTop += box.clientHeight / 2;
            if (++frames > 130) done();
            else requestAnimationFrame(step);
          };
          step();
        })
    )
  );
  expect(task).toBeLessThan(1000 / 60);
});

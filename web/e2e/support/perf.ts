import { type Page, test } from "@playwright/test";

/**
 * Runs `act` while the page draws 120 frames, notes the frames' gaps and the main thread's work
 * with the test's result, where the report shows them, and gives that work per frame. The main
 * thread's work is what decides whether a frame is ready in time; the gaps also count this
 * headless browser's software compositing. `act` is told once the frames are counted
 */
export const workPerFrame = async (
  page: Page,
  what: string,
  act: (counted: () => boolean) => Promise<void>
) => {
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Performance.enable");
  const work = async () => {
    const { metrics } = await cdp.send("Performance.getMetrics");
    const value = (name: string) => metrics.find((m) => m.name === name)!.value * 1000;
    return { task: value("TaskDuration"), script: value("ScriptDuration") };
  };
  const before = await work();
  let counted = false;
  const frames = page
    .evaluate(
      () =>
        new Promise<number[]>((done) => {
          const times: number[] = [];
          const tick = (t: number) => {
            times.push(t);
            if (times.length < 121) requestAnimationFrame(tick);
            else done(times.slice(1).map((t, i) => t - times[i]));
          };
          requestAnimationFrame(tick);
        })
    )
    .then((gaps) => {
      counted = true;
      return gaps;
    });
  await act(() => counted);
  const gaps = (await frames).toSorted((a, b) => a - b);
  const after = await work();
  const mean = gaps.reduce((a, b) => a + b, 0) / gaps.length;
  const p95 = gaps[Math.floor(gaps.length * 0.95)];
  const task = (after.task - before.task) / gaps.length;
  const script = (after.script - before.script) / gaps.length;
  test.info().annotations.push({
    type: what,
    description:
      `frames ${mean.toFixed(1)} ms on average, p95 ${p95.toFixed(1)} ms; main thread ` +
      `${task.toFixed(1)} ms a frame, ${script.toFixed(1)} ms of it script`
  });
  return task;
};

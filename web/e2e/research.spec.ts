import { expect, test } from "@playwright/test";

test.describe("research site", () => {
  test("opens on the overview with counts computed from the data", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("heading", { level: 1 })).toContainText("scheduling policy is an experimental variable");
    const facts = page.locator(".facts");
    await expect(facts).toContainText("1,463");
    await expect(facts).toContainText("5.44 M");
    await expect(page.getByText(/of \d+ numbers quoted in the report are recomputed/)).toBeVisible();
  });

  test("the section navigation moves to the section and marks it current", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("navigation", { name: "Sections" }).getByRole("link", { name: "Scheduler failover" }).click();
    await expect(page).toHaveURL(/#failover$/);
    await expect(page.getByRole("heading", { name: "What a scheduler failure costs" })).toBeInViewport();
    await expect(page.getByRole("navigation", { name: "Sections" }).getByRole("link", { name: "Scheduler failover" })).toHaveAttribute(
      "aria-current",
      "true",
    );
  });

  test("selectors change what the chart and its table show", async ({ page }) => {
    await page.goto("/#load");
    const fig = page.locator("#fig-sweep");
    await expect(fig.getByRole("heading")).toContainText("Uniform");
    const workload = fig.getByRole("group", { name: "workload" });
    await workload.getByRole("button", { name: "Heavy-tailed" }).click();
    await expect(workload.getByRole("button", { name: "Heavy-tailed" })).toHaveAttribute("aria-pressed", "true");
    await expect(fig.getByRole("heading")).toContainText("Heavy-tailed");
    await fig.getByRole("group", { name: "metric" }).getByRole("button", { name: "deadline miss rate" }).click();
    await fig.getByText("Numbers behind this chart").click();
    const table = fig.getByRole("table");
    await expect(table).toBeVisible();
    // EDF's heavy-tailed miss rate at 1.25x: exactly 0.0055, printed 0.005 in the report.
    await expect(table.getByRole("row", { name: /^1\.25×/ })).toContainText("0.0055");
    await expect(fig.locator("svg[role=img]")).toHaveCount(1);
  });

  test("choosing the bursty main-matrix cells shows the load caveat", async ({ page }) => {
    await page.goto("/#matrix");
    await expect(page.getByText("Not load-matched")).toHaveCount(0);
    await page.locator("#matrix").getByRole("group", { name: "workload" }).getByRole("button", { name: /Bursty/ }).click();
    await expect(page.getByText("Not load-matched")).toBeVisible();
    await expect(page.locator("#matrix .note.caveat")).toContainText("1.57×");
  });

  test("censored failover trials are labelled, never plotted as a value", async ({ page }) => {
    await page.goto("/#failover");
    const fig = page.locator("#fig-failover");
    await expect(fig.locator("svg")).toContainText("never recovered");
    await fig.getByText("Numbers behind this chart").click();
    await expect(fig.getByRole("row", { name: /single replica/ })).toContainText("never recovered");
  });

  test("every figure says where its data came from", async ({ page }) => {
    await page.goto("/");
    await page.waitForSelector("#fig-store svg");
    const figures = page.locator("figure.fig:has(.provenance summary)");
    const n = await figures.count();
    expect(n).toBeGreaterThanOrEqual(12);
    for (let i = 0; i < n; i++) {
      await expect(figures.nth(i).locator(".provenance summary")).toContainText("results/");
    }
  });

  test("a dataset that fails to load is reported, and nothing is drawn in its place", async ({ page }) => {
    await page.route("**/data/sweep.json", (r) => r.fulfill({ status: 503, body: "unavailable" }));
    await page.goto("/#load");
    await expect(page.locator("#load").getByRole("alert")).toContainText("could not be shown");
    await expect(page.locator("#fig-sweep")).toHaveCount(0);
    // The rest of the page is unaffected.
    await expect(page.locator("#fig-matrix svg")).toBeVisible();
  });

  test("a dataset that fails validation is reported, not plotted", async ({ page }) => {
    await page.route("**/data/baseline.json", async (route) => {
      const res = await route.fetch();
      const body = await res.json();
      body.rows[0].throughput_per_s = "8000";
      await route.fulfill({ response: res, json: body });
    });
    await page.goto("/#cost");
    await expect(page.locator("#cost").getByRole("alert")).toContainText("throughput_per_s");
    await expect(page.locator("#fig-asynq")).toHaveCount(0);
  });

  test("the pending experiment is shown as pending, with no figure", async ({ page }) => {
    await page.goto("/#limitations");
    const note = page.locator("#limitations .note.pending");
    await expect(note).toContainText("Pending");
    await expect(note).toContainText("partitioned from a Redis");
    await expect(page.locator("#limitations figure")).toHaveCount(0);
  });

  test("keyboard: the skip link reaches the content and controls are buttons", async ({ page }) => {
    await page.goto("/");
    await page.keyboard.press("Tab");
    await expect(page.getByRole("link", { name: "Skip to content" })).toBeFocused();
    await page.keyboard.press("Enter");
    await expect(page).toHaveURL(/#main$/);
    const btn = page.locator("#fig-sweep").getByRole("button", { name: "Bursty" });
    await btn.focus();
    await page.keyboard.press("Enter");
    await expect(btn).toHaveAttribute("aria-pressed", "true");
  });

  test("the report PDF and the data files are served", async ({ request }) => {
    for (const path of ["/docs/report.pdf", "/data/index.json", "/data/failover.json"]) {
      const res = await request.get(path);
      expect(res.status(), path).toBe(200);
    }
  });

  for (const width of [1024, 768]) {
    test(`no horizontal overflow at ${width}px`, async ({ page }) => {
      await page.setViewportSize({ width, height: 1000 });
      await page.goto("/");
      await page.waitForSelector("#fig-store svg");
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
      expect(overflow).toBeLessThanOrEqual(0);
    });
  }
});

import { expect, test, type Page, type Route } from "@playwright/test";

// The live page against a mocked console API. These tests check what the page
// says in each state; the console itself is tested against real processes in
// internal/console (go test -tags integration). A run against a real console
// is CONSOLE_E2E=1, at the bottom.

const now = () => new Date().toISOString();

function snapshot(over: Record<string, unknown> = {}) {
  return {
    seq: 10,
    sampled_at: now(),
    redis: { ok: true, latency_ms: 0.3, namespace: "tq-console-demo" },
    leadership: { held: true, owner: "sched-0", epoch: 3, lease_remaining_ms: 4200, fence: 3, epoch_counter: 3 },
    queue: { pending: 12, not_admitted: 0, in_flight: 8, unacked: 8, dead_lettered: 0, counters: { retried: 0 }, tenant_completed: {} },
    rates: { window_s: 1, dispatched_per_s: 150, completed_per_s: 149 },
    components: [
      { name: "sched-0", role: "scheduler", url: "http://127.0.0.1:19501", health: "ready", metrics: { tq_scheduler_is_leader: 1, tq_scheduler_leader_epoch: 3 }, last_ok_at: now(), process: { state: "running", link: "connected", restarts: 0, pid: 11 } },
      { name: "sched-1", role: "scheduler", url: "http://127.0.0.1:19502", health: "ready", metrics: { tq_scheduler_is_leader: 0 }, last_ok_at: now(), process: { state: "running", link: "connected", restarts: 0, pid: 12 } },
    ],
    leader_sum: 1,
    policy: "wfq",
    ...over,
  };
}

const info = (over: Record<string, unknown> = {}) => ({ mode: "demo", started_at: now(), server_time: now(), poll_ms: 1000, faults_enabled: true, ...over });

const controls = {
  faults_enabled: true,
  actions: { scheduler: ["kill", "terminate", "freeze", "partition", "restart"], worker: ["kill", "freeze", "restart"] },
  processes: { "sched-0": { state: "running", restarts: 0 }, "sched-1": { state: "running", restarts: 0 } },
  min_duration_ms: 1000,
  max_duration_ms: 30000,
  presets: [{ id: "steady", label: "Steady, below capacity", description: "d", type: "uniform", count: 1500, offered_load: 0.7 }],
};

async function mock(page: Page, opts: { status?: (r: Route) => Promise<void>; onPost?: (r: Route) => Promise<void> } = {}) {
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const json = (body: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
    if (route.request().method() === "POST") return opts.onPost ? opts.onPost(route) : json({ result: "ok" });
    switch (url.pathname) {
      case "/api/v1/status":
        return opts.status ? opts.status(route) : json({ console: info(), snapshot: snapshot() });
      case "/api/v1/events":
        return json({ events: [{ seq: 1, at: now(), source: "log", kind: "lease_acquired", subject: "sched-0", message: "became scheduler leader (epoch 3)" }] });
      case "/api/v1/controls":
        return json(controls);
      case "/api/v1/history":
        return json({ console: info(), samples: [] });
      case "/api/v1/dispatches":
        return json({ read_at: now(), from_ms: Date.now() - 90_000, window_s: 90, bucket_ms: 250, count: 0, truncated: false, buckets: [], epochs: [], unstamped: 0, epoch_inversions: 0, gaps: [] });
      case "/api/v1/tenants":
        return json({ read_at: now(), window_s: 120, count: 30, truncated: false, tenants: [
          { tenant: "A", completed: 27, latency_p50_ms: 2400, latency_p95_ms: 2900, latency_max_ms: 3100, deadline_missed: 20 },
          { tenant: "B", completed: 3, latency_p50_ms: 60, latency_p95_ms: 78, latency_max_ms: 80, deadline_missed: 0 } ] });
      case "/api/v1/dlq":
        return json({ read_at: now(), entries: [] });
    }
    return json({ error: "no such endpoint" }, 404);
  });
}

test.describe("live console page", () => {
  test("with no console behind it, says so and explains how to run one", async ({ page }) => {
    // What a static host such as GitHub Pages answers for /api: an HTML 404.
    await page.route("**/api/**", (r) => r.fulfill({ status: 404, contentType: "text/html", body: "<!doctype html><title>404</title>" }));
    await page.goto("/#/live");
    await expect(page.getByRole("heading", { name: "No live console is connected" })).toBeVisible();
    await expect(page.getByText("make console-demo")).toBeVisible();
    await expect(page.locator(".kpis")).toHaveCount(0);
  });

  test("shows live telemetry and labels it as current, not as a result", async ({ page }) => {
    await mock(page);
    await page.goto("/#/live");
    await expect(page.locator(".conn .tag")).toHaveText("live");
    await expect(page.getByText("None of it is a published result")).toBeVisible();
    await expect(page.locator(".kpis")).toContainText("150");
    await expect(page.getByText("exactly one", { exact: true })).toBeVisible();
    await expect(page.getByText("became scheduler leader (epoch 3)")).toBeVisible();
    const tenants = page.locator("section", { has: page.getByRole("heading", { name: /^Tenants/ }) });
    await expect(tenants.getByRole("row", { name: /^B/ })).toContainText("78 ms");
    await expect(tenants.getByRole("heading")).toContainText("policy wfq");
  });

  test("a console that stops answering is shown as not current, with its last data greyed", async ({ page }) => {
    let up = true;
    await mock(page, {
      status: async (r) =>
        up
          ? r.fulfill({ contentType: "application/json", body: JSON.stringify({ console: info(), snapshot: snapshot() }) })
          : r.abort("connectionrefused"),
    });
    await page.goto("/#/live");
    await expect(page.locator(".conn .tag")).toHaveText("live");
    up = false;
    await expect(page.locator(".conn .tag")).toHaveText("console not answering", { timeout: 5000 });
    await expect(page.getByText("Not current")).toBeVisible();
    await expect(page.locator(".live-grid")).toHaveAttribute("data-old", "true");
    await expect(page.locator(".conn")).toContainText("retrying in");
    up = true;
    await expect(page.locator(".conn .tag")).toHaveText("live", { timeout: 12_000 });
    await expect(page.getByText("Not current")).toHaveCount(0);
  });

  test("a console whose own poll has stopped is shown as stale", async ({ page }) => {
    const old = new Date(Date.now() - 20_000).toISOString();
    await mock(page, { status: (r) => r.fulfill({ contentType: "application/json", body: JSON.stringify({ console: info(), snapshot: snapshot({ sampled_at: old }) }) }) });
    await page.goto("/#/live");
    await expect(page.locator(".conn .tag")).toHaveText("stale");
    await expect(page.getByText("has not completed a poll recently")).toBeVisible();
  });

  test("Redis being unreachable makes queue figures unknown, not zero", async ({ page }) => {
    await mock(page, {
      status: (r) =>
        r.fulfill({
          contentType: "application/json",
          body: JSON.stringify({ console: info(), snapshot: snapshot({ redis: { ok: false, error: "dial tcp: connection refused", namespace: "x" }, queue: undefined, leader_sum: null }) }),
        }),
    });
    await page.goto("/#/live");
    await expect(page.getByText("Redis unreachable")).toBeVisible();
    await expect(page.locator(".kpi").first()).toContainText("—");
    await expect(page.getByText("Unknown: Redis, which holds the lease, could not be read.")).toBeVisible();
  });

  test("two schedulers claiming to lead is flagged, and explained", async ({ page }) => {
    await mock(page, { status: (r) => r.fulfill({ contentType: "application/json", body: JSON.stringify({ console: info(), snapshot: snapshot({ leader_sum: 2 }) }) }) });
    await page.goto("/#/live");
    await expect(page.getByText("2 at once")).toBeVisible();
    await expect(page.getByText("the fence refuses the others at Redis")).toBeVisible();
  });

  test("without the token the controls are read-only", async ({ page }) => {
    await mock(page);
    await page.goto("/#/live");
    await expect(page.getByText("read-only")).toBeVisible();
    await expect(page.getByRole("button", { name: "Kill" })).toBeDisabled();
  });

  test("the token leaves the address bar, and a kill needs a second click", async ({ page }) => {
    const posts: { path: string; token: string | null; body: unknown }[] = [];
    await mock(page, {
      onPost: async (r) => {
        posts.push({ path: new URL(r.request().url()).pathname, token: r.request().headers()["x-console-token"] ?? null, body: r.request().postDataJSON() });
        await r.fulfill({ contentType: "application/json", body: JSON.stringify({ result: "sent SIGKILL to sched-0 (pid 11)" }) });
      },
    });
    await page.goto("/#/live?token=0123456789abcdef0123");
    await expect(page).toHaveURL(/#\/live$/);
    const kill = page.getByRole("button", { name: "Kill", exact: true });
    await kill.click();
    await expect(page.getByRole("button", { name: /Confirm: kill sched-0/ })).toBeVisible();
    expect(posts).toHaveLength(0);
    await page.getByRole("button", { name: /Confirm: kill sched-0/ }).click();
    await expect(page.getByText("sent SIGKILL to sched-0 (pid 11)")).toBeVisible();
    expect(posts).toEqual([{ path: "/api/v1/faults", token: "0123456789abcdef0123", body: { action: "kill", target: "leader" } }]);
  });

  test("an armed kill disarms itself", async ({ page }) => {
    await mock(page);
    await page.goto("/#/live?token=0123456789abcdef0123");
    await page.getByRole("button", { name: "Kill", exact: true }).click();
    await expect(page.getByRole("button", { name: /Confirm: kill/ })).toBeVisible();
    await expect(page.getByRole("button", { name: /Confirm: kill/ })).toHaveCount(0, { timeout: 6000 });
  });

  test("a refused control is reported, and a rejected token is dropped", async ({ page }) => {
    await mock(page, { onPost: (r) => r.fulfill({ status: 401, contentType: "application/json", body: JSON.stringify({ error: "missing or wrong console token" }) }) });
    await page.goto("/#/live?token=0123456789abcdef0123");
    await page.getByRole("button", { name: "Steady, below capacity" }).click();
    await expect(page.getByText(/refused: missing or wrong console token/)).toBeVisible();
    await expect(page.getByText("read-only")).toBeVisible();
  });

  test("monitor mode explains why there are no fault controls", async ({ page }) => {
    await mock(page);
    // Registered after the catch-all, so it takes precedence.
    await page.route("**/api/v1/controls", (r) =>
      r.fulfill({ contentType: "application/json", body: JSON.stringify({ faults_enabled: false, reason: "monitor mode: the console did not start these processes, so it will not signal them", min_duration_ms: 1000, max_duration_ms: 30000 }) }),
    );
    await page.goto("/#/live");
    await expect(page.getByText("monitor mode: the console did not start these processes")).toBeVisible();
  });
});

test.describe("against a running console", () => {
  test.skip(!process.env.CONSOLE_E2E, "set CONSOLE_E2E=1 with `make console-demo` running");
  test("the real console answers and reports a leader", async ({ page }) => {
    await page.goto("http://127.0.0.1:8088/#/live");
    await expect(page.locator(".conn .tag")).toHaveText("live", { timeout: 10_000 });
    await expect(page.getByText("lease holder")).toBeVisible();
  });
});

// Records the live-console walkthrough as a video, against a real demo cluster.
//
//   make build && (cd web && npm run build)
//   node web/scripts/record-demo.mjs [--out web/demo-video] [--redis localhost:6379]
//
// It starts `bin/console --demo --enable-faults` on its own port, drives the
// page the way a person would (submit work, kill, freeze and partition the
// leader), and records the browser. Every step waits for the system to reach
// the state its caption describes — a new leader's own log line, a process the
// OS reports stopped, a fence refusal — and aborts the recording if it does
// not, so the video can only show what actually happened. Numbers in captions
// are read from the page at that moment, never written here.
//
// Captions are drawn in a bar labelled "narration", visibly not part of the
// application. Frames come from Chrome's own screencast and are encoded to
// <out>/demo.mp4 by encode-frames.swift, with macOS's AVFoundation (macOS
// only; Playwright's bundled encoder is not signed for recent macOS).

import { execFileSync, spawn } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const arg = (name, def) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : def;
};
const out = resolve(arg("out", join(root, "web", "demo-video")));
const redis = arg("redis", "localhost:6379");
const listen = "127.0.0.1:8099";
const W = 1440;
const H = 900;

function startConsole() {
  return new Promise((resolveUrl, reject) => {
    const proc = spawn(
      join(root, "bin", "console"),
      ["--demo", "--enable-faults", "--redis-addr", redis, "--listen", listen, "--static", join(root, "web", "dist"), "--metrics-port", "19551", "--log-level", "warn"],
      { cwd: root, stdio: ["ignore", "ignore", "pipe"] },
    );
    let buf = "";
    const timer = setTimeout(() => reject(new Error(`console did not print its URL:\n${buf}`)), 20_000);
    proc.stderr.on("data", (d) => {
      buf += d;
      const m = buf.match(/(http:\/\/127\.0\.0\.1:\d+\/#\/live\?token=[0-9a-f]+)/);
      if (m) {
        clearTimeout(timer);
        resolveUrl({ proc, url: m[1] });
      }
    });
    proc.on("exit", (code) => reject(new Error(`console exited (${code}):\n${buf}`)));
  });
}

function stopConsole(proc) {
  return new Promise((done) => {
    if (proc.exitCode !== null) return done();
    proc.once("exit", done);
    proc.kill("SIGTERM");
    setTimeout(() => proc.kill("SIGKILL"), 10_000).unref();
  });
}

// ------------------------------------------------------------------ helpers --

const OVERLAY = `
(() => {
  if (document.getElementById('__narration')) return;
  const bar = document.createElement('div');
  bar.id = '__narration';
  bar.setAttribute('aria-hidden', 'true');
  bar.style.cssText = 'position:fixed;left:0;right:0;bottom:0;z-index:99999;background:rgba(20,20,19,.94);color:#f2f1ec;' +
    'font:15px/1.45 "IBM Plex Sans",system-ui,sans-serif;padding:12px 28px 14px;display:none;border-top:2px solid #c79be0';
  bar.innerHTML = '<div style="font:600 11px/1 \\'IBM Plex Mono\\',monospace;letter-spacing:.08em;text-transform:uppercase;color:#c79be0;margin-bottom:6px">' +
    'narration <span id="__step" style="color:#9a9890"></span></div><div id="__text"></div>';
  document.body.appendChild(bar);
})();`;

async function caption(page, step, text, holdMs = 0) {
  await page.evaluate(OVERLAY);
  await page.evaluate(
    ([s, t]) => {
      document.getElementById("__step").textContent = s;
      document.getElementById("__text").textContent = t;
      document.getElementById("__narration").style.display = "block";
    },
    [step, text],
  );
  if (holdMs) await page.waitForTimeout(holdMs);
}

async function show(page, locator, block = "start") {
  await locator.first().evaluate((el, b) => el.scrollIntoView({ behavior: "smooth", block: b }), block);
  await page.waitForTimeout(900);
}

async function press(page, locator) {
  await show(page, locator.first(), "center");
  // Hold the element itself: a button's label (and so its locator) can change
  // when clicked, as Kill does when it arms.
  const el = await locator.first().elementHandle();
  await el.evaluate((n) => {
    n.style.outline = "3px solid #c79be0";
    n.style.outlineOffset = "2px";
  });
  await page.waitForTimeout(700);
  await el.click();
  // The pointer is invisible in the capture but still hovers: park it on the
  // empty middle of the top bar so no chart tooltip is left open.
  await page.mouse.move(W / 2, 26);
  await page.waitForTimeout(150);
  await el.evaluate((n) => (n.style.outline = "")).catch(() => {});
}

/** Waits for an event line in the console's own event list, newer than `after` events. */
async function waitForEvent(page, pattern, timeout = 15_000) {
  await page.getByRole("button", { name: "all", exact: true }).click().catch(() => {});
  await page.locator(".events li", { hasText: pattern }).first().waitFor({ timeout });
  return (await page.locator(".events li", { hasText: pattern }).first().innerText()).replace(/\s+/g, " ");
}

const count = (page, pattern) => page.locator(".events li", { hasText: pattern }).count();

/**
 * After a fault ends, which of the two safe outcomes happened: the fence
 * refused a write, or the process's own lease renewal noticed first and it
 * stood down without writing. Whichever the system did is what gets narrated.
 */
async function outcome(page, fencedBefore, lostBefore, timeout = 20_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if ((await count(page, /fenced out by a newer scheduler epoch/)) > fencedBefore) return "fenced";
    if ((await count(page, /lost the scheduler lease/)) > lostBefore) {
      // Give a refusal that lands in the same instant a moment to arrive.
      await page.waitForTimeout(2500);
      return (await count(page, /fenced out by a newer scheduler epoch/)) > fencedBefore ? "fenced" : "lease";
    }
    await page.waitForTimeout(500);
  }
  return null;
}

/** Submits a workload preset and waits until the console accepted it (it refuses while one is running). */
async function submit(page, name) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    await press(page, page.getByRole("button", { name }));
    await page.waitForTimeout(400);
    const r = await text(page, ".result");
    if (!/refused/.test(r)) return;
    await page.waitForTimeout(1500);
  }
  throw new Error(`workload ${name} was never accepted`);
}

/** Reads the console API from inside the page, as the page itself does. */
const api = (page, path) => page.evaluate((p) => fetch(`/api/v1${p}`).then((r) => r.json()), path);

const text = async (page, sel) => (await page.locator(sel).first().innerText()).replace(/\s+/g, " ").trim();
const pairValue = (page, label) => page.locator(`dt:text-is("${label}") + dd`).first().innerText();

// --------------------------------------------------------------------- run --

const { proc, url } = await startConsole();
rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
const framesDir = join(out, "frames");
mkdirSync(framesDir);
// The screencast captures the window's own surface, not the emulated
// viewport, so the window must be exactly the viewport's size: left at its
// default it is shorter, and the bottom of every frame (the narration bar
// included) is silently cut off.
const browser = await chromium.launch({ channel: process.env.PW_CHANNEL ?? "chrome", args: [`--window-size=${W},${H}`] });
const context = await browser.newContext({ viewport: { width: W, height: H }, deviceScaleFactor: 1, colorScheme: "light" });
const page = await context.newPage();
const frames = [];
const wrongSize = new Set();
const cdp = await context.newCDPSession(page);
cdp.on("Page.screencastFrame", async ({ data, metadata, sessionId }) => {
  if (metadata.deviceWidth !== W || metadata.deviceHeight !== H) wrongSize.add(`${metadata.deviceWidth}x${metadata.deviceHeight}`);
  const file = `f${String(frames.length).padStart(6, "0")}.jpg`;
  writeFileSync(join(framesDir, file), Buffer.from(data, "base64"));
  frames.push({ file, t: metadata.timestamp });
  await cdp.send("Page.screencastFrameAck", { sessionId }).catch(() => {});
});
await cdp.send("Page.startScreencast", { format: "jpeg", quality: 90, maxWidth: W, maxHeight: H, everyNthFrame: 1 });
const base = url.slice(0, url.indexOf("/#"));
let ok = false;

try {
  // ---- Research site: what was measured.
  await page.goto(`${base}/#overview`);
  await page.locator(".facts").waitFor();
  await caption(page, "· the research site", "A task queue in Go on Redis Streams with four pluggable scheduling policies and one fenced scheduler. The research site publishes the study behind it; every number is recomputed from the raw benchmark data.", 7000);
  await show(page, page.locator("#fig-failover"));
  await caption(page, "· the research site", "The measured result this demo will reproduce once: killing the leading scheduler stops dispatch for 3.5 to 5 s against a 5 s lease, inside the window a two-term model predicts.", 8000);

  // ---- Live console: the same system, running now.
  await page.goto(url);
  await page.locator(".conn .tag", { hasText: "live" }).waitFor({ timeout: 15_000 });
  await page.getByText("exactly one", { exact: true }).waitFor({ timeout: 15_000 });
  const leader0 = (await pairValue(page, "lease holder")).trim();
  const epoch0 = (await pairValue(page, "epoch")).trim();
  await caption(page, "1 / 6 · baseline", `The live console, started on this laptop with two schedulers and two workers. ${leader0} holds the lease under epoch ${epoch0}; the other scheduler stands by. Everything below is read from Redis and from each process's own endpoints.`, 8000);

  // ---- Load.
  await submit(page, "Overload");
  await show(page, page.getByRole("heading", { name: "The last five minutes" }));
  await caption(page, "2 / 6 · overload", "A workload at 1.5× capacity. Dispatch runs at the workers' capacity while the backlog builds in the scheduler's pending set (right-hand chart), which is where the policy decides who waits.", 9000);
  // Keep work flowing through the kill, so the gap measured below is the
  // outage and not an idle queue.
  await show(page, page.getByRole("heading", { name: "Controls" }));
  await submit(page, /^Steady/);
  await page.waitForTimeout(2500);

  // ---- Kill.
  await show(page, page.getByRole("heading", { name: "Controls" }));
  await caption(page, "3 / 6 · crash", "Kill the leading scheduler with SIGKILL. A crash releases nothing, so the standby has to wait for the lease to expire. A kill needs a second click to confirm.");
  await press(page, page.getByRole("button", { name: "Kill", exact: true }));
  await page.getByRole("button", { name: "all", exact: true }).click();
  const acquiredBefore = await count(page, /became scheduler leader/);
  const killAt = Date.now();
  await press(page, page.getByRole("button", { name: /^Confirm: kill/ }));
  const killed = (await text(page, ".result")).match(/to (\S+) \(pid/)?.[1];
  if (!killed) throw new Error("kill was not confirmed by the console");
  await show(page, page.getByRole("heading", { name: "Events" }), "center");
  // Wait for a new line, not any line: the startup election wrote one too.
  // (Polled through locators: the console's CSP forbids the eval that
  // page.waitForFunction relies on, as it should.)
  for (const until = Date.now() + 15_000; (await count(page, /became scheduler leader/)) <= acquiredBefore; ) {
    if (Date.now() > until) throw new Error("no new leader logged an acquisition after the kill");
    await page.waitForTimeout(250);
  }
  const acquired = await waitForEvent(page, /became scheduler leader/, 1000);
  await caption(page, "3 / 6 · crash", `${killed} is gone. The standby's own log, not the console's guess, records the handover: "${acquired.replace(/^.*?log\s*/i, "")}".`, 7000);
  await show(page, page.locator("#disp-h"));
  await page.waitForTimeout(3000);
  const win = await api(page, "/dispatches?window_s=90");
  if (!win.max_gap_end_ms || win.max_gap_end_ms < killAt) throw new Error("the longest dispatch gap is not the one after the kill; refusing to call it the outage");
  await caption(page, "3 / 6 · crash", `The dispatch log, read from the execution stream itself: dispatch stopped for ${win.max_gap_ms.toLocaleString("en-GB")} ms while the lease ran out, then resumed under epoch ${Math.max(...win.epochs)}. Epoch inversions: ${win.epoch_inversions}.`, 8000);

  // ---- Restart and keep work flowing.
  await show(page, page.getByRole("heading", { name: "Controls" }));
  await page.locator("select").first().selectOption(killed);
  await caption(page, "4 / 6 · restore", `Restart ${killed}. It rejoins as the standby, and a steady workload keeps tasks flowing for the next fault.`);
  await press(page, page.getByRole("button", { name: "Restart", exact: true }));
  await page.waitForTimeout(3000);
  await submit(page, /^Steady/);
  await page.waitForTimeout(2500);

  // ---- Freeze.
  await page.locator("select").first().selectOption("leader");
  await page.getByRole("button", { name: "all", exact: true }).click();
  let fencedBefore = await count(page, /fenced out by a newer scheduler epoch/);
  let lostBefore = await count(page, /lost the scheduler lease/);
  await caption(page, "5 / 6 · gray failure", "Freeze the new leader with SIGSTOP for 10 s, twice its lease. It is not dead: when it resumes it will still believe it leads. This is the case fencing exists for.");
  await press(page, page.getByRole("button", { name: "Freeze", exact: true }));
  const frozenMsg = await text(page, ".result");
  if (!/verified stopped/.test(frozenMsg)) throw new Error(`freeze not verified: ${frozenMsg}`);
  await show(page, page.getByRole("heading", { name: "Processes" }));
  await page.locator("td", { hasText: "stopped (SIGSTOP)" }).waitFor({ timeout: 5000 });
  await caption(page, "5 / 6 · gray failure", `The OS confirms the process is stopped (${frozenMsg.match(/ps state \w+/)?.[0] ?? "ps"}). Its endpoints stop answering, so the console reports it unreachable and its leadership unknown — it cannot tell frozen from dead, and says so.`, 7000);
  await show(page, page.getByRole("heading", { name: "Events" }), "center");
  const freezeEnd = await outcome(page, fencedBefore, lostBefore);
  if (!freezeEnd) throw new Error("the resumed leader neither was fenced nor lost its lease");
  await caption(
    page,
    "5 / 6 · gray failure",
    freezeEnd === "fenced"
      ? "It resumed after 10 s, still holding its old epoch, and tried to write before its own lease logic noticed anything. Redis refused the write: the epoch check runs inside the same atomic script as the write it guards."
      : "It resumed after 10 s and, this time, its own lease renewal failed first, so it stood down without attempting a write. That is the other safe outcome; the report measured the fence doing the refusing in 11 of 15 such trials.",
    9000,
  );

  // ---- Partition.
  await show(page, page.getByRole("heading", { name: "Controls" }));
  await submit(page, /^Steady/);
  await page.waitForTimeout(2000);
  fencedBefore = await count(page, /fenced out by a newer scheduler epoch/);
  lostBefore = await count(page, /lost the scheduler lease/);
  await caption(page, "6 / 6 · partition", "Now cut the current leader off from Redis for 10 s, through its own proxy. Unlike a frozen process it keeps running, watches every call fail, and cannot tell an unreachable Redis from having been replaced.");
  await press(page, page.getByRole("button", { name: "Partition", exact: true }));
  await show(page, page.getByRole("heading", { name: "Leadership" }));
  const twoClaim = await page.getByText(/^\d+ at once$/).waitFor({ timeout: 12_000 }).then(() => true, () => false);
  await caption(
    page,
    "6 / 6 · partition",
    twoClaim
      ? "Two schedulers now claim to lead: the standby took the expired lease, and the cut-off leader still believes it holds it. Only the newer epoch can write."
      : "The standby has taken the expired lease. The cut-off leader cannot be reached to ask what it believes, so the console reports its claim as unknown rather than guessing.",
    7000,
  );
  await show(page, page.getByRole("heading", { name: "Events" }), "center");
  const healEnd = await outcome(page, fencedBefore, lostBefore);
  if (!healEnd) throw new Error("the healed leader neither was fenced nor lost its lease");
  await caption(
    page,
    "6 / 6 · partition",
    healEnd === "fenced"
      ? "When the link healed, its first write under the old epoch was refused at Redis, and it stood down. Exactly one leader again."
      : "When the link healed, its lease renewal was told it had been replaced before it wrote anything, and it stood down. Exactly one leader again.",
    7000,
  );

  await show(page, page.locator("#disp-h"));
  await page.waitForTimeout(2500);
  const whole = await api(page, "/dispatches?window_s=300");
  if (whole.truncated) throw new Error("the whole-run dispatch read was truncated");
  await caption(page, "end", `Over the whole run: ${whole.count.toLocaleString("en-GB")} dispatches under ${whole.epochs.length} leadership terms, and ${whole.epoch_inversions} epoch inversions. This was one observation on one laptop; the measured distributions, and what they do not show, are on the research site.`, 9000);
  ok = true;
} finally {
  await cdp.send("Page.stopScreencast").catch(() => {});
  await context.close();
  await browser.close();
  await stopConsole(proc);
}

if (!ok) process.exit(1);
if (wrongSize.size) {
  console.error(`captured frames were ${[...wrongSize].join(", ")}, not ${W}x${H}; refusing to encode a cropped video`);
  process.exit(1);
}
writeFileSync(join(framesDir, "frames.json"), JSON.stringify(frames));
const encoder = join(tmpdir(), "tq-encode-frames");
execFileSync("xcrun", ["swiftc", "-O", join(root, "web", "scripts", "encode-frames.swift"), "-o", encoder], { stdio: "inherit" });
execFileSync(encoder, [framesDir, join(out, "demo.mp4")], { stdio: "inherit" });
rmSync(framesDir, { recursive: true, force: true });

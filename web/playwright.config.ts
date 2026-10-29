import { defineConfig } from "@playwright/test";

// Browser tests run against the production build, served by `vite preview`,
// in the locally installed Chrome (no browser download needed). The live
// console's tests that need a running backend are in e2e/live.spec.ts and are
// skipped unless CONSOLE_E2E=1.
export default defineConfig({
  testDir: "e2e",
  timeout: 30_000,
  fullyParallel: true,
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:4174",
    channel: process.env.PW_CHANNEL ?? "chrome",
    viewport: { width: 1400, height: 900 },
  },
  webServer: {
    command: "npm run build && npx vite preview --host 127.0.0.1 --port 4174 --strictPort",
    url: "http://127.0.0.1:4174",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});

/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// BASE is the path the site is served under. GitHub Pages serves a project
// site at /<repo>/, so the Pages workflow sets it; locally it is "/".
//
// The /api proxy is for the live console only. It forwards to the console's
// loopback listener, and rewrites Host so the console's DNS-rebinding check
// sees the address it is actually bound to.
const consoleAddr = process.env.CONSOLE_ADDR ?? "127.0.0.1:8088";

export default defineConfig({
  base: process.env.BASE ?? "/",
  plugins: [react()],
  build: { outDir: "dist", sourcemap: true, chunkSizeWarningLimit: 700 },
  server: {
    host: "127.0.0.1",
    proxy: { "/api": { target: `http://${consoleAddr}`, changeOrigin: true } },
  },
  test: { include: ["src/**/*.test.ts"], environment: "node" },
});

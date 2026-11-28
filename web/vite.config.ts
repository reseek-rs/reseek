/// <reference types="vitest/config" />
import adapter from "@sveltejs/adapter-static";
import { sveltekit } from "@sveltejs/kit/vite";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

import { bundledPackages } from "./scripts/licenses/bundled.ts";

// `just licenses` names the file the bundled npm packages are written to
const licenses = process.env.RESEEK_WEB_LICENSES;

export default defineConfig({
  plugins: [
    tailwindcss(),
    // the hash router keeps every screen under one index.html, so one build works under any URL
    // base the daemon is given
    sveltekit({
      adapter: adapter(),
      router: { type: "hash" },
      compilerOptions: { runes: true }
    }),
    licenses ? bundledPackages(licenses) : []
  ],
  // the svelte client build, which component tests mount into jsdom
  resolve: process.env.VITEST ? { conditions: ["browser"] } : undefined,
  test: {
    include: ["src/**/*.test.ts", "scripts/**/*.test.ts"],
    environment: "jsdom",
    setupFiles: ["src/lib/testing/setup.ts"]
  },
  server: {
    // same origin as the dev UI, so the session cookie and the event stream work without CORS
    proxy: { "/api": { target: process.env.RESEEK_API ?? "http://localhost:5030" } }
  }
});

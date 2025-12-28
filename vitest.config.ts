import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: [
      "node_modules/@durable-streams/server-conformance-tests/dist/test-runner.js",
    ],
    exclude: [],
  },
})

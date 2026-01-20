/**
 * Server conformance tests for durable-streams-go.
 *
 * Run with: npm test (after starting the Go server)
 * Server URL is configured via CONFORMANCE_TEST_URL env var.
 *
 * NOTE: We use this local wrapper instead of `npx @durable-streams/server-conformance-tests --run`
 * because the package's CLI mode has a bug where vitest searches for test files in the current
 * working directory (using pattern `**\/*.{test,spec}.?(c|m)[jt]s?(x)`) rather than running
 * the package's internal test-runner.js. This wrapper imports the library and runs it within
 * a proper vitest test file that matches the expected pattern.
 */

import { runConformanceTests } from "@durable-streams/server-conformance-tests"

const baseUrl = process.env.CONFORMANCE_TEST_URL || "http://localhost:4437"

runConformanceTests({ baseUrl })

import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
	resolve: {
		// tests/integration/testing.test.ts imports the package like a consumer
		alias: [
			{
				find: "pgconductor-js/testing",
				replacement: fileURLToPath(new URL("src/testing.ts", import.meta.url)),
			},
			{
				find: "pgconductor-js",
				replacement: fileURLToPath(new URL("src/index.ts", import.meta.url)),
			},
		],
	},
	test: {
		// Integration test files share one Postgres; each test creates its own database
		maxWorkers: 2,
		// postgres.js writes queries in setImmediate, so faking it would stall every query
		fakeTimers: { toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] },
		projects: [
			{
				extends: true,
				test: { name: "unit", include: ["tests/unit/**/*.test.ts"] },
			},
			{
				extends: true,
				test: {
					name: "integration",
					include: ["tests/integration/**/*.test.ts"],
					globalSetup: ["tests/global-setup.ts"],
					testTimeout: 30_000,
					hookTimeout: 60_000,
				},
			},
		],
	},
});

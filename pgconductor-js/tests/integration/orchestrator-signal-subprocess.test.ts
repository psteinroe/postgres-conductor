import { afterAll, beforeAll, test, expect } from "bun:test";
import { spawn } from "child_process";
import { writeFileSync, unlinkSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import { TestDatabasePool } from "../fixtures/test-database";

let pool: TestDatabasePool;

beforeAll(async () => {
	pool = await TestDatabasePool.create();

	// Ensure tmp directory exists
	const tmpDir = join(__dirname, "../../tmp");
	if (!existsSync(tmpDir)) {
		mkdirSync(tmpDir, { recursive: true });
	}
}, 60000);

afterAll(async () => {
	await pool?.destroy();
});

/**
 * Helper to run a subprocess script and collect output.
 */
async function runSubprocess(scriptPath: string, signal: string): Promise<string> {
	const child = spawn("bun", ["run", scriptPath], {
		stdio: ["ignore", "pipe", "pipe"],
		env: process.env,
	});

	let stdout = "";
	let stderr = "";

	child.stdout?.on("data", (data) => {
		stdout += data.toString();
	});

	child.stderr?.on("data", (data) => {
		stderr += data.toString();
	});

	// Wait for startup
	await new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => {
			child.kill("SIGKILL");
			reject(
				new Error(
					`Timeout waiting for orchestrator to start.\nStdout: ${stdout}\nStderr: ${stderr}`,
				),
			);
		}, 10000);

		const checkInterval = setInterval(() => {
			if (stdout.includes("ORCHESTRATOR_STARTED")) {
				clearTimeout(timeout);
				clearInterval(checkInterval);
				resolve();
			}
		}, 100);
	});

	// Give it time to initialize
	await new Promise((r) => setTimeout(r, 500));

	// Send signal
	child.kill(signal as any);

	// Wait for process to exit
	await new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error(`Process did not exit within 5s after ${signal}`));
		}, 5000);

		child.on("exit", () => {
			clearTimeout(timeout);
			resolve();
		});
	});

	return stdout;
}

test("real process handles SIGTERM gracefully", async () => {
	const db = await pool.child();

	const scriptPath = join(__dirname, "../../tmp/signal-test-sigterm.ts");
	writeFileSync(
		scriptPath,
		`
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { defineTask } from "../src/task-definition";
import { TaskSchemas } from "../src/schemas";
import { z } from "zod";
import postgres from "postgres";

const sql = postgres("${db.url}");

const taskDefinition = defineTask({
	name: "idle-task",
	payload: z.object({}),
});

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([taskDefinition]),
	context: {},
});
await conductor.ensureInstalled();

const task = conductor.createTask(
	taskDefinition,
	{ invocable: true },
	async (event, _ctx) => {
		// No-op task
	},
);

const orch = Orchestrator.create({ conductor, tasks: [task] });

console.log("ORCHESTRATOR_STARTED");

await orch.run();

console.log("ORCHESTRATOR_STOPPED");

await sql.end();
`,
	);

	const stdout = await runSubprocess(scriptPath, "SIGTERM");

	// Verify shutdown happened gracefully
	expect(stdout).toContain("ORCHESTRATOR_STARTED");
	expect(stdout).toContain("ORCHESTRATOR_STOPPED");

	// Cleanup
	unlinkSync(scriptPath);
}, 30000);

test("real process handles SIGINT gracefully", async () => {
	const db = await pool.child();

	const scriptPath = join(__dirname, "../../tmp/signal-test-sigint.ts");
	writeFileSync(
		scriptPath,
		`
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { defineTask } from "../src/task-definition";
import { TaskSchemas } from "../src/schemas";
import { z } from "zod";
import postgres from "postgres";

const sql = postgres("${db.url}");

const taskDefinition = defineTask({
	name: "idle-task",
	payload: z.object({}),
});

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([taskDefinition]),
	context: {},
});
await conductor.ensureInstalled();

const task = conductor.createTask(
	taskDefinition,
	{ invocable: true },
	async (event, _ctx) => {
		// No-op task
	},
);

const orch = Orchestrator.create({ conductor, tasks: [task] });

console.log("ORCHESTRATOR_STARTED");

await orch.run();

console.log("ORCHESTRATOR_STOPPED");

await sql.end();
`,
	);

	const stdout = await runSubprocess(scriptPath, "SIGINT");

	// Verify shutdown happened gracefully
	expect(stdout).toContain("ORCHESTRATOR_STARTED");
	expect(stdout).toContain("ORCHESTRATOR_STOPPED");

	// Cleanup
	unlinkSync(scriptPath);
}, 30000);

test("duplicate signals are handled gracefully", async () => {
	const db = await pool.child();

	const scriptPath = join(__dirname, "../../tmp/signal-test-duplicate.ts");
	writeFileSync(
		scriptPath,
		`
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { defineTask } from "../src/task-definition";
import { TaskSchemas } from "../src/schemas";
import { z } from "zod";
import postgres from "postgres";

const sql = postgres("${db.url}");

const taskDefinition = defineTask({
	name: "idle-task",
	payload: z.object({}),
});

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([taskDefinition]),
	context: {},
});
await conductor.ensureInstalled();

const task = conductor.createTask(
	taskDefinition,
	{ invocable: true },
	async (event, _ctx) => {
		// No-op task
	},
);

const orch = Orchestrator.create({ conductor, tasks: [task] });

console.log("ORCHESTRATOR_STARTED");

await orch.run();

console.log("ORCHESTRATOR_STOPPED");

await sql.end();

console.log("SQL_ENDED");

// Give event loop time to drain before forcing exit
await new Promise((r) => setTimeout(r, 100));
process.exit(0);
`,
	);

	const child = spawn("bun", ["run", scriptPath], {
		stdio: ["ignore", "pipe", "pipe"],
		env: process.env,
	});

	let stdout = "";
	child.stdout?.on("data", (data) => {
		stdout += data.toString();
	});

	// Wait for startup
	await new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error("Timeout waiting for orchestrator to start"));
		}, 10000);

		const checkInterval = setInterval(() => {
			if (stdout.includes("ORCHESTRATOR_STARTED")) {
				clearTimeout(timeout);
				clearInterval(checkInterval);
				resolve();
			}
		}, 100);
	});

	await new Promise((r) => setTimeout(r, 500));

	// Send first signal
	child.kill("SIGTERM");

	// Immediately send second signal (should be ignored due to isShuttingDown)
	await new Promise((r) => setTimeout(r, 50));
	child.kill("SIGINT");

	// Wait for process to exit
	await new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => {
			child.kill("SIGKILL");
			reject(
				new Error(`Process did not exit within 10s after duplicate signals.\nStdout: ${stdout}`),
			);
		}, 10000);

		child.on("exit", () => {
			clearTimeout(timeout);
			resolve();
		});
	});

	// Should still shutdown gracefully
	expect(stdout).toContain("ORCHESTRATOR_STOPPED");

	// Cleanup
	unlinkSync(scriptPath);
}, 30000);

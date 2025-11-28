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

test("long-running task with checkpoint exits quickly on signal", async () => {
	const db = await pool.child();

	const scriptPath = join(__dirname, "../../tmp/signal-test-checkpoint.ts");
	writeFileSync(
		scriptPath,
		`
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { defineTask } from "../src/task-definition";
import { TaskSchemas } from "../src/schemas";
import { z } from "zod";
import postgres from "postgres";

const sql = postgres("${db.url}", { max: 5 });

const taskDefinition = defineTask({
	name: "infinite-loop-task",
	payload: z.object({}),
});

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([taskDefinition]),
	context: {},
});
await conductor.ensureInstalled();

let iterationCount = 0;

const task = conductor.createTask(
	taskDefinition,
	{ invocable: true },
	async (event, ctx) => {
		if (event.event === "pgconductor.invoke") {
			console.log("TASK_STARTED");

			// Infinite loop that checks abort via checkpoint
			while (true) {
				iterationCount++;
				console.log("ITERATION_" + iterationCount);

				// This will throw if aborted
				await ctx.checkpoint();

				// Simulate work
				await new Promise((r) => setTimeout(r, 1000));
			}
		}
	},
);

const orch = Orchestrator.create({ conductor, tasks: [task] });

console.log("ORCHESTRATOR_STARTED");

// Start orchestrator and invoke task
await orch.start();

// Invoke the infinite loop task
await conductor.invoke(taskDefinition, {});

// Let orchestrator run (will be stopped by signal)
await orch.stopped;

console.log("ORCHESTRATOR_STOPPED");

await sql.end();

// Give event loop time to drain
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

	// Wait for task to start running
	await new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error("Timeout waiting for task to start"));
		}, 10000);

		const checkInterval = setInterval(() => {
			if (stdout.includes("TASK_STARTED")) {
				clearTimeout(timeout);
				clearInterval(checkInterval);
				resolve();
			}
		}, 100);
	});

	// Wait for at least one iteration to complete
	await new Promise<void>((resolve) => {
		const checkInterval = setInterval(() => {
			if (stdout.includes("ITERATION_1")) {
				clearInterval(checkInterval);
				resolve();
			}
		}, 100);
	});

	// Record when we send the signal
	const signalTime = Date.now();

	// Send SIGTERM - task should abort at next checkpoint
	child.kill("SIGTERM");

	// Wait for process to exit
	await new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error(`Process did not exit within 3s after SIGTERM.\nStdout: ${stdout}`));
		}, 3000);

		child.on("exit", () => {
			clearTimeout(timeout);
			resolve();
		});
	});

	const exitTime = Date.now();
	const shutdownDuration = exitTime - signalTime;

	// Verify shutdown happened quickly (within ~2s - 1 iteration + overhead)
	expect(shutdownDuration).toBeLessThan(2500);

	// Verify the task started and ran at least one iteration
	expect(stdout).toContain("TASK_STARTED");
	expect(stdout).toContain("ITERATION_1");
	expect(stdout).toContain("ORCHESTRATOR_STOPPED");

	// Verify execution was released in database
	const executions = await db.sql`
		SELECT * FROM pgconductor._private_executions
		WHERE task_key = 'infinite-loop-task'
	`;

	// Execution should exist but not be locked
	expect(executions.length).toBeGreaterThan(0);
	const execution = executions[0];
	expect(execution?.locked_by).toBe(null);

	// Cleanup
	unlinkSync(scriptPath);
}, 30000);

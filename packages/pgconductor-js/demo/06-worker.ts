import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { TaskSchemas } from "../src/schemas";
import { fastTask, slowTask, normalTask, defaultQueueTask } from "./schemas";

const sql = postgres(
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres",
);

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([fastTask, slowTask, normalTask, defaultQueueTask]),
	context: {},
});

const fast = conductor.createTask(
	{ name: "fast-task", queue: "fast" },
	{ invocable: true },
	async (event, ctx) => {
		const { id } = event.payload;
		ctx.logger.info(`[FAST] Processing task ${id} (100ms)`);
		await new Promise((resolve) => setTimeout(resolve, 100));
		ctx.logger.info(`[FAST] ✓ Completed task ${id}`);
	},
);

const slow = conductor.createTask(
	{ name: "slow-task", queue: "slow" },
	{ invocable: true },
	async (event, ctx) => {
		const { id } = event.payload;
		ctx.logger.info(`[SLOW] Processing task ${id} (2000ms)`);
		await new Promise((resolve) => setTimeout(resolve, 2000));
		ctx.logger.info(`[SLOW] ✓ Completed task ${id}`);
	},
);

const normal = conductor.createTask(
	{ name: "normal-task", queue: "normal" },
	{ invocable: true },
	async (event, ctx) => {
		const { id } = event.payload;
		ctx.logger.info(`[NORMAL] Processing task ${id} (500ms)`);
		await new Promise((resolve) => setTimeout(resolve, 500));
		ctx.logger.info(`[NORMAL] ✓ Completed task ${id}`);
	},
);

const defaultQueue = conductor.createTask(
	{ name: "default-queue-task" },
	{ invocable: true },
	async (event, ctx) => {
		const { id } = event.payload;
		ctx.logger.info(`[DEFAULT] Processing task ${id} (300ms)`);
		await new Promise((resolve) => setTimeout(resolve, 300));
		ctx.logger.info(`[DEFAULT] ✓ Completed task ${id}`);
	},
);

const fastWorker = conductor.createWorker({
	queue: "fast",
	tasks: [fast],
	config: { pollIntervalMs: 50, concurrency: 5 },
});

const slowWorker = conductor.createWorker({
	queue: "slow",
	tasks: [slow],
	config: { pollIntervalMs: 100, concurrency: 1 },
});

const normalWorker = conductor.createWorker({
	queue: "normal",
	tasks: [normal],
	config: { pollIntervalMs: 50, concurrency: 3 },
});

const orchestrator = Orchestrator.create({
	conductor,
	tasks: [defaultQueue],
	workers: [fastWorker, slowWorker, normalWorker],
	defaultWorker: { pollIntervalMs: 50, concurrency: 2 },
});

await orchestrator.start();

console.log("\n✓ Multi-worker orchestrator running:");
console.log("  - Default worker: 2 concurrent tasks (300ms) - uses 'default' queue");
console.log("  - Fast queue: 5 concurrent tasks (100ms) - custom worker");
console.log("  - Slow queue: 1 concurrent task (2000ms) - custom worker");
console.log("  - Normal queue: 3 concurrent tasks (500ms) - custom worker");
console.log("\nPress Ctrl+C to stop.\n");

await orchestrator.stopped;
await sql.end();

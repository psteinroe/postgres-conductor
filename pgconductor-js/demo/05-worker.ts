import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { TaskSchemas } from "../src/schemas";
import { schedulerTask, dynamicTask } from "./schemas";

const sql = postgres(
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres",
);

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([schedulerTask, dynamicTask]),
	context: {},
});

let executionCount = 0;

const dynamic = conductor.createTask(
	{ name: "dynamic-task" },
	{ invocable: true },
	async (_event, ctx) => {
		executionCount++;
		const timestamp = new Date().toISOString();
		ctx.logger.info(`[${executionCount}] Dynamic task executed at ${timestamp}`);
	},
);

const scheduler = conductor.createTask(
	{ name: "scheduler" },
	{ invocable: true },
	async (event, ctx) => {
		if (event.event === "pgconductor.invoke") {
			const { action } = event.payload;

			if (action === "schedule") {
				await ctx.schedule({ name: "dynamic-task" }, "every-3-seconds", { cron: "*/3 * * * * *" });
				ctx.logger.info("✓ Scheduled dynamic-task to run every 3 seconds");
			} else if (action === "unschedule") {
				await ctx.unschedule({ name: "dynamic-task" }, "every-3-seconds");
				ctx.logger.info("✓ Unscheduled dynamic-task");
			}
		}
	},
);

const orchestrator = Orchestrator.create({
	conductor,
	tasks: [dynamic, scheduler],
	defaultWorker: { pollIntervalMs: 50 },
});

await orchestrator.start();

console.log("\n✓ Worker running. Invoke scheduler task to control dynamic scheduling.");
console.log("  - just demo invoke-05 schedule");
console.log("  - just demo invoke-05 unschedule");
console.log("\nPress Ctrl+C to stop.\n");

await orchestrator.stopped;
await sql.end();

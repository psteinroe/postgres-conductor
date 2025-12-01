import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { TaskSchemas } from "../src/schemas";
import { reportTask } from "./schemas";

const sql = postgres(process.env.DATABASE_URL || "postgres://localhost:5432/pgconductor");

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([reportTask]),
	context: {},
});

let executionCount = 0;

const dailyReport = conductor.createTask(
	{ name: "daily-report" },
	[{ invocable: true }, { cron: "*/3 * * * * *", name: "every-3-seconds" }],
	async (event, ctx) => {
		executionCount++;
		const timestamp = new Date().toISOString();

		if (event.event === "every-3-seconds") {
			ctx.logger.info(`[${executionCount}] Cron: Generating report at ${timestamp}`);
		} else if (event.event === "pgconductor.invoke") {
			ctx.logger.info(`[${executionCount}] Manual: Generating report at ${timestamp}`);
		}

		ctx.logger.info(`  Report generated successfully`);
	},
);

const orchestrator = Orchestrator.create({
	conductor,
	tasks: [dailyReport],
	defaultWorker: { pollIntervalMs: 50 },
});

await orchestrator.start();

console.log("\n✓ Worker running with cron schedule (every 3 seconds). Press Ctrl+C to stop.\n");

await orchestrator.stopped;
await sql.end();

import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { TaskSchemas } from "../src/schemas";
import { parentTask, childTask } from "./schemas";

const sql = postgres(
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres",
);

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([parentTask, childTask]),
	context: {},
});

const childProcessor = conductor.createTask(
	{ name: "child-processor" },
	{ invocable: true },
	async (event, ctx) => {
		const { input } = event.payload;
		ctx.logger.info(`  Child: Processing ${input}...`);

		const result = input * 2;
		ctx.logger.info(`  Child: Computed ${result}`);

		return { output: result };
	},
);

const parentWorkflow = conductor.createTask(
	{ name: "parent-workflow" },
	{ invocable: true },
	async (event, ctx) => {
		const { value } = event.payload;
		ctx.logger.info(`Parent: Starting workflow with ${value}`);

		const childResult = await ctx.invoke(
			"invoke-child-processor",
			{ name: "child-processor" },
			{ input: value },
		);

		ctx.logger.info(`Parent: Child returned ${childResult.output}`);

		const finalResult = childResult.output + 10;
		ctx.logger.info(`Parent: Final result is ${finalResult}`);

		return { result: finalResult };
	},
);

const orchestrator = Orchestrator.create({
	conductor,
	tasks: [parentWorkflow, childProcessor],
	defaultWorker: { pollIntervalMs: 50 },
});

await orchestrator.start();

console.log("\n✓ Worker running. Press Ctrl+C to stop.\n");

await orchestrator.stopped;
await sql.end();

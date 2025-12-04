import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { TaskSchemas } from "../src/schemas";
import { processTask } from "./schemas";

const sql = postgres(
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres",
);

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([processTask]),
	context: {},
});

const processData = conductor.createTask(
	{ name: "process-data" },
	{ invocable: true },
	async (event, ctx) => {
		const { items } = event.payload;

		const validated = await ctx.step("validate-items", async () => {
			ctx.logger.info(`Validating ${items.length} items...`);
			return items.filter((item) => item.length > 0);
		});

		const transformed = await ctx.step("transform-to-uppercase", async () => {
			ctx.logger.info(`Transforming ${validated.length} items...`);
			return validated.map((item) => item.toUpperCase());
		});

		await ctx.sleep("pause-before-save", 1000);

		const saved = await ctx.step("save-to-database", async () => {
			ctx.logger.info(`Saving ${transformed.length} items...`);
			return transformed.length;
		});

		ctx.logger.info(`✓ Processed ${saved} items`);
		return { processed: saved };
	},
);

const orchestrator = Orchestrator.create({
	conductor,
	tasks: [processData],
	defaultWorker: { pollIntervalMs: 50 },
});

await orchestrator.start();

console.log("\n✓ Worker running. Press Ctrl+C to stop.\n");

await orchestrator.stopped;
await sql.end();

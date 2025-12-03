import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { TaskSchemas } from "../src/schemas";
import { greetTask } from "./schemas";
import context from "./context";

const sql = postgres(
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres",
);

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([greetTask]),
	context,
});

const greet = conductor.createTask({ name: "greet" }, { invocable: true }, async (event, ctx) => {
	if (event.event === "pgconductor.invoke") {
		const { name } = event.payload;
		ctx.sayHi(name);
		ctx.logger.info("Task completed successfully");
	}
});

const orchestrator = Orchestrator.create({
	conductor,
	tasks: [greet],
});

await orchestrator.start();

console.log("\n✓ Worker running. Press Ctrl+C to stop.\n");

await orchestrator.stopped;
await sql.end();

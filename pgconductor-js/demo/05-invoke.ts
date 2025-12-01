import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { TaskSchemas } from "../src/schemas";
import { schedulerTask, dynamicTask } from "./schemas";

const sql = postgres(process.env.DATABASE_URL || "postgres://localhost:5432/pgconductor");

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([schedulerTask, dynamicTask]),
	context: {},
});

const action = (process.argv[2] || "schedule") as "schedule" | "unschedule";

if (action !== "schedule" && action !== "unschedule") {
	console.error("Usage: bun run 05-invoke.ts <schedule|unschedule>");
	process.exit(1);
}

await conductor.invoke({ name: "scheduler" }, { action });

if (action === "schedule") {
	console.log("✓ Scheduled dynamic-task to run every 3 seconds");
} else {
	console.log("✓ Unscheduled dynamic-task");
}

await sql.end();

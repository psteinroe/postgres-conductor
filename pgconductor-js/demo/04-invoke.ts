import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { TaskSchemas } from "../src/schemas";
import { reportTask } from "./schemas";

const sql = postgres(process.env.DATABASE_URL || "postgres://localhost:5432/pgconductor");

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([reportTask]),
	context: {},
});

await conductor.invoke({ name: "daily-report" }, {});

console.log("✓ Manual report task invoked");

await sql.end();

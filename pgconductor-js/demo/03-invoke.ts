import postgres from "postgres";
import { Conductor } from "../src/conductor";
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

const value = parseInt(process.argv[2] || "5", 10);

await conductor.invoke({ name: "parent-workflow" }, { value });

console.log(`✓ Parent workflow invoked with value=${value}`);

await sql.end();

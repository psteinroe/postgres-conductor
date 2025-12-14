import postgres from "postgres";
import { Conductor } from "../src/conductor";
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

const items =
	process.argv.slice(2).length > 0 ? process.argv.slice(2) : ["apple", "banana", "cherry"];

await conductor.invoke({ name: "process-data" }, { items });

console.log(`✓ Task invoked with ${items.length} items: ${items.join(", ")}`);

await sql.end();

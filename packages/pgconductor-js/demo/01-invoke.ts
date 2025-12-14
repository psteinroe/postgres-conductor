import postgres from "postgres";
import { Conductor } from "../src/conductor";
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

const name = process.argv[2] || "World";
await conductor.invoke({ name: "greet" }, { name });

console.log(`✓ Task invoked with name="${name}"`);

await sql.end();

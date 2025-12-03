import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { TaskSchemas } from "../src/schemas";
import { fastTask, slowTask, normalTask, defaultQueueTask } from "./schemas";

const sql = postgres(
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres",
);

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([fastTask, slowTask, normalTask, defaultQueueTask]),
	context: {},
});

const count = parseInt(process.argv[2] || "10", 10);

console.log(`Enqueueing ${count} tasks to each queue (${count * 4} total)...\n`);

for (let i = 1; i <= count; i++) {
	await conductor.invoke({ name: "default-queue-task" }, { id: i });
	await conductor.invoke({ name: "fast-task", queue: "fast" }, { id: i });
	await conductor.invoke({ name: "slow-task", queue: "slow" }, { id: i });
	await conductor.invoke({ name: "normal-task", queue: "normal" }, { id: i });
}

console.log(`✓ Enqueued:`);
console.log(`  - ${count} default queue tasks (300ms each) - handled by default worker`);
console.log(`  - ${count} fast tasks (100ms each) - handled by custom fast worker`);
console.log(`  - ${count} slow tasks (2000ms each) - handled by custom slow worker`);
console.log(`  - ${count} normal tasks (500ms each) - handled by custom normal worker`);
console.log(`\nWatch the orchestrator manage both default and custom workers!`);

await sql.end();

import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { TaskSchemas, EventSchemas } from "../src/schemas";
import { eventListenerTask, eventEmitterTask, userCreatedEvent } from "./schemas";

const sql = postgres(
	process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/postgres",
);

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([eventListenerTask, eventEmitterTask]),
	// events: EventSchemas.fromSchema([userCreatedEvent]),
	context: {},
});

await conductor.invoke({ name: "event-listener" }, {});

console.log("✓ Listener task started. It will wait for user.created event (60s timeout)");

await sql.end();

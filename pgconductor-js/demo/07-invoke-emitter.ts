import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { TaskSchemas, EventSchemas } from "../src/schemas";
import { eventListenerTask, eventEmitterTask, userCreatedEvent } from "./schemas";

const sql = postgres(process.env.DATABASE_URL || "postgres://localhost:5432/pgconductor");

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([eventListenerTask, eventEmitterTask]),
	events: EventSchemas.fromSchema([userCreatedEvent]),
	context: {},
});

const message = process.argv[2] || "Hello Events";

await conductor.invoke({ name: "event-emitter" }, { message });

console.log(`✓ Emitter task invoked. Event will be sent with message="${message}"`);

await sql.end();

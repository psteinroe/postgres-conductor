import postgres from "postgres";
import { Conductor } from "../src/conductor";
import { Orchestrator } from "../src/orchestrator";
import { TaskSchemas, EventSchemas } from "../src/schemas";
import { eventListenerTask, eventEmitterTask, userCreatedEvent } from "./schemas";

const sql = postgres(process.env.DATABASE_URL || "postgres://localhost:5432/pgconductor");

const conductor = Conductor.create({
	sql,
	tasks: TaskSchemas.fromSchema([eventListenerTask, eventEmitterTask]),
	events: EventSchemas.fromSchema([userCreatedEvent]),
	context: {},
});

const listener = conductor.createTask(
	{ name: "event-listener" },
	{ invocable: true },
	async (_event, ctx) => {
		ctx.logger.info("Listener: Waiting for user.created event...");

		const eventData = await ctx.waitForEvent("wait-for-user-created", {
			event: "user.created",
			timeout: 60000,
		});

		ctx.logger.info(
			`Listener: Received event! User: ${eventData.userId}, Email: ${eventData.email}`,
		);

		return { eventData };
	},
);

const emitter = conductor.createTask(
	{ name: "event-emitter" },
	{ invocable: true },
	async (event, ctx) => {
		if (event.event === "pgconductor.invoke") {
			const { message } = event.payload;
			ctx.logger.info(`Emitter: Sending message "${message}"`);

			await ctx.emit(
				{ event: "user.created" },
				{
					userId: `user_${Date.now()}`,
					email: "demo@example.com",
				},
			);

			ctx.logger.info("Emitter: Event sent!");
		}
	},
);

const orchestrator = Orchestrator.create({
	conductor,
	tasks: [listener, emitter],
	defaultWorker: { pollIntervalMs: 50 },
});

await orchestrator.start();

console.log("\n✓ Worker running. Press Ctrl+C to stop.\n");

await orchestrator.stopped;
await sql.end();

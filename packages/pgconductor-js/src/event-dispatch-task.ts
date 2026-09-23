import type { DatabaseClient, Payload } from "./database-client";
import type { BatchTaskContext, BatchTaskEvent } from "./task-context";
import { Task } from "./task";

export const EVENT_DISPATCH_QUEUE = "pgconductor.internal";
export const EVENT_DISPATCH_TASK = "pgconductor.event-dispatch";
const EVENT_DISPATCH_BATCH_SIZE = 10;

type EventDispatchPayload = {
	eventKey: string;
	payload: Payload;
};

type EventDispatchContext = BatchTaskContext & { db: Pick<DatabaseClient, "dispatchCustomEvents"> };
type EventDispatchEvent = { name: "pgconductor.invoke"; payload: EventDispatchPayload };

export const eventDispatchTask = new Task<
	typeof EVENT_DISPATCH_TASK,
	typeof EVENT_DISPATCH_QUEUE,
	EventDispatchPayload,
	void,
	EventDispatchContext,
	BatchTaskEvent<EventDispatchEvent>[]
>(
	{
		name: EVENT_DISPATCH_TASK,
		queue: EVENT_DISPATCH_QUEUE,
		maxAttempts: 3,
		removeOnComplete: true,
		batch: { size: EVENT_DISPATCH_BATCH_SIZE, timeoutMs: 10 },
	},
	{ invocable: true },
	async (events, context) => {
		const first = events[0];
		if (!first) return;

		await context.db.dispatchCustomEvents(
			{
				eventIds: events.map((event) => event.execution.id),
				orchestratorId: first.execution.locked_by,
			},
			{ signal: context.signal },
		);
	},
);

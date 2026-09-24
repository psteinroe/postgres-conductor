import type { DatabaseClient, Payload } from "./database-client";
import type { BatchTaskContext, BatchTaskEvent } from "./task-context";
import { Task } from "./task";
import * as assert from "./lib/assert";

export const EVENT_DISPATCH_QUEUE = "pgconductor.internal";
export const EVENT_DISPATCH_TASK = "pgconductor.event-dispatch";
const EVENT_DISPATCH_BATCH_SIZE = 10;

type EventDispatchPayload = {
	eventKey: string;
	payload: Payload;
};

type EventDispatchEvent = { name: "pgconductor.invoke"; payload: EventDispatchPayload };
type EventDispatchDatabase = Pick<DatabaseClient, "dispatchCustomEvents">;

export function createEventDispatchTask(db: EventDispatchDatabase) {
	return Task.create<
		typeof EVENT_DISPATCH_TASK,
		typeof EVENT_DISPATCH_QUEUE,
		EventDispatchPayload,
		void,
		BatchTaskContext,
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
			assert.ok(events.length > 0, "Event dispatch batch must not be empty");
			const first = events[0]!;
			const orchestratorId = first.execution.locked_by;
			assert.ok(
				events.every((event) => event.execution.locked_by === orchestratorId),
				"Event dispatch batch must have one claim owner",
			);

			await db.dispatchCustomEvents(
				{
					eventIds: events.map((event) => event.execution.id),
					orchestratorId,
				},
				{ signal: context.signal },
			);
		},
	);
}

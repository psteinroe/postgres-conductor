import { expect, mock, test } from "bun:test";
import {
	eventDispatchTask,
	EVENT_DISPATCH_QUEUE,
	EVENT_DISPATCH_TASK,
} from "../../src/event-dispatch-task";
import { DefaultLogger } from "../../src/lib/logger";
import { BatchTaskContext, createTaskSignal } from "../../src/task-context";

test("defines the internal event dispatch task", () => {
	expect({
		name: eventDispatchTask.name,
		queue: eventDispatchTask.queue,
		maxAttempts: eventDispatchTask.maxAttempts,
		removeOnComplete: eventDispatchTask.removeOnComplete,
		batch: eventDispatchTask.batch,
	}).toEqual({
		name: EVENT_DISPATCH_TASK,
		queue: EVENT_DISPATCH_QUEUE,
		maxAttempts: 3,
		removeOnComplete: true,
		batch: { size: 10, timeoutMs: 10 },
	});
});

test("dispatches from ordinary batch event execution metadata", async () => {
	const id = "00000000-0000-0000-0000-000000000001";
	const owner = "00000000-0000-0000-0000-000000000002";
	const dispatchCustomEvents = mock(async () => [id]);
	const db = { dispatchCustomEvents };
	const context = BatchTaskContext.create(
		createTaskSignal(new AbortController().signal),
		new DefaultLogger(),
		{ db },
	);

	const result = await eventDispatchTask.execute(
		[
			{
				name: "pgconductor.invoke",
				payload: { eventKey: "order.created", payload: { orderId: "order-1" } },
				execution: {
					id,
					queue: EVENT_DISPATCH_QUEUE,
					task_key: EVENT_DISPATCH_TASK,
					locked_by: owner,
				},
			},
		],
		context,
	);

	expect(dispatchCustomEvents).toHaveBeenCalledWith(
		{ eventIds: [id], orchestratorId: owner },
		{ signal: context.signal },
	);
	expect(result).toBeUndefined();
});

test("leaves stale claim settlement to the ordinary worker", async () => {
	const db = { dispatchCustomEvents: mock(async () => []) };
	const context = BatchTaskContext.create(
		createTaskSignal(new AbortController().signal),
		new DefaultLogger(),
		{ db },
	);
	const result = await eventDispatchTask.execute(
		[
			{
				name: "pgconductor.invoke",
				payload: { eventKey: "order.created", payload: {} },
				execution: {
					id: "00000000-0000-0000-0000-000000000001",
					queue: EVENT_DISPATCH_QUEUE,
					task_key: EVENT_DISPATCH_TASK,
					locked_by: "00000000-0000-0000-0000-000000000002",
				},
			},
		],
		context,
	);

	expect(result).toBeUndefined();
});

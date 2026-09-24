import { expect, mock, test } from "bun:test";
import {
	createEventDispatchTask,
	EVENT_DISPATCH_QUEUE,
	EVENT_DISPATCH_TASK,
} from "../../src/event-dispatch-task";
import { DefaultLogger } from "../../src/lib/logger";
import { BatchTaskContext, createTaskSignal } from "../../src/task-context";

function context() {
	return new BatchTaskContext(createTaskSignal(new AbortController().signal), new DefaultLogger());
}

test("defines the internal event dispatch task", () => {
	const task = createEventDispatchTask({ dispatchCustomEvents: mock(async () => []) });

	expect({
		name: task.name,
		queue: task.queue,
		maxAttempts: task.maxAttempts,
		removeOnComplete: task.removeOnComplete,
		batch: task.batch,
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
	const task = createEventDispatchTask({ dispatchCustomEvents });
	const batchContext = context();

	const result = await task.execute(
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
		batchContext,
	);

	expect(dispatchCustomEvents).toHaveBeenCalledWith(
		{ eventIds: [id], orchestratorId: owner },
		{ signal: batchContext.signal },
	);
	expect(result).toBeUndefined();
});

test("leaves stale claim settlement to the ordinary worker", async () => {
	const dispatchCustomEvents = mock(async () => []);
	const task = createEventDispatchTask({ dispatchCustomEvents });
	const result = await task.execute(
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
		context(),
	);

	expect(result).toBeUndefined();
});

test("rejects empty and mixed-owner batches", async () => {
	const task = createEventDispatchTask({ dispatchCustomEvents: mock(async () => []) });
	const owner = "00000000-0000-0000-0000-000000000001";
	const event = (id: string, lockedBy: string) => ({
		name: "pgconductor.invoke" as const,
		payload: { eventKey: "order.created", payload: {} },
		execution: {
			id,
			queue: EVENT_DISPATCH_QUEUE,
			task_key: EVENT_DISPATCH_TASK,
			locked_by: lockedBy,
		},
	});

	await expect(task.execute([], context())).rejects.toThrow(
		"Event dispatch batch must not be empty",
	);
	await expect(
		task.execute(
			[
				event("00000000-0000-0000-0000-000000000002", owner),
				event("00000000-0000-0000-0000-000000000003", "00000000-0000-0000-0000-000000000004"),
			],
			context(),
		),
	).rejects.toThrow("Event dispatch batch must have one claim owner");
});

import { expect, mock, test } from "bun:test";
import type { DatabaseClient, Execution } from "../../src/database-client";
import {
	eventDispatchTask,
	EVENT_DISPATCH_QUEUE,
	EVENT_DISPATCH_TASK,
} from "../../src/event-dispatch-task";

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

test("dispatches claimed event executions through its task handler", async () => {
	const execution: Execution = {
		id: "00000000-0000-0000-0000-000000000001",
		task_key: EVENT_DISPATCH_TASK,
		queue: EVENT_DISPATCH_QUEUE,
		payload: { eventKey: "order.created", payload: { orderId: "order-1" } },
		waiting_on_execution_id: null,
		waiting_step_key: null,
		locked_by: "orchestrator-1",
		cancelled: false,
		last_error: null,
	};
	const dispatchCustomEvents = mock(async () => [execution.id]);
	const db = { dispatchCustomEvents } as unknown as DatabaseClient;
	const signal = new AbortController().signal;

	const results = await eventDispatchTask.execute([execution], {
		db,
		orchestratorId: "orchestrator-1",
		signal,
	});

	expect(dispatchCustomEvents).toHaveBeenCalledWith(
		{
			eventIds: [execution.id],
			orchestratorId: "orchestrator-1",
		},
		{ signal },
	);
	expect(results).toEqual([
		{
			execution_id: execution.id,
			orchestrator_id: "orchestrator-1",
			queue: EVENT_DISPATCH_QUEUE,
			task_key: EVENT_DISPATCH_TASK,
			status: "completed",
			result: undefined,
		},
	]);
});

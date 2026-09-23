import { expect, test } from "bun:test";
import {
	createEventDispatchTask,
	EVENT_DISPATCH_QUEUE,
	EVENT_DISPATCH_TASK,
} from "../../src/event-dispatch-task";

test("defines the internal event dispatch task", () => {
	const task = createEventDispatchTask();

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

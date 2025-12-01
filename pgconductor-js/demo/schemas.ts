import { z } from "zod";
import { defineTask } from "../src/task-definition";
import { defineEvent } from "../src/event-definition";

// Task Definitions
export const greetTask = defineTask({
	name: "greet",
	payload: z.object({ name: z.string() }),
});

export const processTask = defineTask({
	name: "process-data",
	payload: z.object({ items: z.array(z.string()) }),
	returns: z.object({ processed: z.number() }),
});

export const parentTask = defineTask({
	name: "parent-workflow",
	payload: z.object({ value: z.number() }),
	returns: z.object({ result: z.number() }),
});

export const childTask = defineTask({
	name: "child-processor",
	payload: z.object({ input: z.number() }),
	returns: z.object({ output: z.number() }),
});

export const eventListenerTask = defineTask({
	name: "event-listener",
	payload: z.object({}),
	returns: z.object({ eventData: z.any() }),
});

export const eventEmitterTask = defineTask({
	name: "event-emitter",
	payload: z.object({ message: z.string() }),
});

export const reportTask = defineTask({
	name: "daily-report",
});

export const schedulerTask = defineTask({
	name: "scheduler",
	payload: z.object({ action: z.enum(["schedule", "unschedule"]) }),
});

export const dynamicTask = defineTask({
	name: "dynamic-task",
});

export const fastTask = defineTask({
	name: "fast-task",
	queue: "fast",
	payload: z.object({ id: z.number() }),
});

export const slowTask = defineTask({
	name: "slow-task",
	queue: "slow",
	payload: z.object({ id: z.number() }),
});

export const normalTask = defineTask({
	name: "normal-task",
	queue: "normal",
	payload: z.object({ id: z.number() }),
});

export const defaultQueueTask = defineTask({
	name: "default-queue-task",
	payload: z.object({ id: z.number() }),
});

// Event Definitions
export const userCreatedEvent = defineEvent({
	name: "user.created",
	payload: z.object({
		userId: z.string(),
		email: z.string(),
	}),
});

export const dataProcessedEvent = defineEvent({
	name: "data.processed",
	payload: z.object({
		count: z.number(),
		timestamp: z.string(),
	}),
});

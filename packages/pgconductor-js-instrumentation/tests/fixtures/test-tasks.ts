import { z } from "zod";

/**
 * Simple task definitions for testing.
 */
export const testTaskDefinitions = [
	{
		name: "simple-task",
		queue: "default",
		payload: z.object({ value: z.number() }),
		returns: z.number(),
	},
	{
		name: "step-task",
		queue: "default",
		payload: z.object({ input: z.string() }),
		returns: z.string(),
	},
	{
		name: "sleep-task",
		queue: "default",
		payload: z.object({}),
		returns: z.void(),
	},
	{
		name: "invoke-child-task",
		queue: "default",
		payload: z.object({ childValue: z.number() }),
		returns: z.number(),
	},
	{
		name: "child-task",
		queue: "default",
		payload: z.object({ value: z.number() }),
		returns: z.number(),
	},
	{
		name: "error-task",
		queue: "default",
		payload: z.object({}),
		returns: z.void(),
	},
	{
		name: "retry-task",
		queue: "default",
		payload: z.object({ failCount: z.number() }),
		returns: z.string(),
	},
] as const;

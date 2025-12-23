/**
 * Task definitions for performance testing
 */
import { defineTask } from "../../src/task-definition";
import type { Conductor } from "../../src";
import { z } from "zod";

// Define all task schemas
export const perfTaskDefinitions = [
	defineTask({
		name: "noop",
		payload: z.object({ id: z.number() }),
	}),
	defineTask({
		name: "cpu",
		payload: z.object({ id: z.number() }),
	}),
	defineTask({
		name: "io",
		payload: z.object({ id: z.number() }),
	}),
	defineTask({
		name: "steps",
		payload: z.object({ id: z.number() }),
	}),
	defineTask({
		name: "task-a",
		payload: z.object({ id: z.number() }),
	}),
	defineTask({
		name: "task-b",
		payload: z.object({ id: z.number() }),
	}),
] as const;

// Task implementations
export function createNoopTask<C extends Conductor<any, any, any>>(
	conductor: C,
	concurrency?: number,
) {
	return (conductor as any).createTask(
		{ name: "noop", ...(concurrency != null && { concurrency }) } as const,
		{ invocable: true } as const,
		async () => {
			// Minimal work - just return
			return { processed: true };
		},
	);
}

export function createCpuTask<C extends Conductor<any, any, any>>(
	conductor: C,
	concurrency?: number,
) {
	return (conductor as any).createTask(
		{ name: "cpu", ...(concurrency != null && { concurrency }) } as const,
		{ invocable: true } as const,
		async () => {
			// Compute fibonacci(20) to simulate CPU work
			function fib(n: number): number {
				if (n <= 1) return n;
				return fib(n - 1) + fib(n - 2);
			}

			const result = fib(20);
			return { result };
		},
	);
}

export function createIoTask<C extends Conductor<any, any, any>>(
	conductor: C,
	concurrency?: number,
) {
	return (conductor as any).createTask(
		{ name: "io", ...(concurrency != null && { concurrency }) } as const,
		{ invocable: true } as const,
		async () => {
			// Simulate I/O delay
			await new Promise((resolve) => setTimeout(resolve, 10));
			return { processed: true };
		},
	);
}

export function createStepsTask<C extends Conductor<any, any, any>>(
	conductor: C,
	concurrency?: number,
) {
	return (conductor as any).createTask(
		{ name: "steps", ...(concurrency != null && { concurrency }) } as const,
		{ invocable: true } as const,
		async (event: any, ctx: any) => {
			// Execute 10 steps
			const results = [];
			for (let i = 0; i < 10; i++) {
				const result = await ctx.step(`step-${i}`, async () => {
					return { step: i, value: i * 2 };
				});
				results.push(result);
			}
			return { results };
		},
	);
}

export function createTaskA<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
	concurrency?: number,
) {
	return (conductor as any).createTask(
		{ name: "task-a", ...(concurrency != null && { concurrency }) } as const,
		{ invocable: true } as const,
		async () => {
			// Minimal work - same as noop
			return { processed: true };
		},
	);
}

export function createTaskB<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
	concurrency?: number,
) {
	return (conductor as any).createTask(
		{ name: "task-b", ...(concurrency != null && { concurrency }) } as const,
		{ invocable: true } as const,
		async () => {
			// Minimal work - same as noop
			return { processed: true };
		},
	);
}

// Factory function to create task based on type
export function createTaskByType<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
	type: string,
	concurrency?: number,
) {
	switch (type) {
		case "noop":
			return createNoopTask(conductor, concurrency);
		case "cpu":
			return createCpuTask(conductor, concurrency);
		case "io":
			return createIoTask(conductor, concurrency);
		case "steps":
			return createStepsTask(conductor, concurrency);
		case "task-a":
			return createTaskA(conductor, concurrency);
		case "task-b":
			return createTaskB(conductor, concurrency);
		default:
			throw new Error(`Unknown task type: ${type}`);
	}
}

/**
 * Task definitions for performance testing
 */
import { defineTask } from "../../src/task-definition";
import type { Conductor } from "../../src";
import type { TaskSchemas } from "../../src/schemas";
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
] as const;

// Task implementations
export function createNoopTask<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
) {
	return (conductor as any).createTask(
		{ name: "noop" } as const,
		{ invocable: true } as const,
		async (event: any) => {
			// Minimal work - just return
			return { processed: true };
		},
	);
}

export function createCpuTask<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
) {
	return (conductor as any).createTask(
		{ name: "cpu" } as const,
		{ invocable: true } as const,
		async (event: any) => {
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

export function createIoTask<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
) {
	return (conductor as any).createTask(
		{ name: "io" } as const,
		{ invocable: true } as const,
		async (event: any) => {
			// Simulate I/O delay
			await new Promise((resolve) => setTimeout(resolve, 10));
			return { processed: true };
		},
	);
}

export function createStepsTask<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
) {
	return (conductor as any).createTask(
		{ name: "steps" } as const,
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

// Factory function to create task based on type
export function createTaskByType<C extends Conductor<any, any, any, any, any, any, any>>(
	conductor: C,
	type: string,
) {
	switch (type) {
		case "noop":
			return createNoopTask(conductor);
		case "cpu":
			return createCpuTask(conductor);
		case "io":
			return createIoTask(conductor);
		case "steps":
			return createStepsTask(conductor);
		default:
			throw new Error(`Unknown task type: ${type}`);
	}
}

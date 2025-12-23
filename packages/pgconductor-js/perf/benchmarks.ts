import { Orchestrator } from "../src/orchestrator";
import type { BenchmarkContext } from "./fixtures/benchmark-database";
import type { BenchmarkScenario } from "./lib/benchmark-runner";

function createTasks(ctx: BenchmarkContext) {
	const simple = ctx.conductor.createTask({ name: "simple" }, { invocable: true }, async () => ({
		result: "ok",
	}));

	const withSteps = ctx.conductor.createTask(
		{ name: "with-steps" },
		{ invocable: true },
		async (event, taskCtx) => {
			await taskCtx.step("step1", async () => "data");
			await taskCtx.step("step2", async () => "result");
		},
	);

	const work = ctx.conductor.createTask({ name: "work" }, { invocable: true }, async () => {
		await new Promise((resolve) => setTimeout(resolve, 10));
	});

	const workQ1 = ctx.conductor.createTask(
		{ name: "work-q1", queue: "q1" },
		{ invocable: true },
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 10));
		},
	);

	const workQ2 = ctx.conductor.createTask(
		{ name: "work-q2", queue: "q2" },
		{ invocable: true },
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 10));
		},
	);

	const workQ3 = ctx.conductor.createTask(
		{ name: "work-q3", queue: "q3" },
		{ invocable: true },
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 10));
		},
	);

	const workQ4 = ctx.conductor.createTask(
		{ name: "work-q4", queue: "q4" },
		{ invocable: true },
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 10));
		},
	);

	const unlimited = ctx.conductor.createTask(
		{ name: "unlimited" },
		{ invocable: true },
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 10));
		},
	);

	const limited = ctx.conductor.createTask(
		{ name: "limited", concurrency: 10 },
		{ invocable: true },
		async () => {
			await new Promise((resolve) => setTimeout(resolve, 10));
		},
	);

	return {
		simple,
		withSteps,
		work,
		workQ1,
		workQ2,
		workQ3,
		workQ4,
		unlimited,
		limited,
	};
}

export function createBenchmarks(ctx: BenchmarkContext): Record<string, BenchmarkScenario> {
	const tasks = createTasks(ctx);

	return {
		// === Throughput ===
		"throughput/simple-100": {
			orchestrator: Orchestrator.create({
				conductor: ctx.conductor,
				tasks: [tasks.simple],
			}),
			invoke: async () => {
				await Promise.all(
					Array.from({ length: 100 }, () => ctx.conductor.invoke({ name: "simple" }, {})),
				);
			},
		},

		"throughput/with-steps-100": {
			orchestrator: Orchestrator.create({
				conductor: ctx.conductor,
				tasks: [tasks.withSteps],
			}),
			invoke: async () => {
				await Promise.all(
					Array.from({ length: 100 }, () => ctx.conductor.invoke({ name: "with-steps" }, {})),
				);
			},
		},

		// === Scaling ===
		"scaling/1-queue-1000": {
			orchestrator: Orchestrator.create({
				conductor: ctx.conductor,
				tasks: [tasks.work],
				defaultWorker: { concurrency: 10 },
			}),
			invoke: async () => {
				await Promise.all(
					Array.from({ length: 1000 }, () => ctx.conductor.invoke({ name: "work" }, {})),
				);
			},
		},

		"scaling/4-queues-1000": {
			orchestrator: Orchestrator.create({
				conductor: ctx.conductor,
				workers: [
					ctx.conductor.createWorker({
						queue: "q1",
						tasks: [tasks.workQ1],
						config: { concurrency: 10 },
					}),
					ctx.conductor.createWorker({
						queue: "q2",
						tasks: [tasks.workQ2],
						config: { concurrency: 10 },
					}),
					ctx.conductor.createWorker({
						queue: "q3",
						tasks: [tasks.workQ3],
						config: { concurrency: 10 },
					}),
					ctx.conductor.createWorker({
						queue: "q4",
						tasks: [tasks.workQ4],
						config: { concurrency: 10 },
					}),
				],
			}),
			invoke: async () => {
				await Promise.all([
					...Array.from({ length: 250 }, () =>
						ctx.conductor.invoke({ name: "work-q1", queue: "q1" }, {}),
					),
					...Array.from({ length: 250 }, () =>
						ctx.conductor.invoke({ name: "work-q2", queue: "q2" }, {}),
					),
					...Array.from({ length: 250 }, () =>
						ctx.conductor.invoke({ name: "work-q3", queue: "q3" }, {}),
					),
					...Array.from({ length: 250 }, () =>
						ctx.conductor.invoke({ name: "work-q4", queue: "q4" }, {}),
					),
				]);
			},
		},

		// === Concurrency ===
		"concurrency/no-limit-100": {
			orchestrator: Orchestrator.create({
				conductor: ctx.conductor,
				tasks: [tasks.unlimited],
				defaultWorker: { concurrency: 50 },
			}),
			invoke: async () => {
				await Promise.all(
					Array.from({ length: 100 }, () => ctx.conductor.invoke({ name: "unlimited" }, {})),
				);
			},
		},

		"concurrency/limit-10-100": {
			orchestrator: Orchestrator.create({
				conductor: ctx.conductor,
				tasks: [tasks.limited],
				defaultWorker: { concurrency: 50 },
			}),
			invoke: async () => {
				await Promise.all(
					Array.from({ length: 100 }, () => ctx.conductor.invoke({ name: "limited" }, {})),
				);
			},
		},
	};
}

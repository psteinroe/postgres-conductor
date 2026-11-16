import { test, expect, describe } from "vitest";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { z } from "zod";

// Mock sql instance for type-only tests (no actual DB connection)
const mockSql = {} as any;

describe("task queue type constraints", () => {
	test("createWorker accepts tasks from matching queue", () => {
		const notificationTask = defineTask({
			name: "send-email",
			queue: "notifications",
			payload: z.object({ to: z.string() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([notificationTask]),
			context: {},
		});

		const emailTask = conductor.createTask(
			{ name: "send-email", queue: "notifications" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		// task queue matches worker queue
		const worker = conductor.createWorker({
			queue: "notifications",
			tasks: [emailTask],
		});

		expect(worker).toBeDefined();
	});

	test("createWorker rejects tasks from different queue", () => {
		const notificationTask = defineTask({
			name: "send-email",
			queue: "notifications",
			payload: z.object({ to: z.string() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([notificationTask]),
			context: {},
		});

		const emailTask = conductor.createTask(
			{ name: "send-email", queue: "notifications" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		expect(() =>
			conductor.createWorker({
				queue: "default",
				// @ts-expect-error - queue mismatch
				tasks: [emailTask],
			}),
		).toThrow('Task "send-email" belongs to queue "notifications", not "default"');
	});

	test("createWorker rejects mixed queue tasks", () => {
		const notificationTask = defineTask({
			name: "send-email",
			queue: "notifications",
			payload: z.object({ to: z.string() }),
		});

		const defaultTask = defineTask({
			name: "process-data",
			payload: z.object({ data: z.string() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([notificationTask, defaultTask]),
			context: {},
		});

		const emailTask = conductor.createTask(
			{ name: "send-email", queue: "notifications" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		const processTask = conductor.createTask(
			{ name: "process-data" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		// Type error: tasks have different queues
		expect(() =>
			conductor.createWorker({
				queue: "notifications",
				tasks: [
					emailTask,
					// @ts-expect-error - processTask is on default queue, not notifications
					processTask,
				],
			}),
		).toThrow('Task "process-data" belongs to queue "default", not "notifications"');
	});

	test("Orchestrator.create accepts tasks from default queue", () => {
		const defaultTask = defineTask({
			name: "process-data",
			payload: z.object({ data: z.string() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([defaultTask]),
			context: {},
		});

		const processTask = conductor.createTask(
			{ name: "process-data" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		// task is on default queue
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [processTask],
		});

		expect(orchestrator).toBeDefined();
	});

	test("Orchestrator.create rejects tasks from non-default queue", () => {
		const notificationTask = defineTask({
			name: "send-email",
			queue: "notifications",
			payload: z.object({ to: z.string() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([notificationTask]),
			context: {},
		});

		const emailTask = conductor.createTask(
			{ name: "send-email", queue: "notifications" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		expect(() =>
			Orchestrator.create({
				conductor,
				// @ts-expect-error - task is on notifications queue, not default
				tasks: [emailTask],
			}),
		).toThrow('Task "send-email" belongs to queue "notifications", not "default"');
	});

	test("Orchestrator.create rejects mixed queue tasks", () => {
		const notificationTask = defineTask({
			name: "send-email",
			queue: "notifications",
			payload: z.object({ to: z.string() }),
		});

		const defaultTask = defineTask({
			name: "process-data",
			payload: z.object({ data: z.string() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([notificationTask, defaultTask]),
			context: {},
		});

		const emailTask = conductor.createTask(
			{ name: "send-email", queue: "notifications" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		const processTask = conductor.createTask(
			{ name: "process-data" },
			{ invocable: true },
			async (_event, _ctx) => {},
		);

		expect(() =>
			Orchestrator.create({
				conductor,
				tasks: [
					processTask,
					// @ts-expect-error - emailTask is on notifications queue, not default
					emailTask,
				],
			}),
		).toThrow('Task "send-email" belongs to queue "notifications", not "default"');
	});
});

describe("a task of another queue must be named with its queue", () => {
	const email = defineTask({
		name: "send-email",
		queue: "notifications",
		payload: z.object({ to: z.string() }),
		returns: z.object({ ok: z.boolean() }),
		signals: { stop: z.object({}) },
	});
	const local = defineTask({ name: "local", returns: z.object({ n: z.number() }) });
	const conductor = Conductor.create({
		sql: mockSql,
		tasks: TaskSchemas.fromSchema([email, local]),
		context: {},
	});
	const notifications = { name: "send-email", queue: "notifications" } as const;

	test("conductor methods", () => {
		if (false) {
			// @ts-expect-error - queue left out
			void conductor.waitForResult({ name: "send-email" }, "id");
			// @ts-expect-error - queue left out
			void conductor.getExecution({ name: "send-email" }, "id");
			// @ts-expect-error - queue left out
			void conductor.getExecution({ task: { name: "send-email" }, key: "k" });
			// @ts-expect-error - queue left out
			void conductor.listExecutions({ task: { name: "send-email" } });
			// @ts-expect-error - queue left out
			void conductor.signal({ task: { name: "send-email" }, id: "id" }, { name: "stop", id: "s" });
			// @ts-expect-error - queue left out
			void conductor.signalWithStart({
				task: "send-email",
				key: "k",
				signal: { name: "stop", id: "s" },
			});

			expectTypeOf(conductor.waitForResult(notifications, "id")).resolves.toEqualTypeOf<{
				ok: boolean;
			}>();
			expectTypeOf(conductor.waitForResult({ name: "local" }, "id")).resolves.toEqualTypeOf<{
				n: number;
			}>();
			void conductor.getExecution(notifications, "id");
			void conductor.listExecutions({ task: notifications, where: { to: "a" } });
			void conductor.signal(
				{ task: notifications, key: "k" },
				{ name: "stop", id: "s", payload: {} },
			);
			void conductor.signalWithStart({
				task: "send-email",
				queue: "notifications",
				key: "k",
				signal: { name: "stop", id: "s", payload: {} },
			});
		}
	});

	test("context methods", () => {
		conductor.createTask({ name: "local" }, { invocable: true }, async (_event, ctx) => {
			if (false) {
				// @ts-expect-error - queue left out
				await ctx.waitForResult("w", { name: "send-email" }, "id");
				// @ts-expect-error - queue left out
				await ctx.unschedule({ name: "send-email" }, "nightly");
				// @ts-expect-error - queue left out
				await ctx.getExecution({ name: "send-email" }, "id");
				// @ts-expect-error - queue left out
				await ctx.sendSignal({ task: { name: "send-email" }, id: "id" }, { name: "stop", id: "s" });

				await ctx.unschedule(notifications, "nightly");
				await ctx.invoke("i", notifications, { to: "a" });
				expectTypeOf(await ctx.waitForResult("w", notifications, "id")).toEqualTypeOf<
					| { status: "completed"; result: { ok: boolean } }
					| { status: "failed"; error: string }
					| { status: "cancelled"; reason: string }
					| { status: "timeout" }
				>();
			}
			return { n: 1 };
		});
	});
});

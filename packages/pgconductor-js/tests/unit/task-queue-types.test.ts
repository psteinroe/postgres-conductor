import { test, expect, describe } from "bun:test";
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

		conductor.createWorker({
			queue: "default",
			// @ts-expect-error - queue mismatch
			tasks: [emailTask],
		});
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
		conductor.createWorker({
			queue: "notifications",
			tasks: [
				emailTask,
				// @ts-expect-error - processTask is on default queue, not notifications
				processTask,
			],
		});
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

		Orchestrator.create({
			conductor,
			// @ts-expect-error - task is on notifications queue, not default
			tasks: [emailTask],
		});
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

		Orchestrator.create({
			conductor,
			tasks: [
				processTask,
				// @ts-expect-error - emailTask is on notifications queue, not default
				emailTask,
			],
		});
	});
});

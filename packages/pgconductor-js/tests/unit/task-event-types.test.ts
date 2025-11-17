import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { z } from "zod";

describe("task event types", () => {
	test("createTask with discriminated union - cron has no payload, invoke has payload", () => {
		const taskDef = defineTask({
			name: "test-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = new Conductor({
			connectionString: "postgres://test",
			tasks: [taskDef] as const,
			context: {},
		});

		const task = conductor.createTask(
			{ name: "test-task" },
			[{ invocable: true }, { cron: "0 0 * * *" }] as const,
			async (event, _ctx) => {
				expectTypeOf(event).toExtend<
					| { event: "pgconductor.cron" }
					| { event: "pgconductor.invoke"; payload: { value: number } }
				>();

				if (event.event === "pgconductor.cron") {
					expectTypeOf(event).toEqualTypeOf<{ event: "pgconductor.cron" }>();

					// @ts-expect-error - cron events don't have payload
					const _invalid = event.payload;
				} else {
					expectTypeOf(event).toEqualTypeOf<{
						event: "pgconductor.invoke";
						payload: { value: number };
					}>();

					expectTypeOf(event.payload).toEqualTypeOf<{ value: number }>();
					expectTypeOf(event.payload.value).toEqualTypeOf<number>();
				}
			},
		);

		expect(task.name).toBe("test-task");
	});

	test("createTask with empty payload - cron still has no payload", () => {
		const taskDef = defineTask({
			name: "empty-task",
		});

		const conductor = new Conductor({
			connectionString: "postgres://test",
			tasks: [taskDef] as const,
			context: {},
		});

		const task = conductor.createTask(
			{ name: "empty-task" },
			[{ invocable: true }, { cron: "*/5 * * * *" }] as const,
			async (event, _ctx) => {
				if (event.event === "pgconductor.cron") {
					expectTypeOf(event).toEqualTypeOf<{ event: "pgconductor.cron" }>();
				} else {
					expectTypeOf(event).toExtend<{
						event: "pgconductor.invoke";
						payload: object;
					}>();
				}
			},
		);

		expect(task.name).toBe("empty-task");
	});

	test("createTask with only cron trigger", () => {
		const taskDef = defineTask({
			name: "empty-task",
		});

		const conductor = new Conductor({
			connectionString: "postgres://test",
			tasks: [taskDef] as const,
			context: {},
		});

		const task = conductor.createTask(
			{ name: "my-cron" },
			[{ cron: "*/5 * * * *" }] as const,
			async (event, _ctx) => {
				// Event should only be cron, no invoke event possible
				expectTypeOf(event).toEqualTypeOf<{ event: "pgconductor.cron" }>();

				// Verify it's cron
				expectTypeOf(event.event).toEqualTypeOf<"pgconductor.cron">();
			},
		);

		expect(task.name).toBe("my-cron");
	});

	test("createTask with only invocable trigger - only has invoke event", () => {
		const taskDef = defineTask({
			name: "invocable-task",
			payload: z.object({ data: z.string() }),
		});

		const conductor = new Conductor({
			connectionString: "postgres://test",
			tasks: [taskDef] as const,
			context: {},
		});

		const task = conductor.createTask(
			{ name: "invocable-task" },
			{ invocable: true },
			async (event, _ctx) => {
				// Event should only be invoke, no cron event possible
				expectTypeOf(event).toEqualTypeOf<{
					event: "pgconductor.invoke";
					payload: { data: string };
				}>();

				// Verify event properties
				expectTypeOf(event.event).toEqualTypeOf<"pgconductor.invoke">();
				expectTypeOf(event.payload).toEqualTypeOf<{ data: string }>();
				expectTypeOf(event.payload.data).toEqualTypeOf<string>();
			},
		);

		expect(task.name).toBe("invocable-task");
	});

	test("createTask with both invocable and cron triggers - has both events", () => {
		const taskDef = defineTask({
			name: "both-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = new Conductor({
			connectionString: "postgres://test",
			tasks: [taskDef] as const,
			context: {},
		});

		const task = conductor.createTask(
			{ name: "both-task" },
			[{ invocable: true }, { cron: "0 0 * * *" }] as const,
			async (event, _ctx) => {
				// Event can be either cron or invoke
				expectTypeOf(event).toExtend<
					| { event: "pgconductor.cron" }
					| { event: "pgconductor.invoke"; payload: { value: number } }
				>();

				if (event.event === "pgconductor.cron") {
					expectTypeOf(event).toEqualTypeOf<{ event: "pgconductor.cron" }>();
				} else {
					expectTypeOf(event).toEqualTypeOf<{
						event: "pgconductor.invoke";
						payload: { value: number };
					}>();
					expectTypeOf(event.payload.value).toEqualTypeOf<number>();
				}
			},
		);

		expect(task.name).toBe("both-task");
	});

	test("type error: invocable trigger without task definition", () => {
		const conductor = new Conductor({
			connectionString: "postgres://test",
			tasks: [] as const,
			context: {},
		});

		conductor.createTask(
			{ name: "undefined-task" },
			// @ts-expect-error - invocable trigger requires task definition
			{ invocable: true },
			async (_event, _ctx) => {},
		);
	});

	test("type error: task definition without invocable trigger", () => {
		const taskDef = defineTask({
			name: "defined-task",
		});

		const conductor = new Conductor({
			connectionString: "postgres://test",
			tasks: [taskDef] as const,
			context: {},
		});

		conductor.createTask(
			{ name: "defined-task" },
			// @ts-expect-error - task definition requires invocable trigger
			{ cron: "0 0 * * *" },
			async (_event, _ctx) => {},
		);
	});
});

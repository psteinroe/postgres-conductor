import { describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";

const sourceDefinition = defineTask({
	name: "source",
	queue: "source-q",
	payload: z.object({ value: z.string() }),
});
const compatibleDefinition = defineTask({
	name: "compatible",
	queue: "dlq",
	payload: z.object({ value: z.string() }),
});
const incompatibleDefinition = defineTask({
	name: "incompatible",
	queue: "dlq",
	payload: z.object({ other: z.number() }),
});

function conductor() {
	return Conductor.create({
		sql: {} as any,
		tasks: TaskSchemas.fromSchema([sourceDefinition, compatibleDefinition, incompatibleDefinition]),
		context: {},
	});
}

describe("dead-letter task types and validation", () => {
	test("accepts compatible targets and rejects incompatible payloads", () => {
		const c = conductor();
		const compatible = c.createTask(
			{ name: "compatible", queue: "dlq" },
			{ invocable: true },
			async () => {},
		);
		c.createTask(
			{ name: "source", queue: "source-q", deadLetter: { queue: "dlq", task: compatible } },
			{ invocable: true },
			async () => {},
		);

		const incompatible = c.createTask(
			{ name: "incompatible", queue: "dlq" },
			{ invocable: true },
			async () => {},
		);
		if (false)
			c.createTask(
				// @ts-expect-error The DLQ handler must accept the source payload.
				{ name: "source", queue: "source-q", deadLetter: { queue: "dlq", task: incompatible } },
				{ invocable: true },
				async () => {},
			);
	});

	test("rejects a direct self-target but permits a cross-queue identity", () => {
		const c = conductor();
		expect(() =>
			c.createTask(
				{ name: "source", queue: "source-q", deadLetter: { queue: "source-q" } },
				{ invocable: true },
				async () => {},
			),
		).toThrow("cannot dead-letter directly to itself");

		expect(() =>
			c.createTask(
				{ name: "source", queue: "source-q", deadLetter: { queue: "other-q" } },
				{ invocable: true },
				async () => {},
			),
		).not.toThrow();
	});
});

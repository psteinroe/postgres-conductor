import { z } from "zod";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import { test, expect, describe, beforeAll, afterAll, afterEach } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask, type DefineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

// Resolves asynchronously, like a validator that checks a value against a service
const asyncCode: StandardSchemaV1<{ code: string }, { code: string }> = {
	"~standard": {
		version: 1,
		vendor: "test",
		validate: async (value) => {
			await new Promise((resolve) => setTimeout(resolve, 10));
			const code = typeof value === "object" && value && "code" in value ? value.code : null;
			return code === "valid"
				? { value: { code } }
				: { issues: [{ message: "unknown code", path: ["code"] }] };
		},
	},
};

const throwing: StandardSchemaV1<{}, {}> = {
	"~standard": {
		version: 1,
		vendor: "test",
		validate: () => {
			throw new Error("validator unavailable");
		},
	},
};

const charge = defineTask({
	name: "charge",
	payload: z.object({ amount: z.number().positive() }),
	returns: z.object({ charged: z.number() }),
});

const normalize = defineTask({
	name: "normalize",
	payload: z.object({
		email: z.string().trim().toLowerCase(),
		retries: z.number().default(3),
	}),
	returns: z.object({ email: z.string(), at: z.string().transform((at) => at.slice(0, 4)) }),
});

const redeem = defineTask({ name: "redeem", payload: asyncCode });

const sum = defineTask({
	name: "sum",
	payload: z.object({ a: z.number() }),
	returns: z.object({ total: z.number().int() }),
});

const unavailable = defineTask({ name: "unavailable", payload: throwing });

const batchSquare = defineTask({
	name: "batch-square",
	payload: z.object({ value: z.number().int() }),
	returns: z.object({ square: z.number().max(50) }),
});

type Untyped = DefineTask<{ name: "untyped"; payload: { amount: number } }>;

describe("Task schema validation", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function setup() {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				charge,
				normalize,
				redeem,
				sum,
				unavailable,
				batchSquare,
			]).fromUnion<Untyped>(),
			context: {},
		});

		const received: { task: string; payload: unknown }[] = [];
		const record = (task: string, event: { name: string; payload?: unknown }) => {
			received.push({ task, payload: event.payload });
		};

		const tasks = [
			conductor.createTask({ name: "charge" }, { invocable: true }, async (event) => {
				record("charge", event);
				return { charged: event.payload.amount };
			}),
			conductor.createTask({ name: "normalize" }, { invocable: true }, async (event) => {
				record("normalize", event);
				return { email: event.payload.email, at: "2024-01-01" };
			}),
			conductor.createTask({ name: "redeem" }, { invocable: true }, async (event) => {
				record("redeem", event);
			}),
			conductor.createTask({ name: "sum" }, { invocable: true }, async (event) => {
				record("sum", event);
				return { total: event.payload.a };
			}),
			conductor.createTask({ name: "unavailable" }, { invocable: true }, async (event) => {
				record("unavailable", event);
			}),
			conductor.createTask(
				{ name: "batch-square", batch: { size: 4, timeoutMs: 50 } },
				{ invocable: true },
				async (events) =>
					events.map((event) => {
						record("batch-square", event);
						return { square: event.payload.value ** 2 };
					}),
			),
			conductor.createTask({ name: "untyped" }, { invocable: true }, async (event) => {
				record("untyped", event);
			}),
		];

		const orchestrator = Orchestrator.create({
			conductor,
			tasks,
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50, concurrency: 2 },
		});

		await conductor.ensureInstalled();
		return { db, conductor, orchestrator, received };
	}

	test("an invalid payload fails without retries and never reaches the handler", async () => {
		const { conductor, orchestrator, received } = await setup();

		const id = await conductor.invoke({ name: "charge" }, { amount: -1 });
		await orchestrator.drain();

		expect(await conductor.getExecution(id)).toMatchObject({
			status: "failed",
			attempts: 1,
			error: 'Invalid payload for task "charge": amount: Too small: expected number to be >0',
		});
		expect(received).toEqual([]);
	});

	test("payloads of executions invoked from SQL are validated", async () => {
		const { db, conductor, orchestrator, received } = await setup();

		const [invalid] = await db.sql<{ id: string }[]>`
			select id from pgconductor.invoke('charge', 'default', '{"amount": "10"}'::jsonb)
		`;
		const [missing] = await db.sql<{ id: string }[]>`
			select id from pgconductor.invoke('charge')
		`;
		const [valid] = await db.sql<{ id: string }[]>`
			select id from pgconductor.invoke('charge', 'default', '{"amount": 10}'::jsonb)
		`;
		await orchestrator.drain();

		expect(await conductor.getExecution(invalid?.id || "")).toMatchObject({
			status: "failed",
			error:
				'Invalid payload for task "charge": amount: Invalid input: expected number, received string',
		});
		expect(await conductor.getExecution(missing?.id || "")).toMatchObject({ status: "failed" });
		expect(await conductor.getExecution(valid?.id || "")).toMatchObject({
			status: "completed",
			result: { charged: 10 },
		});
		expect(received).toEqual([{ task: "charge", payload: { amount: 10 } }]);
	});

	test("the handler receives the parsed payload and the parsed return value is stored", async () => {
		const { conductor, orchestrator, received } = await setup();

		const id = await conductor.invoke({ name: "normalize" }, {
			email: "  Alice@Example.COM ",
			retries: 1,
			ignored: true,
		} as {
			email: string;
			retries: number;
		});
		await orchestrator.drain();

		expect(received).toEqual([
			{ task: "normalize", payload: { email: "alice@example.com", retries: 1 } },
		]);
		expect(await conductor.getExecution(id)).toMatchObject({
			status: "completed",
			result: { email: "alice@example.com", at: "2024" },
		});
	});

	test("schema defaults apply to payloads", async () => {
		const { db, orchestrator, received } = await setup();

		await db.sql`select pgconductor.invoke('normalize', 'default', '{"email": "bob@example.com"}'::jsonb)`;
		await orchestrator.drain();

		expect(received).toEqual([
			{ task: "normalize", payload: { email: "bob@example.com", retries: 3 } },
		]);
	});

	test("async validators are awaited", async () => {
		const { conductor, orchestrator, received } = await setup();

		const valid = await conductor.invoke({ name: "redeem" }, { code: "valid" });
		const invalid = await conductor.invoke({ name: "redeem" }, { code: "expired" });
		await orchestrator.drain();

		expect(await conductor.getExecution(valid)).toMatchObject({ status: "completed" });
		expect(await conductor.getExecution(invalid)).toMatchObject({
			status: "failed",
			attempts: 1,
			error: 'Invalid payload for task "redeem": code: unknown code',
		});
		expect(received).toEqual([{ task: "redeem", payload: { code: "valid" } }]);
	});

	test("an invalid return value fails without retries", async () => {
		const { conductor, orchestrator, received } = await setup();

		const id = await conductor.invoke({ name: "sum" }, { a: 1.5 });
		await orchestrator.drain();

		expect(await conductor.getExecution(id)).toMatchObject({
			status: "failed",
			attempts: 1,
			result: null,
			error:
				'Invalid return value of task "sum": total: Invalid input: expected int, received number',
		});
		expect(received).toHaveLength(1);
	});

	test("a validator that throws fails the attempt and retries", async () => {
		const { db, conductor, orchestrator, received } = await setup();

		const id = await conductor.invoke({ name: "unavailable" }, {});
		await orchestrator.drain();

		const [row] = await db.sql<{ attempts: number; failed_at: Date | null; last_error: string }[]>`
			select attempts, failed_at, last_error from pgconductor._private_executions where id = ${id}
		`;
		expect(row).toEqual({ attempts: 1, failed_at: null, last_error: "validator unavailable" });
		expect(received).toEqual([]);
	});

	test("batch tasks validate payloads and return values per item", async () => {
		const { conductor, orchestrator, received } = await setup();

		const [three, half, nine, four] = await conductor.invoke({ name: "batch-square" }, [
			{ payload: { value: 3 } },
			{ payload: { value: 0.5 } },
			{ payload: { value: 9 } },
			{ payload: { value: 4 } },
		]);
		await orchestrator.drain();

		expect(received.map(({ payload }) => payload)).toHaveLength(3);
		expect(received.map(({ payload }) => payload)).toEqual(
			expect.arrayContaining([{ value: 3 }, { value: 9 }, { value: 4 }]),
		);
		expect(await conductor.getExecution(three || "")).toMatchObject({
			status: "completed",
			result: { square: 9 },
		});
		expect(await conductor.getExecution(half || "")).toMatchObject({
			status: "failed",
			attempts: 1,
			error:
				'Invalid payload for task "batch-square": value: Invalid input: expected int, received number',
		});
		expect(await conductor.getExecution(nine || "")).toMatchObject({
			status: "failed",
			attempts: 1,
			error:
				'Invalid return value of task "batch-square": square: Too big: expected number to be <=50',
		});
		expect(await conductor.getExecution(four || "")).toMatchObject({
			status: "completed",
			result: { square: 16 },
		});
	});

	test("type-only definitions are not validated", async () => {
		const { db, orchestrator, received } = await setup();

		await db.sql`select pgconductor.invoke('untyped', 'default', '{"amount": "ten"}'::jsonb)`;
		await orchestrator.drain();

		expect(received).toEqual([{ task: "untyped", payload: { amount: "ten" } }]);
	});
});

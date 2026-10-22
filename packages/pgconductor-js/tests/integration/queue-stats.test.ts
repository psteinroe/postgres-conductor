import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const workDefinition = defineTask({ name: "work", payload: z.object({ hold: z.boolean() }) });
const boomDefinition = defineTask({ name: "boom", queue: "other" });

describe("pgconductor.queue_stats", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.allSettled(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
		await Promise.all(databases.map((database) => database.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("counts ready, running, suspended and recently failed executions per queue", async () => {
		const db = await pool.child();
		databases.push(db);
		await db.clock.set(new Date("2024-01-01T12:00:00Z"));
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([workDefinition, boomDefinition]),
			context: {},
			clock: db.clock,
		});
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async (event, ctx) => {
			if (event.name === "pgconductor.invoke" && event.payload.hold) {
				await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve));
			}
		});
		const boom = conductor.createTask(
			{ name: "boom", queue: "other", maxAttempts: 1 },
			{ invocable: true },
			async () => {
				throw new Error("boom");
			},
		);
		const config = { pollIntervalMs: 10, flushIntervalMs: 10, concurrency: 1 };
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [work],
			defaultWorker: config,
			workers: [conductor.createWorker({ queue: "other", tasks: [boom], config })],
		});
		orchestrators.push(orchestrator);
		await conductor.ensureInstalled();
		await orchestrator.start();

		const failed = async (id: string) =>
			waitForCondition(async () => (await conductor.getExecution(id))?.status === "failed");
		await failed(await conductor.invoke({ name: "boom", queue: "other" }, {}));
		await db.clock.advance("2h");
		await failed(await conductor.invoke({ name: "boom", queue: "other" }, {}));
		const cancelled = await conductor.invoke(
			{ name: "boom", queue: "other" },
			{},
			{ run_at: new Date(db.clock.now().getTime() + 3_600_000) },
		);
		await conductor.cancel(cancelled);

		const held = await conductor.invoke({ name: "work" }, { hold: true });
		await waitForCondition(async () => (await conductor.getExecution(held))?.status === "running");
		await conductor.invoke({ name: "work" }, { hold: false });
		await conductor.invoke({ name: "work" }, { hold: false });
		await conductor.invoke(
			{ name: "work" },
			{ hold: false },
			{ run_at: new Date(db.clock.now().getTime() + 3_600_000) },
		);
		await db.clock.advance("10m");

		// every queue also holds the next occurrence of its daily maintenance task
		const stats = await db.sql`
			select queue, ready::int, extract(epoch from oldest_ready_age)::int as oldest_ready_age,
				running::int, suspended::int, failed_last_hour::int
			from pgconductor.queue_stats
			where queue <> 'pgconductor.internal'
			order by queue
		`;

		expect(stats).toEqual([
			{
				queue: "default",
				ready: 2,
				oldest_ready_age: expect.any(Number),
				running: 1,
				suspended: 2,
				failed_last_hour: 0,
			},
			{
				queue: "other",
				ready: 0,
				oldest_ready_age: null,
				running: 0,
				suspended: 1,
				failed_last_hour: 1,
			},
		]);
		expect(stats[0]?.oldest_ready_age).toBeGreaterThanOrEqual(600);
		expect(stats[0]?.oldest_ready_age).toBeLessThan(660);
	});
});

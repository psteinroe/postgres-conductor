import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";

const workerConfig = {
	concurrency: 10,
	fetchBatchSize: 10,
	flushBatchSize: 10,
	pollIntervalMs: 10,
	flushIntervalMs: 10,
};

// Group limits are intentionally soft: concurrent claim transactions may race.
// These tests use blockers and avoid asserting exact global bounds across workers.

async function waitUntil(predicate: () => boolean, timeoutMs = 20_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) {
			throw new Error("Timed out waiting for condition");
		}
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
}

describe("Group concurrency", () => {
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

	test("serializes executions in the same group", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = defineTask({
			name: "same-group",
			payload: z.object({ id: z.number() }),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const started: number[] = [];
		const blockers = new Map<number, Deferred<void>>([
			[1, new Deferred<void>()],
			[2, new Deferred<void>()],
		]);
		const task = conductor.createTask(
			{ name: "same-group", groupConcurrency: 1 },
			{ invocable: true },
			async (event) => {
				started.push(event.payload.id);
				const blocker = blockers.get(event.payload.id);
				if (blocker) await blocker.promise;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: workerConfig,
		});

		await orchestrator.start();
		try {
			await conductor.invoke({ name: "same-group" }, { id: 1 }, { group: "tenant-a" });
			await conductor.invoke({ name: "same-group" }, { id: 2 }, { group: "tenant-a" });
			await waitUntil(() => started.length === 1);
			await new Promise((resolve) => setTimeout(resolve, 100));
			const first = started[0];
			expect(first === 1 || first === 2).toBe(true);

			blockers.get(first!)?.resolve();
			await waitUntil(() => started.length === 2);
			expect(new Set(started)).toEqual(new Set([1, 2]));
		} finally {
			blockers.forEach((blocker) => blocker.resolve());
			await orchestrator.stop();
		}
	}, 30000);

	test("allows different groups to run in parallel", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = defineTask({
			name: "different-groups",
			payload: z.object({ id: z.number() }),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const started: number[] = [];
		const blocker = new Deferred<void>();
		const task = conductor.createTask(
			{ name: "different-groups", groupConcurrency: 1 },
			{ invocable: true },
			async (event) => {
				started.push(event.payload.id);
				await blocker.promise;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: workerConfig,
		});

		await orchestrator.start();
		try {
			await conductor.invoke({ name: "different-groups" }, { id: 1 }, { group: "tenant-a" });
			await waitUntil(() => started.includes(1));
			await conductor.invoke({ name: "different-groups" }, { id: 2 }, { group: "tenant-b" });
			await waitUntil(() => started.length === 2);
			expect(new Set(started)).toEqual(new Set([1, 2]));
		} finally {
			blocker.resolve();
			await orchestrator.stop();
		}
	}, 30000);

	test("does not apply the group limit to ungrouped executions", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = defineTask({
			name: "ungrouped",
			payload: z.object({ id: z.number() }),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const started: number[] = [];
		const blocker = new Deferred<void>();
		const task = conductor.createTask(
			{ name: "ungrouped", groupConcurrency: 1 },
			{ invocable: true },
			async (event) => {
				started.push(event.payload.id);
				await blocker.promise;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: workerConfig,
		});

		await orchestrator.start();
		try {
			await conductor.invoke({ name: "ungrouped" }, { id: 1 }, { group: "tenant-a" });
			await conductor.invoke({ name: "ungrouped" }, { id: 2 });
			await waitUntil(() => started.length === 2);
			expect(new Set(started)).toEqual(new Set([1, 2]));
		} finally {
			blocker.resolve();
			await orchestrator.stop();
		}
	}, 30000);

	test("composes task and group concurrency limits", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = defineTask({
			name: "composed-limits",
			payload: z.object({ id: z.number() }),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const started: number[] = [];
		const blockers = new Map<number, Deferred<void>>([
			[1, new Deferred<void>()],
			[2, new Deferred<void>()],
			[3, new Deferred<void>()],
		]);
		const task = conductor.createTask(
			{ name: "composed-limits", concurrency: 2, groupConcurrency: 1 },
			{ invocable: true },
			async (event) => {
				started.push(event.payload.id);
				const blocker = blockers.get(event.payload.id);
				if (blocker) await blocker.promise;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: workerConfig,
		});

		await orchestrator.start();
		try {
			await conductor.invoke({ name: "composed-limits" }, { id: 1 }, { group: "tenant-a" });
			await conductor.invoke({ name: "composed-limits" }, { id: 3 }, { group: "tenant-b" });
			await conductor.invoke({ name: "composed-limits" }, { id: 2 }, { group: "tenant-a" });
			await waitUntil(() => started.includes(3) && started.some((id) => id === 1 || id === 2));
			await new Promise((resolve) => setTimeout(resolve, 100));
			expect(started).toContain(3);
			expect(started.filter((id) => id === 1 || id === 2)).toHaveLength(1);

			blockers.forEach((blocker) => blocker.resolve());
			await waitUntil(() => started.length === 3);
		} finally {
			blockers.forEach((blocker) => blocker.resolve());
			await orchestrator.stop();
		}
	}, 30000);

	test("does not share a group across tasks or queues", async () => {
		const db = await pool.child();
		databases.push(db);
		const taskADefinition = defineTask({ name: "scope-a" });
		const taskBDefinition = defineTask({ name: "scope-b" });
		const queueTaskDefinition = defineTask({ name: "scope-queue", queue: "other" });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskADefinition, taskBDefinition, queueTaskDefinition]),
			context: {},
		});
		const started = new Set<string>();
		const blocker = new Deferred<void>();
		const taskA = conductor.createTask(
			{ name: "scope-a", groupConcurrency: 1 },
			{ invocable: true },
			async () => {
				started.add("task-a");
				await blocker.promise;
			},
		);
		const taskB = conductor.createTask(
			{ name: "scope-b", groupConcurrency: 1 },
			{ invocable: true },
			async () => {
				started.add("task-b");
				await blocker.promise;
			},
		);
		const queueTask = conductor.createTask(
			{ name: "scope-queue", queue: "other", groupConcurrency: 1 },
			{ invocable: true },
			async () => {
				started.add("queue");
				await blocker.promise;
			},
		);
		const otherWorker = conductor.createWorker({
			queue: "other",
			tasks: [queueTask],
			config: workerConfig,
		});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [taskA, taskB],
			workers: [otherWorker],
			defaultWorker: workerConfig,
		});

		await orchestrator.start();
		try {
			await conductor.invoke({ name: "scope-a" }, {}, { group: "shared" });
			await conductor.invoke({ name: "scope-b" }, {}, { group: "shared" });
			await conductor.invoke({ name: "scope-queue", queue: "other" }, {}, { group: "shared" });
			await waitUntil(() => started.size === 3);
			expect(started).toEqual(new Set(["task-a", "task-b", "queue"]));
		} finally {
			blocker.resolve();
			await orchestrator.stop();
		}
	}, 30000);

	test("propagates group metadata through batch invocation", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = defineTask({
			name: "batch-group",
			payload: z.object({ id: z.number() }),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const started: number[] = [];
		const blockers = new Map<number, Deferred<void>>([
			[1, new Deferred<void>()],
			[2, new Deferred<void>()],
		]);
		const task = conductor.createTask(
			{ name: "batch-group", groupConcurrency: 1 },
			{ invocable: true },
			async (event) => {
				started.push(event.payload.id);
				const blocker = blockers.get(event.payload.id);
				if (blocker) await blocker.promise;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: workerConfig,
		});

		await orchestrator.start();
		try {
			await conductor.invoke({ name: "batch-group" }, [
				{ payload: { id: 1 }, group: "batch-tenant" },
				{ payload: { id: 2 }, group: "batch-tenant" },
			]);
			const rows = await db.sql<{ group: string | null }[]>`
				select "group"
				from pgconductor._private_executions
				where task_key = 'batch-group'
				order by created_at asc, id asc
			`;
			expect(rows.map((row) => row.group)).toEqual(["batch-tenant", "batch-tenant"]);

			await waitUntil(() => started.length === 1);
			await new Promise((resolve) => setTimeout(resolve, 100));
			const first = started[0];
			expect(first === 1 || first === 2).toBe(true);
			blockers.get(first!)?.resolve();
			await waitUntil(() => started.length === 2);
			expect(new Set(started)).toEqual(new Set([1, 2]));
		} finally {
			blockers.forEach((blocker) => blocker.resolve());
			await orchestrator.stop();
		}
	}, 30000);

	test("rejects invalid task and group concurrency values", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = defineTask({ name: "validation" });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const invalidValues = [0, -1, 1.5];

		for (const value of invalidValues) {
			expect(() =>
				conductor.createTask(
					{ name: "validation", concurrency: value },
					{ invocable: true },
					async () => {},
				),
			).toThrow("concurrency must be a positive integer");
			expect(() =>
				conductor.createTask(
					{ name: "validation", groupConcurrency: value },
					{ invocable: true },
					async () => {},
				),
			).toThrow("groupConcurrency must be a positive integer");
		}
	}, 30000);
});

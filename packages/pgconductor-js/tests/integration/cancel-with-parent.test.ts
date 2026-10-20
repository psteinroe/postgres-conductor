import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const mode = z.enum(["block", "sleep", "wait", "await", "nest", "quick"]);
const event = defineEvent({ name: "linked.never", payload: z.object({}) });

type ParentEnd = "cancel-suspended" | "cancel-running" | "fail";

describe("ctx.invoke with cancelWithParent (Postgres integration)", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];
	let release = new Deferred<void>();

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	beforeEach(() => {
		release = new Deferred<void>();
	});

	afterEach(async () => {
		release.resolve();
		await Promise.allSettled(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
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
				defineTask({ name: "parent", payload: z.object({ end: z.string() }) }),
				defineTask({ name: "child", payload: z.object({ label: z.string(), mode }) }),
			]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const end = new Deferred<void>();
		const child = conductor.createTask({ name: "child" }, { invocable: true }, async (e, ctx) => {
			switch (e.payload.mode) {
				case "block":
					return release.promise;
				case "sleep":
					return ctx.sleep("nap", 60 * 60 * 1000);
				case "wait":
					await ctx.waitForEvent("never", { event });
					return;
				case "await":
					return ctx.invokeAndWait("inner", { name: "child" }, { label: "awaited", mode: "block" });
				case "nest":
					await ctx.invoke(
						"inner",
						{ name: "child" },
						{ label: "nested", mode: "sleep" },
						{ cancelWithParent: true },
					);
					return ctx.sleep("nap", 60 * 60 * 1000);
			}
		});
		const parent = conductor.createTask(
			{ name: "parent", maxAttempts: 1 },
			{ invocable: true },
			async (e, ctx) => {
				const linked = [
					["running", "block"],
					["sleeping", "sleep"],
					["waiting", "wait"],
					["awaiting", "await"],
					["nesting", "nest"],
					["completed", "quick"],
				] as const;
				for (const [label, mode] of linked) {
					await ctx.invoke(label, { name: "child" }, { label, mode }, { cancelWithParent: true });
				}
				await ctx.invoke(
					"pending",
					{ name: "child" },
					{ label: "pending", mode: "quick" },
					{ cancelWithParent: true, run_at: new Date(Date.now() + 60 * 60 * 1000) },
				);
				await ctx.invoke("unlinked", { name: "child" }, { label: "unlinked", mode: "block" });
				if (e.payload.end === "cancel-suspended") return ctx.sleep("nap", 60 * 60 * 1000);
				await end.promise;
				throw new Error("parent failed");
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, child],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10, concurrency: 10 },
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();

		const children = () =>
			db.sql<
				{
					label: string;
					running: boolean;
					suspended: boolean;
					attempts: number;
					status: string;
					error: string | null;
				}[]
			>`
				select payload->>'label' as label, locked_by is not null as running, attempts,
					locked_by is null and run_at > now() as suspended,
					case
						when completed_at is not null then 'completed'
						when failed_at is not null and cancelled then 'cancelled'
						when failed_at is not null then 'failed'
						when locked_by is not null then 'running'
						else 'pending'
					end as status,
					last_error as error
				from pgconductor._private_executions
				where task_key = 'child'
			`;

		return { db, conductor, end, children };
	}

	async function endParent(parentEnd: ParentEnd) {
		const { conductor, end, children } = await setup();
		const parentId = await conductor.invoke({ name: "parent" }, { end: parentEnd });

		await waitForCondition(async () => {
			const rows = Object.fromEntries((await children()).map((row) => [row.label, row]));
			const running = ["running", "awaited", "unlinked"];
			const suspended = ["sleeping", "waiting", "awaiting", "nesting", "nested"];
			return (
				running.every((label) => rows[label]?.running) &&
				suspended.every((label) => rows[label]?.suspended) &&
				rows.completed?.status === "completed" &&
				rows.pending?.attempts === 0
			);
		});
		if (parentEnd === "fail") end.resolve();
		else expect(await conductor.cancel(parentId, { reason: "stop" })).toBe(true);

		const reason = parentEnd === "fail" ? "parent failed" : "stop";
		const linked = ["running", "sleeping", "waiting", "awaiting", "awaited", "nesting", "nested"];
		await waitForCondition(async () =>
			(await children()).every((row) => !linked.includes(row.label) || row.status !== "running"),
		);
		release.resolve();
		await waitForCondition(async () =>
			(await children()).every((row) => row.label !== "unlinked" || row.status === "completed"),
		);

		const rows = Object.fromEntries(
			(await children()).map(({ label, status, error }) => [label, { status, error }]),
		);
		expect(rows).toEqual({
			running: { status: "cancelled", error: reason },
			sleeping: { status: "cancelled", error: reason },
			waiting: { status: "cancelled", error: reason },
			awaiting: { status: "cancelled", error: reason },
			awaited: { status: "cancelled", error: reason },
			nesting: { status: "cancelled", error: reason },
			nested: { status: "cancelled", error: reason },
			pending: { status: "cancelled", error: reason },
			completed: { status: "completed", error: null },
			unlinked: { status: "completed", error: null },
		});
		expect(await conductor.getExecution(parentId)).toMatchObject({
			status: parentEnd === "fail" ? "failed" : "cancelled",
			error: reason,
		});
	}

	test("cancelling a suspended parent cancels its linked children in every state", async () => {
		await endParent("cancel-suspended");
	}, 30000);

	test("cancelling a running parent cancels its linked children once it stops", async () => {
		await endParent("cancel-running");
	}, 30000);

	test("a parent failing permanently cancels its linked children", async () => {
		await endParent("fail");
	}, 30000);

	test("a parent that retries or completes leaves its linked children running", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "parent", returns: z.object({ id: z.string() }) }),
				defineTask({ name: "work", returns: z.object({ done: z.boolean() }) }),
			]),
			context: {},
		});
		let attempts = 0;
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async () => {
			await release.promise;
			return { done: true };
		});
		const parent = conductor.createTask(
			{ name: "parent", maxAttempts: 2 },
			{ invocable: true },
			async (_e, ctx) => {
				const id = await ctx.invoke("work", { name: "work" }, {}, { cancelWithParent: true });
				if (++attempts === 1) throw new Error("retry");
				return { id };
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, work],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10, concurrency: 2 },
		});
		orchestrators.push(orchestrator);

		await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });
		await orchestrator.start();
		const parentId = await conductor.invoke({ name: "parent" }, {});
		let retryAt: Date | undefined;
		await waitForCondition(async () => {
			const [row] = await db.sql<{ run_at: Date }[]>`
				select run_at from pgconductor._private_executions
				where id = ${parentId} and last_error is not null and locked_by is null
			`;
			retryAt = row?.run_at;
			return retryAt !== undefined;
		});
		await db.client.setFakeTime({ date: new Date((retryAt?.getTime() || 0) + 1) });
		const { id } = await conductor.waitForResult({ name: "parent" }, parentId, {
			pollIntervalMs: 10,
		});
		release.resolve();
		const result = await conductor.waitForResult({ name: "work" }, id, { pollIntervalMs: 10 });
		await db.client.clearFakeTime();

		expect(result).toEqual({ done: true });
		const [row] = await db.sql<{ parent_execution_id: string; cancel_with_parent: boolean }[]>`
			select parent_execution_id, cancel_with_parent from pgconductor._private_executions
			where id = ${id}
		`;
		expect(row).toEqual({ parent_execution_id: parentId, cancel_with_parent: true });
	}, 30000);
	for (const awaited of ["sleep", "throw"] as const) {
		test(`a parent failed by its awaited child (${awaited}) cancels its linked children`, async () => {
			const db = await pool.child();
			databases.push(db);
			const conductor = Conductor.create({
				sql: db.sql,
				tasks: TaskSchemas.fromSchema([
					defineTask({ name: "parent" }),
					defineTask({ name: "work", payload: z.object({ mode: z.string() }) }),
				]),
				context: {},
			});
			const work = conductor.createTask(
				{ name: "work", maxAttempts: 1 },
				{ invocable: true },
				async (e, ctx) => {
					if (e.payload.mode === "block") return release.promise;
					if (e.payload.mode === "sleep") return ctx.sleep("nap", 60 * 60 * 1000);
					throw new Error("boom");
				},
			);
			const parent = conductor.createTask(
				{ name: "parent" },
				{ invocable: true },
				async (_e, ctx) => {
					await ctx.invoke(
						"linked",
						{ name: "work" },
						{ mode: "block" },
						{ cancelWithParent: true },
					);
					await ctx.invokeAndWait("awaited", { name: "work" }, { mode: awaited });
				},
			);
			const orchestrator = Orchestrator.create({
				conductor,
				tasks: [parent, work],
				defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10, concurrency: 3 },
			});
			orchestrators.push(orchestrator);

			await orchestrator.start();
			const parentId = await conductor.invoke({ name: "parent" }, {});
			const rows = () =>
				db.sql<{ id: string; mode: string; locked_by: string | null; last_error: string | null }[]>`
					select id, payload->>'mode' as mode, locked_by, last_error
					from pgconductor._private_executions where task_key = 'work'
				`;
			const reason = awaited === "sleep" ? "stop" : "boom";
			await waitForCondition(async () => {
				const all = await rows();
				const linked = all.find((row) => row.mode === "block");
				const child = all.find((row) => row.mode === awaited);
				return (
					!!linked?.locked_by &&
					!!child &&
					!child.locked_by &&
					!!child.last_error === (awaited === "throw")
				);
			});
			if (awaited === "sleep") {
				const child = (await rows()).find((row) => row.mode === "sleep");
				expect(await conductor.cancel(child?.id || "", { reason })).toBe(true);
			}
			const linkedId = (await rows()).find((row) => row.mode === "block")?.id || "";
			await waitForCondition(
				async () => (await conductor.getExecution(linkedId))?.status === "cancelled",
			);

			expect(await conductor.getExecution(linkedId)).toMatchObject({
				error: `Child execution failed: ${reason}`,
			});
			expect(await conductor.getExecution(parentId)).toMatchObject({
				status: "failed",
				error: `Child execution failed: ${reason}`,
			});
		}, 30000);
	}

	test("a linked child suspending in the settlement that fails its parent is cancelled", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "parent" }), defineTask({ name: "work" })]),
			context: {},
		});
		const started = new Deferred<void>();
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async (_e, ctx) => {
			started.resolve();
			await release.promise;
			await ctx.sleep("nap", 60 * 60 * 1000);
		});
		const parent = conductor.createTask(
			{ name: "parent", maxAttempts: 1 },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.invoke("linked", { name: "work" }, {}, { cancelWithParent: true });
				await release.promise;
				throw new Error("parent failed");
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, work],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 1000, concurrency: 2 },
		});
		orchestrators.push(orchestrator);

		await orchestrator.start();
		const parentId = await conductor.invoke({ name: "parent" }, {});
		await started.promise;
		release.resolve();
		await waitForCondition(
			async () => (await conductor.getExecution(parentId))?.status === "failed",
		);

		const [linked] = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'work'
		`;
		expect(await conductor.getExecution(linked?.id || "")).toMatchObject({
			status: "cancelled",
			error: "parent failed",
		});
	}, 30000);
});

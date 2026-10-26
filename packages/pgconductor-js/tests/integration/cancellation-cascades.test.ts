import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import {
	ending,
	getExecution,
	insertExecution,
	install,
	openSession,
	outcome,
	settle,
	waitUntilBlocked,
	type Session,
} from "../fixtures/sessions";

// Every path that ends an execution cancels its linked children (cancelWithParent) from the
// graph as it is once the path holds its locks
describe("cancellation cascades (Postgres integration)", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const sessions: Session[] = [];
	const ORCH_A = crypto.randomUUID();
	const ORCH_B = crypto.randomUUID();

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.allSettled(sessions.map((s) => s.end()));
		sessions.length = 0;
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function database(removeOnFailDays: number | null = null) {
		const db = await pool.child();
		databases.push(db);
		await install(db, removeOnFailDays);
		return db;
	}

	// P has linked child L, which is running. Session A settles L as invoke_child (starts G and
	// suspends L on it) and leaves the transaction open.
	async function suspendingLinkedChild(db: TestDatabase, parentRunning: boolean) {
		const a = await openSession(db);
		const b = await openSession(db);
		sessions.push(a, b);

		const tokenP = crypto.randomUUID();
		const tokenL = crypto.randomUUID();
		const P = await insertExecution(
			db.sql,
			parentRunning
				? { task_key: "parent", locked_by: ORCH_B, claim_token: tokenP }
				: { task_key: "parent", waiting_step_key: "some-event", suspended: true },
		);
		const L = await insertExecution(db.sql, {
			task_key: "linked",
			parent_execution_id: P,
			cancel_with_parent: true,
			locked_by: ORCH_A,
			claim_token: tokenL,
		});

		await a.sql`begin`;
		await settle(a.sql, ORCH_A, [
			{
				execution_id: L,
				task_key: "linked",
				claim_token: tokenL,
				status: "invoke_child",
				timeout_ms: "infinity",
				step_key: "invoke-g",
				child_task_name: "grandchild",
				child_task_queue: "default",
				child_payload: null,
			},
		]);
		const [{ G }] = await a.sql<[{ G: string }]>`
			select id as "G" from pgconductor._private_executions where parent_execution_id = ${L}::uuid
		`;
		return { a, b, P, L, G, tokenP };
	}

	for (const removeOnFailDays of [null, 0]) {
		test(`a parent's failure settlement cancels a grandchild started while it waited (removeOnFail ${removeOnFailDays})`, async () => {
			const db = await database(removeOnFailDays);
			const { a, b, P, L, G, tokenP } = await suspendingLinkedChild(db, true);

			const failP = outcome(
				settle(b.sql, ORCH_B, [
					{
						execution_id: P,
						task_key: "parent",
						claim_token: tokenP,
						status: "permanently_failed",
						error: "parent failed",
					},
				]),
			);
			await waitUntilBlocked(db.sql, b, a);
			await a.sql`commit`;
			expect(await failP).toBe("ok");

			const cancelled = removeOnFailDays === 0 ? "removed" : { failed: true, cancelled: true };
			expect(await ending(db.sql, L)).toEqual(cancelled);
			expect(await ending(db.sql, G)).toEqual(cancelled);
		});
	}

	test("a parent's failure settlement sees linked children settled in the same batch in their new state", async () => {
		const db = await database();
		const token = crypto.randomUUID();
		const P = await insertExecution(db.sql, {
			task_key: "parent",
			locked_by: ORCH_A,
			claim_token: token,
		});
		const linked = () =>
			insertExecution(db.sql, {
				task_key: "linked",
				parent_execution_id: P,
				cancel_with_parent: true,
				locked_by: ORCH_A,
				claim_token: token,
			});
		const sleeping = await linked();
		const completing = await linked();

		await settle(db.sql, ORCH_A, [
			{
				execution_id: P,
				task_key: "parent",
				claim_token: token,
				status: "permanently_failed",
				error: "parent failed",
			},
			{
				execution_id: sleeping,
				task_key: "linked",
				claim_token: token,
				status: "released",
				reschedule_in_ms: "infinity",
				step_key: "nap",
			},
			{
				execution_id: completing,
				task_key: "linked",
				claim_token: token,
				status: "completed",
				result: { ok: true },
			},
		]);

		expect(await getExecution(db.sql, sleeping)).toMatchObject({
			cancelled: true,
			last_error: "parent failed",
			locked_by: null,
		});
		expect((await getExecution(db.sql, sleeping))?.failed_at).not.toBeNull();
		expect(await getExecution(db.sql, completing)).toMatchObject({
			cancelled: false,
			failed_at: null,
		});
		expect((await getExecution(db.sql, completing))?.completed_at).not.toBeNull();
	});

	test("cancel_execution of a suspended parent cancels a grandchild started while it waited", async () => {
		const db = await database();
		const { a, b, P, L, G } = await suspendingLinkedChild(db, false);

		const cancelP = outcome(b.sql`select pgconductor.cancel_execution(${P}::uuid)`);
		await waitUntilBlocked(db.sql, b, a);
		await a.sql`commit`;
		expect(await cancelP).toBe("ok");

		expect(await ending(db.sql, L)).toEqual({ failed: true, cancelled: true });
		expect(await ending(db.sql, G)).toEqual({ failed: true, cancelled: true });
	});

	// Reinvoking a deduped parent supersedes its running attempt, which releases the claim, so
	// neither that attempt's settlement nor a later cancel can cascade to the children
	for (const removeOnFailDays of [null, 0]) {
		for (const childState of ["pending", "suspended"] as const) {
			test(`superseding a parent cancels its ${childState} linked child (removeOnFail ${removeOnFailDays})`, async () => {
				const db = await database(removeOnFailDays);
				const P = await insertExecution(db.sql, {
					task_key: "parent",
					dedupe_key: "k",
					locked_by: ORCH_A,
					claim_token: crypto.randomUUID(),
				});
				const C = await insertExecution(db.sql, {
					task_key: "linked",
					parent_execution_id: P,
					cancel_with_parent: true,
					...(childState === "suspended" ? { waiting_step_key: "forever", suspended: true } : {}),
				});

				const [reinvoked] = await db.sql<[{ id: string }]>`
					select id from pgconductor.invoke(p_task_key := 'parent', p_dedupe_key := 'k')
				`;
				expect(reinvoked.id).not.toBe(P);
				expect((await getExecution(db.sql, P))?.last_error || "removed").toMatch(
					removeOnFailDays === 0 ? "removed" : "superseded by reinvoke",
				);

				const child = await getExecution(db.sql, C);
				expect(
					child ? { cancelled: child.cancelled, last_error: child.last_error } : "removed",
				).toEqual(
					removeOnFailDays === 0
						? "removed"
						: { cancelled: true, last_error: "superseded by reinvoke" },
				);
			});
		}
	}

	test("an invokeAndWait timeout cancels the awaited child with its children", async () => {
		const db = await database();
		const tokenP = crypto.randomUUID();
		const P = await insertExecution(db.sql, {
			task_key: "parent",
			locked_by: ORCH_A,
			claim_token: tokenP,
		});
		const C = crypto.randomUUID();
		const G = await insertExecution(db.sql, { task_key: "grandchild", parent_execution_id: C });
		await insertExecution(db.sql, {
			id: C,
			task_key: "child",
			parent_execution_id: P,
			waiting_on_execution_id: G,
			waiting_step_key: "wait-g",
			suspended: true,
		});
		const L = await insertExecution(db.sql, {
			task_key: "linked",
			parent_execution_id: C,
			cancel_with_parent: true,
		});
		// P resumed after its timeout, still waiting on C
		await db.sql`
			update pgconductor._private_executions set waiting_on_execution_id = ${C}::uuid,
				waiting_step_key = 'wait-c'
			where id = ${P}::uuid
		`;

		await db.client.clearWaitingState({
			executionId: P,
			queue: "default",
			orchestratorId: ORCH_A,
			claimToken: tokenP,
		});

		const cancelled = { failed: true, cancelled: true };
		expect(await ending(db.sql, C)).toEqual(cancelled);
		expect((await getExecution(db.sql, C))?.last_error).toBe("Cancelled: parent timed out");
		expect(await ending(db.sql, G)).toEqual(cancelled);
		expect(await ending(db.sql, L)).toEqual(cancelled);
		expect(await getExecution(db.sql, P)).toMatchObject({
			waiting_on_execution_id: null,
			failed_at: null,
			cancelled: false,
		});
	});
});

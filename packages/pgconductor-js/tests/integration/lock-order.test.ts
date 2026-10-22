import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import type { Sql } from "postgres";
import { QueryBuilder } from "../../src/query-builder";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import {
	insertExecution,
	install,
	openSession,
	outcome,
	settle,
	waitUntilBlocked,
	type Session,
} from "../fixtures/sessions";

// Each test lets session B take the first lock of its real statement inside an open
// transaction, starts session A's real statement until Postgres reports it waiting on B, then
// runs B's real statement. Both must succeed; a lock-order inversion makes Postgres abort one
// of them with 40P01.
describe("lock order (Postgres integration)", () => {
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

	async function setup() {
		const db = await pool.child();
		databases.push(db);
		await install(db);
		const a = await openSession(db);
		const b = await openSession(db);
		sessions.push(a, b);
		return { db, a, b };
	}

	async function lockExecution(session: Session, id: string) {
		await session.sql`begin`;
		await session.sql`select 1 from pgconductor._private_executions where id = ${id}::uuid for update`;
	}

	// Runs A until it waits on B, then B's statement; returns how each ended
	async function race(
		db: TestDatabase,
		a: Session,
		b: Session,
		statementA: PromiseLike<unknown> | null,
		statementB: () => PromiseLike<unknown> | null,
	) {
		const doneA = outcome(statementA);
		await waitUntilBlocked(db.sql, a, b);
		const resultB = await outcome(statementB());
		await b.sql.unsafe(resultB === "ok" ? "commit" : "rollback");
		return { a: await doneA, b: resultB };
	}

	// P has linked child L (cancelWithParent); L is suspended in invokeAndWait on running C
	async function linkedChain(db: TestDatabase) {
		const tokenP = crypto.randomUUID();
		const tokenC = crypto.randomUUID();
		const P = await insertExecution(db.sql, {
			task_key: "parent",
			locked_by: ORCH_A,
			claim_token: tokenP,
		});
		const L = crypto.randomUUID();
		const C = await insertExecution(db.sql, {
			task_key: "child",
			parent_execution_id: L,
			locked_by: ORCH_B,
			claim_token: tokenC,
		});
		await insertExecution(db.sql, {
			id: L,
			task_key: "linked",
			parent_execution_id: P,
			cancel_with_parent: true,
			waiting_on_execution_id: C,
			waiting_step_key: "wait-c",
			suspended: true,
		});
		const failP = {
			execution_id: P,
			queue: "default",
			task_key: "parent",
			claim_token: tokenP,
			status: "permanently_failed",
			error: "parent failed",
		} as const;
		return { P, L, C, tokenC, failP };
	}

	test("a parent failure cascading to L and the child C it waits on vs completion of C waking L", async () => {
		const { db, a, b } = await setup();
		const { C, tokenC, failP } = await linkedChain(db);

		await lockExecution(b, C);
		const result = await race(db, a, b, settle(a.sql, ORCH_A, [failP]), () =>
			settle(b.sql, ORCH_B, [
				{
					execution_id: C,
					task_key: "child",
					claim_token: tokenC,
					status: "completed",
					result: { ok: true },
				},
			]),
		);
		expect(result).toEqual({ a: "ok", b: "ok" });
	});

	test("a parent failure cascading to L and the child C it waits on vs permanent failure of C failing L", async () => {
		const { db, a, b } = await setup();
		const { C, tokenC, failP } = await linkedChain(db);

		await lockExecution(b, C);
		const result = await race(db, a, b, settle(a.sql, ORCH_A, [failP]), () =>
			settle(b.sql, ORCH_B, [
				{
					execution_id: C,
					task_key: "child",
					claim_token: tokenC,
					status: "permanently_failed",
					error: "child failed",
				},
			]),
		);
		expect(result).toEqual({ a: "ok", b: "ok" });
	});

	test("cancel_execution of a parent waiting for the result of its linked child vs that child's result delivery", async () => {
		const { db, a, b } = await setup();
		const P = await insertExecution(db.sql, {
			task_key: "parent",
			waiting_step_key: "wait-c",
			suspended: true,
		});
		const tokenC = crypto.randomUUID();
		const C = await insertExecution(db.sql, {
			task_key: "linked",
			parent_execution_id: P,
			cancel_with_parent: true,
			locked_by: ORCH_B,
			claim_token: tokenC,
			result_waiters: [{ execution_id: P, queue: "default", step_key: "wait-c" }],
		});

		await lockExecution(b, C);
		const result = await race(
			db,
			a,
			b,
			a.sql`select pgconductor.cancel_execution(${P}::uuid)`,
			() =>
				settle(b.sql, ORCH_B, [
					{
						execution_id: C,
						task_key: "linked",
						claim_token: tokenC,
						status: "completed",
						result: { ok: true },
					},
				]),
		);
		expect(result).toEqual({ a: "ok", b: "ok" });
	});

	test("waitForAny registration over [X, Y] vs completion of Y waking X (X < Y)", async () => {
		const { db, a, b } = await setup();
		const [X, Y] = [crypto.randomUUID(), crypto.randomUUID()].sort() as [string, string];
		const tokenP = crypto.randomUUID();
		const tokenY = crypto.randomUUID();
		const P = await insertExecution(db.sql, {
			task_key: "racer",
			locked_by: ORCH_A,
			claim_token: tokenP,
		});
		await insertExecution(db.sql, {
			id: X,
			task_key: "parent",
			waiting_on_execution_id: Y,
			waiting_step_key: "wait-y",
			suspended: true,
		});
		await insertExecution(db.sql, {
			id: Y,
			task_key: "child",
			parent_execution_id: X,
			locked_by: ORCH_B,
			claim_token: tokenY,
		});
		const branches = [
			{ key: "x", stepKey: "race:x", executionId: X },
			{ key: "y", stepKey: "race:y", executionId: Y },
		];

		await lockExecution(b, Y);
		const register = a.sql`
			select pgconductor._private_register_event_race(
				${P}::uuid, 'default', 'racer', ${ORCH_A}::uuid, ${tokenP}::uuid, 'race',
				${a.sql.json(branches)}::jsonb, null
			)
		`;
		const result = await race(db, a, b, register, () =>
			settle(b.sql, ORCH_B, [
				{
					execution_id: Y,
					task_key: "child",
					claim_token: tokenY,
					status: "completed",
					result: { ok: true },
				},
			]),
		);
		expect(result).toEqual({ a: "ok", b: "ok" });
	});

	for (const queueName of ["dq", "src"]) {
		test(`drop_queue('dq') vs worker registration of ${queueName === "dq" ? "the same queue" : "a queue dead-lettering into it"}`, async () => {
			const { db, a, b } = await setup();
			const register = (sql: Sql) =>
				new QueryBuilder(sql).buildRegisterWorker({
					queueName,
					taskSpecs: [
						queueName === "dq"
							? { key: "dt", queue: "dq" }
							: { key: "st", queue: "src", deadLetterQueue: "dq", deadLetterTaskKey: "dt" },
					],
					cronSchedules: [],
					eventSubscriptions: [],
				});
			await new QueryBuilder(db.sql).buildRegisterWorker({
				queueName: "dq",
				taskSpecs: [{ key: "dt", queue: "dq" }],
				cronSchedules: [],
				eventSubscriptions: [],
			});
			await register(db.sql);

			// B: the registration holding its first lock, the rows of its queues
			await b.sql`begin`;
			await b.sql`
				select 1 from pgconductor._private_queues where name in ('dq', ${queueName})
				order by name for update
			`;

			const result = await race(db, a, b, a.sql`select pgconductor.drop_queue('dq')`, () =>
				register(b.sql),
			);
			expect(result).toEqual({ a: "ok", b: "ok" });
		});
	}
});

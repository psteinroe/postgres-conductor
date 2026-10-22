import postgres, { type ReservedSql, type Sql } from "postgres";
import { Conductor } from "../../src/conductor";
import type { ExecutionResult } from "../../src/database-client";
import { QueryBuilder } from "../../src/query-builder";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import type { TestDatabase } from "./test-database";
import { waitForCondition } from "../test-utils";

// Helpers for tests that interleave real statements of two sessions on executions in a given state

export const TASKS = ["parent", "linked", "child", "grandchild", "racer"] as const;

// Installs the schema and registers TASKS on the default queue without starting workers
export async function install(db: TestDatabase, removeOnFailDays: number | null = null) {
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema(TASKS.map((name) => defineTask({ name }))),
		context: {},
	});
	await conductor.ensureInstalled();
	await db.sql`insert into pgconductor._private_queues (name) values ('default') on conflict do nothing`;
	for (const key of TASKS) {
		await db.sql`
			insert into pgconductor._private_tasks (key, queue, max_attempts, remove_on_fail_days)
			values (${key}, 'default', 1, ${removeOnFailDays})
		`;
	}
}

export type Row = {
	id?: string;
	task_key: string;
	locked_by?: string;
	claim_token?: string;
	parent_execution_id?: string;
	cancel_with_parent?: boolean;
	waiting_on_execution_id?: string;
	waiting_step_key?: string;
	dedupe_key?: string;
	result_waiters?: { execution_id: string; queue: string; step_key: string }[];
	suspended?: boolean;
};

// Inserts an execution in the given state: claimed when locked_by is set, otherwise
// suspended (run_at infinity) when suspended is set, otherwise pending
export async function insertExecution(sql: Sql, row: Row): Promise<string> {
	const id = row.id || crypto.randomUUID();
	await sql`
		insert into pgconductor._private_executions (
			id, task_key, queue, locked_by, locked_at, claim_token, attempts, run_at,
			parent_execution_id, cancel_with_parent, waiting_on_execution_id, waiting_step_key,
			dedupe_key, result_waiters
		) values (
			${id}::uuid, ${row.task_key}, 'default', ${row.locked_by || null}::uuid,
			case when ${row.locked_by || null}::uuid is null then null else pgconductor._private_current_time() end,
			${row.claim_token || null}::uuid,
			case when ${row.locked_by || null}::uuid is null then 0 else 1 end,
			case when ${row.suspended || false}::boolean then 'infinity'::timestamptz
				else pgconductor._private_current_time() end,
			${row.parent_execution_id || null}::uuid, ${row.cancel_with_parent || false},
			${row.waiting_on_execution_id || null}::uuid, ${row.waiting_step_key || null},
			${row.dedupe_key || null},
			${row.result_waiters ? sql.json(row.result_waiters) : null}::jsonb
		)
	`;
	return id;
}

export type Execution = {
	id: string;
	task_key: string;
	locked_by: string | null;
	completed_at: Date | null;
	failed_at: Date | null;
	cancelled: boolean;
	last_error: string | null;
	waiting_on_execution_id: string | null;
	parent_execution_id: string | null;
};

export async function getExecution(sql: Sql, id: string): Promise<Execution | undefined> {
	const [row] = await sql<Execution[]>`
		select id, task_key, locked_by, completed_at, failed_at, cancelled, last_error,
			waiting_on_execution_id, parent_execution_id
		from pgconductor._private_executions where id = ${id}::uuid
	`;
	return row;
}

// "removed", or whether the execution failed and was cancelled
export async function ending(sql: Sql, id: string) {
	const execution = await getExecution(sql, id);
	return execution
		? { failed: execution.failed_at !== null, cancelled: execution.cancelled }
		: "removed";
}

// A dedicated backend, so a test controls its transactions and can name it as a lock holder
export type Session = { sql: ReservedSql; pid: number; end: () => Promise<void> };

export async function openSession(db: TestDatabase): Promise<Session> {
	const pool = postgres(db.url, { max: 1, onnotice: () => {} });
	const sql = await pool.reserve();
	const [{ pid }] = await sql<[{ pid: number }]>`select pg_backend_pid() as pid`;
	return {
		sql,
		pid,
		end: async () => {
			sql.release();
			await pool.end({ timeout: 1 });
		},
	};
}

// The settlement statement a worker of the default queue flushes, run on the given session
export function settle(sql: Sql, orchestratorId: string, results: ExecutionResult[]) {
	return new QueryBuilder(sql).buildReturnExecutions({
		orchestratorId,
		queueName: "default",
		results,
	});
}

// Barrier: resolves once `waiter` waits for a lock held by `holder`
export async function waitUntilBlocked(observer: Sql, waiter: Session, holder: Session) {
	await waitForCondition(async () => {
		const [{ blocked }] = await observer<[{ blocked: boolean }]>`
			select ${holder.pid}::int = any(pg_blocking_pids(${waiter.pid}::int)) as blocked
		`;
		return blocked;
	}, 10_000);
}

// "ok", or the SQLSTATE, message and context the statement failed with
export async function outcome(promise: PromiseLike<unknown> | null): Promise<string> {
	try {
		await promise;
		return "ok";
	} catch (error) {
		const err = error as Error & { code?: string; where?: string };
		const where = err.where ? ` [${err.where.replace(/\s+/g, " ")}]` : "";
		return `${err.code}: ${err.message}${where}`;
	}
}

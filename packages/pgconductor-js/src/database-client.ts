import postgres, { type PendingQuery, type Sql } from "postgres";
import * as assert from "./lib/assert";
import { waitFor } from "./lib/wait-for";
import type { Migration } from "./migration-store";

export type JsonValue =
	| string
	| number
	| boolean
	| null
	| Payload
	| JsonValue[];
export type Payload = { [key: string]: JsonValue };

export interface ExecutionSpec {
	task_key: string;
	payload?: Payload | null;
	run_at?: Date | null;
	dedupe_key?: string | null;
	cron_expression?: string | null;
	priority?: number | null;
	parent_execution_id?: string | null;
	parent_step_key?: string | null;
	parent_timeout_ms?: number | null;
}

export interface TaskSpec {
	key: string;
	queue?: string | null;
	maxAttempts?: number | null;
	window?: [string, string] | null;
}

export interface Execution {
	id: string;
	task_key: string;
	payload: Payload;
	waiting_on_execution_id: string | null;
	dedupe_key?: string | null;
	cron_expression?: string | null;
}

export interface ExecutionResult {
	execution_id: string;
	task_key: string;
	status: "completed" | "failed" | "released";
	result?: Payload;
	error?: string;
}

const RETRYABLE_SQLSTATE_CODES = new Set([
	"40001", // serialization_failure
	"40P01", // deadlock_detected
	"55P03", // lock_not_available
	"57P01", // admin_shutdown
	"57P02", // crash_shutdown
	"57P03", // cannot_connect_now
	"53300", // too_many_connections
	"08000", // connection_exception
	"08003", // connection_does_not_exist
	"08006", // connection_failure
	"08001", // sqlclient_unable_to_establish_sqlconnection
]);

const RETRYABLE_SYSTEM_ERROR_CODES = new Set([
	"ECONNRESET",
	"ECONNREFUSED",
	"EPIPE",
	"ETIMEDOUT",
	"EHOSTUNREACH",
	"ENETUNREACH",
	"CONNECTION_CLOSED",
	"CONNECTION_DESTROYED",
	"CONNECT_TIMEOUT",
	"ENOTFOUND",
	"EAI_AGAIN",
	"ECONNABORTED",
	"EPROTO",
	"ERR_TLS_HANDSHAKE_TIMEOUT",
	"ERR_TLS_CERT_ALTNAME_INVALID",
	"ERR_TLS_REQUIRED_SERVER_NAME",
	"ERR_TLS_CERT_SIGNATURE_ALGORITHM_UNSUPPORTED",
	"ERR_TLS_PSK_SET_IDENTIY_HINT_FAILED",
	"ERR_TLS_DH_PARAM_SIZE",
	"ERR_SSL_WRONG_VERSION_NUMBER",
]);

const MIN_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 60_000;
const BACKOFF_MULTIPLIER = 2;

type DatabaseClientOptions =
	| { connectionString: string; sql?: never }
	| { sql: Sql; connectionString?: never };

type QueryOptions = {
	label?: string;
	signal?: AbortSignal;
};

type ErrorWithOptionalCode = Error & { code?: string };

export class DatabaseClient {
	private readonly sql: Sql;
	private readonly ownsSql: boolean;

	constructor(options: DatabaseClientOptions) {
		if ("sql" in options && options.sql) {
			this.sql = options.sql;
			this.ownsSql = false;
		} else if ("connectionString" in options && options.connectionString) {
			this.sql = postgres(options.connectionString);
			this.ownsSql = true;
		} else {
			throw new Error(
				"DatabaseClient requires either a connectionString or an existing postgres instance",
			);
		}
	}

	async close(): Promise<void> {
		if (!this.ownsSql) return;
		await this.sql.end({ timeout: 0 });
	}

	private async query<T>(
		fn: (sql: Sql) => Promise<T>,
		options?: QueryOptions,
	): Promise<T> {
		let attempt = 0;
		while (true) {
			if (options?.signal?.aborted) {
				throw new Error("Operation aborted");
			}
			try {
				return await fn(this.sql);
			} catch (error) {
				const err = error as ErrorWithOptionalCode;
				if (!this.isRetryableError(err)) {
					throw err;
				}

				attempt += 1;
				const delay = Math.min(
					MAX_RETRY_DELAY_MS,
					MIN_RETRY_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, attempt - 1),
				);
				const label = options?.label ? ` (${options.label})` : "";
				console.warn(
					`Database transient error${label}: ${err.message}. Retrying in ${delay}ms (attempt ${attempt}).`,
				);
				await waitFor(delay, {
					jitter: delay / 2,
					signal: options?.signal,
				});
			}
		}
	}

	private isRetryableError(err: ErrorWithOptionalCode): boolean {
		const code = err.code;
		if (!code) {
			return false;
		}
		return (
			RETRYABLE_SQLSTATE_CODES.has(code) ||
			RETRYABLE_SYSTEM_ERROR_CODES.has(code)
		);
	}

	async orchestratorHeartbeat(
		orchestratorId: string,
		version: string,
		migrationNumber: number,
		signal: AbortSignal,
	): Promise<boolean> {
		return this.query(
			async (sql) => {
				const result = await sql<[{ shutdown_signal: boolean }]>`
					with latest as (
						select coalesce(max(version), -1) as db_version
						from pgconductor.schema_migrations
					), upsert as (
						insert into pgconductor.orchestrators as o (
							id,
							version,
							migration_number,
							last_heartbeat_at
						)
						values (
							${orchestratorId}::uuid,
							${version}::text,
							${migrationNumber}::integer,
							pgconductor.current_time()
						)
						on conflict (id)
						do update
						set
							last_heartbeat_at = pgconductor.current_time(),
							version = excluded.version,
							migration_number = excluded.migration_number
						returning shutdown_signal
					), marked as (
						update pgconductor.orchestrators
						set shutdown_signal = true
						where id = ${orchestratorId}::uuid
							and (select db_version from latest) > ${migrationNumber}::integer
						returning shutdown_signal
					)
					select coalesce(
						(select shutdown_signal from marked),
						case
							when (select db_version from latest) > ${migrationNumber}::integer then true
							else (select shutdown_signal from upsert)
						end
					) as shutdown_signal
				`;

				const row = result[0];

				assert.ok(row, "No result returned from orchestrators_heartbeat");

				return row.shutdown_signal;
			},
			{ label: "orchestratorHeartbeat", signal },
		);
	}

	async recoverStaleOrchestrators(
		maxAge: string,
		signal: AbortSignal,
	): Promise<void> {
		return this.query(
			async (sql) => {
				await sql`
					with expired as (
						delete from pgconductor.orchestrators o
						where o.last_heartbeat_at < pgconductor.current_time() - ${maxAge}::interval
						returning o.id
					)
					update pgconductor.executions e
					set
						locked_by = null,
						locked_at = null
					from expired
					where e.locked_by = expired.id
				`;
			},
			{ label: "recoverStaleOrchestrators", signal },
		);
	}

	async sweepOrchestrators(
		migrationNumber: number,
		signal: AbortSignal,
	): Promise<void> {
		return this.query(
			async (sql) => {
				await sql`
					update pgconductor.orchestrators
					set shutdown_signal = true
					where migration_number < ${migrationNumber}::integer
				`;
			},
			{ label: "sweepOrchestrators", signal },
		);
	}

	/**
	 * Get the currently installed migration version
	 * Returns -1 if schema doesn't exist (not installed)
	 * Returns the highest migration version from schema_migrations table
	 */
	async getInstalledMigrationNumber(signal: AbortSignal): Promise<number> {
		return this.query(
			async (sql) => {
				try {
					const result = await sql<{ version: number | null }[]>`
					select max(version) as version
					from pgconductor.schema_migrations
				`;
					return result[0]?.version || -1;
				} catch (err) {
					const pgErr = err as { code?: string };
					if (pgErr?.code === "42P01" || pgErr?.code === "3F000") {
						return -1;
					}
					throw err;
				}
			},
			{ label: "getInstalledVersion", signal },
		);
	}

	async applyMigration(
		migration: Migration,
		signal: AbortSignal,
	): Promise<"applied" | "busy"> {
		return this.query(
			async (sql) => {
				return sql.begin(async (tx) => {
					const lockResult = await tx<{ locked: boolean }[]>`
					select pg_try_advisory_xact_lock(hashtext('pgconductor:migrations')) as locked
				`;

					if (!lockResult[0]?.locked) {
						return "busy" as const;
					}

					const applied =
						migration.version > 0
							? (
									await tx<{ exists: boolean }[]>`
					select exists (
						select 1
						from pgconductor.schema_migrations
						where version = ${migration.version}
					) as exists
				`
								).shift()?.exists
							: false;

					if (applied) {
						return "applied" as const;
					}

					await tx.unsafe(migration.sql);
					await tx`
					insert into pgconductor.schema_migrations (version, name, breaking)
					values (${migration.version}, ${migration.name}, ${migration.breaking})
				`;

					return "applied" as const;
				});
			},
			{ label: "applyMigration", signal },
		);
	}

	async countActiveOrchestratorsBelow(
		version: number,
		signal: AbortSignal,
	): Promise<number> {
		return this.query(
			async (sql) => {
				try {
					const result = await sql<{ count: number }[]>`
					select count(*) as count
					from pgconductor.orchestrators
					where migration_number < ${version}::integer
					  and shutdown_signal = false
				`;
					return Number(result[0]?.count || 0);
				} catch (err) {
					const pgErr = err as { code?: string };
					if (pgErr?.code === "42P01") {
						return 0;
					}
					throw err;
				}
			},
			{ label: "countActiveOrchestratorsBelow", signal },
		);
	}

	async orchestratorShutdown(
		orchestratorId: string,
		signal: AbortSignal,
	): Promise<void> {
		return this.query(
			async (sql) => {
				await sql`
					with deleted as (
						delete from pgconductor.orchestrators
						where id = ${orchestratorId}::uuid
						returning id
					)
					update pgconductor.executions e
					set
						locked_by = null,
						locked_at = null
					from deleted
					where e.locked_by = deleted.id
				`;
			},
			{ label: "orchestratorShutdown", signal },
		);
	}

	async getExecutions(
		orchestratorId: string,
		queueName: string,
		batchSize: number,
		taskKeys: string[],
		taskMaxAttempts: Record<string, number>,
		signal: AbortSignal,
	): Promise<Execution[]> {
		return this.query(
			async (sql) => {
				const maxAttemptsCases = taskKeys.map((key) => {
					const attempts = taskMaxAttempts[key];
					assert.ok(attempts, `Missing max_attempts for task ${key}`);
					return sql`when e.task_key = ${key} then ${attempts}`;
				});

				return await sql<Execution[]>`
					with e as (
						select
							e.id,
							e.task_key
						from pgconductor.executions e
						where e.queue = ${queueName}::text
							and e.task_key = any(${taskKeys}::text[])
							and e.attempts < (case ${maxAttemptsCases} end)::integer
							and e.locked_at is null
							and e.run_at <= pgconductor.current_time()
						order by e.priority asc, e.run_at asc
						limit ${batchSize}::integer
						for update skip locked
					)

					update pgconductor.executions
					set
						attempts = executions.attempts + 1,
						locked_by = ${orchestratorId}::uuid,
						locked_at = pgconductor.current_time()
					from e
					where executions.id = e.id
					returning
						executions.id,
						executions.task_key,
						executions.payload,
						executions.waiting_on_execution_id,
						executions.dedupe_key,
						executions.cron_expression
				`;
			},
			{ label: "getExecutions", signal },
		);
	}

	async returnExecutions(
		results: ExecutionResult[],
		taskMaxAttempts: Record<string, number>,
		signal?: AbortSignal,
	): Promise<void> {
		if (results.length === 0) return;

		// Group by status for dynamic query building
		const completed = results.filter((r) => r.status === "completed");
		const failed = results.filter((r) => r.status === "failed");
		const released = results.filter((r) => r.status === "released");

		return this.query(
			async (sql) => {
				const ctes: PendingQuery<any>[] = [];

				// Build completed CTEs
				if (completed.length > 0) {
					const completedData = completed.map((r) => ({
						execution_id: r.execution_id,
						result: r.result || null,
					}));

					// Parse completed execution IDs and results from TypeScript
					ctes.push(sql`completed_results as (
						select
							(r->>'execution_id')::uuid as execution_id,
							(r->'result')::jsonb as result
						from jsonb_array_elements(${sql.json(completedData)}::jsonb) r
					)`);

					// Delete completed executions from active queue
					ctes.push(sql`completed as (
						delete from pgconductor.executions e
						using completed_results r
						where e.id = r.execution_id
						returning e.*
					)`);

					// Archive completed executions for history/audit
					ctes.push(sql`inserted_completed as (
						insert into pgconductor.completed_executions (
							id, task_key, dedupe_key, created_at, failed_at, completed_at,
							payload, run_at, locked_at, locked_by, attempts, last_error, priority, queue
						)
						select
							id, task_key, dedupe_key, created_at, failed_at,
							pgconductor.current_time(),
							payload, run_at, locked_at, locked_by, attempts, last_error, priority, queue
						from completed
						returning id
					)`);

					// If this was a child execution, save result as step in parent (ctx.invoke pattern)
					ctes.push(sql`parent_steps_completed as (
						insert into pgconductor.steps (execution_id, key, result)
						select
							parent_e.id,
							parent_e.waiting_step_key,
							r.result
						from completed_results r
						join pgconductor.executions parent_e on parent_e.waiting_on_execution_id = r.execution_id
						on conflict (execution_id, key) do nothing
						returning execution_id
					)`);

					// Wake up parent executions now that child is complete
					ctes.push(sql`updated_parents_completed as (
						update pgconductor.executions
						set
							run_at = pgconductor.current_time(),
							waiting_on_execution_id = null,
							waiting_step_key = null
						from parent_steps_completed
						where id = parent_steps_completed.execution_id
						returning id
					)`);
				}

				// Build failed CTEs
				if (failed.length > 0) {
					const failedData = failed.map((r) => ({
						execution_id: r.execution_id,
						error: r.error || "unknown error",
					}));

					// Build dynamic CASE statement only for tasks that actually failed
					const failedTaskKeys = [...new Set(failed.map((r) => r.task_key))];
					const maxAttemptsCases = failedTaskKeys.map((key) => {
						const attempts = taskMaxAttempts[key];
						assert.ok(attempts, `Missing max_attempts for task ${key}`);
						return sql`when e.task_key = ${key} then ${attempts}`;
					});

					// Parse failed execution IDs and errors from TypeScript
					ctes.push(sql`failed_results as (
						select
							(r->>'execution_id')::uuid as execution_id,
							r->>'error' as error
						from jsonb_array_elements(${sql.json(failedData)}::jsonb) r
					)`);

					// Find executions that exhausted all retries (attempts >= max_attempts)
					ctes.push(sql`permanently_failed_children as (
						select
							r.execution_id,
							r.error as child_error
						from failed_results r
						join pgconductor.executions e on e.id = r.execution_id
						where e.attempts >= (case ${maxAttemptsCases} end)::integer
					)`);

					// Delete permanently failed executions + cascade to parents (child failure fails parent)
					ctes.push(sql`deleted_failed as (
						delete from pgconductor.executions e
						where (
							e.id in (select execution_id from permanently_failed_children)
							or
							e.waiting_on_execution_id in (select execution_id from permanently_failed_children)
						)
						returning
							e.id, e.task_key, e.dedupe_key, e.created_at, e.payload,
							e.run_at, e.locked_at, e.locked_by, e.attempts, e.priority, e.queue,
							coalesce(
								(select error from failed_results where execution_id = e.id),
								(select 'Child execution failed: ' || coalesce(child_error, 'unknown error')
									from permanently_failed_children
									where execution_id = e.waiting_on_execution_id)
							) as last_error
					)`);

					// Schedule retry for failed executions that still have attempts left
					ctes.push(sql`retried as (
						update pgconductor.executions e
						set
							last_error = coalesce(r.error, 'unknown error'),
							run_at = greatest(pgconductor.current_time(), e.run_at)
								+ ((array[15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200])[least(e.attempts, 10)] * interval '1 second'),
							locked_by = null,
							locked_at = null
						from failed_results r
						where e.id = r.execution_id
							and e.attempts < (case ${maxAttemptsCases} end)::integer
						returning e.*
					)`);

					// Archive permanently failed executions for history/audit
					ctes.push(sql`inserted_failed as (
						insert into pgconductor.failed_executions (
							id, task_key, dedupe_key, created_at, failed_at, completed_at,
							payload, run_at, locked_at, locked_by, attempts, last_error, priority, queue
						)
						select
							id, task_key, dedupe_key, created_at,
							pgconductor.current_time(),
							null,
							payload, run_at, locked_at, locked_by, attempts, last_error, priority, queue
						from deleted_failed
						returning id
					)`);
				}

				// Build released CTEs
				if (released.length > 0) {
					const releasedIds = released.map((r) => r.execution_id);

					// Release execution back to queue (hangup case: sleep/invoke/checkpoint)
					ctes.push(sql`updated_released as (
						update pgconductor.executions
						set
							attempts = greatest(attempts - 1, 0),
							locked_by = null,
							locked_at = null
						where id = any(${sql.array(releasedIds)}::uuid[])
						returning id
					)`);
				}

				assert.ok(
					ctes.length,
					"At least one CTE should be present in returnExecutions",
				);

				const combined = ctes.reduce((acc, cte, i) =>
					i === 0 ? cte : sql`${acc}, ${cte}`,
				);

				await sql`with ${combined} select 1`;
			},
			{ label: "returnExecutions", signal },
		);
	}

	async registerWorker(
		queueName: string,
		taskSpecs: TaskSpec[],
		cronSchedules: ExecutionSpec[],
		signal?: AbortSignal,
	): Promise<void> {
		return this.query(
			async (sql) => {
				const taskSpecRows = taskSpecs.map((spec) => ({
					key: spec.key,
					queue: spec.queue || null,
					max_attempts: spec.maxAttempts || null,
					window_start: spec.window?.[0] || null,
					window_end: spec.window?.[1] || null,
				}));

				const cronScheduleRows = cronSchedules.map((spec) => {
					assert.ok(
						spec.cron_expression,
						"cron_expression is required for cron schedules",
					);

					return {
						task_key: spec.task_key,
						payload: spec.payload || null,
						run_at: spec.run_at,
						dedupe_key: spec.dedupe_key,
						cron_expression: spec.cron_expression,
					};
				});

				await sql`
					select pgconductor.register_worker(
						p_queue_name := ${queueName}::text,
						p_task_specs := array(
							select json_populate_recordset(null::pgconductor.task_spec, ${sql.json(taskSpecRows)}::json)::pgconductor.task_spec
						),
						p_cron_schedules := array(
							select json_populate_recordset(null::pgconductor.execution_spec, ${sql.json(cronScheduleRows)}::json)::pgconductor.execution_spec
						)
					)
				`;
			},
			{ label: "registerWorker", signal },
		);
	}

	async invoke(spec: ExecutionSpec, signal?: AbortSignal): Promise<void> {
		return this.query(
			async (sql) => {
				await sql<[{ id: string }]>`
				select pgconductor.invoke(
					task_key := ${spec.task_key}::text,
					payload := ${spec.payload ? sql.json(spec.payload) : null}::jsonb,
					run_at := ${spec.run_at ? spec.run_at.toISOString() : null}::timestamptz,
					dedupe_key := ${spec.dedupe_key || null}::text,
					cron_expression := ${spec.cron_expression || null}::text,
					priority := ${spec.priority || null}::integer,
					parent_execution_id := ${spec.parent_execution_id || null}::uuid,
					parent_step_key := ${spec.parent_step_key || null}::text,
					parent_timeout_ms := ${spec.parent_timeout_ms || null}::integer
				) as id
			`;
			},
			{ label: "invoke", signal },
		);
	}

	async invokeBatch(
		specs: ExecutionSpec[],
		signal?: AbortSignal,
	): Promise<void> {
		const specsArray = specs.map((spec) => ({
			task_key: spec.task_key,
			payload: spec.payload || null,
			run_at: spec.run_at,
			dedupe_key: spec.dedupe_key,
			cron_expression: spec.cron_expression || null,
			priority: spec.priority,
		}));

		return this.query(
			async (sql) => {
				await sql<{ id: string }[]>`
				select id from pgconductor.invoke(
					specs := array(
						select json_populate_recordset(null::pgconductor.execution_spec, ${sql.json(specsArray)})
					)
				)
			`;
			},
			{ label: "invokeBatch", signal },
		);
	}

	async loadStep(
		executionId: string,
		key: string,
		signal?: AbortSignal,
	): Promise<Payload | null | undefined> {
		return this.query(
			async (sql) => {
				const rows = await sql<[{ result: Payload | null }]>`
					select result from pgconductor.steps
					where execution_id = ${executionId}::uuid and key = ${key}::text
				`;

				if (!rows[0]) {
					return undefined;
				}
				return rows[0].result;
			},
			{ label: "loadStep", signal },
		);
	}

	async saveStep(
		executionId: string,
		key: string,
		result: Payload | null,
		runAtMs?: number,
		signal?: AbortSignal,
	): Promise<void> {
		return this.query(
			async (sql) => {
				if (runAtMs) {
					// Sleep case: insert step and update run_at
					await sql`
						with inserted as (
							insert into pgconductor.steps (execution_id, key, result)
							values (${executionId}::uuid, ${key}::text, ${sql.json(result)}::jsonb)
							on conflict (execution_id, key) do nothing
							returning id
						)
						update pgconductor.executions
						set run_at = pgconductor.current_time() + (${runAtMs}::integer || ' milliseconds')::interval
						where id = ${executionId}::uuid
							and exists (select 1 from inserted)
					`;
				} else {
					// Regular step: just insert
					await sql`
						insert into pgconductor.steps (execution_id, key, result)
						values (${executionId}::uuid, ${key}::text, ${sql.json(result)}::jsonb)
						on conflict (execution_id, key) do nothing
					`;
				}
			},
			{ label: "saveStep", signal },
		);
	}

	async clearWaitingState(
		executionId: string,
		signal?: AbortSignal,
	): Promise<void> {
		return this.query(
			async (sql) => {
				await sql`
					update pgconductor.executions
					set
						waiting_on_execution_id = null,
						waiting_step_key = null
					where id = ${executionId}::uuid
				`;
			},
			{ label: "clearWaitingState", signal },
		);
	}

	/**
	 * Set fake time for testing purposes.
	 * All calls to pgconductor.current_time() will return this value.
	 *
	 * IMPORTANT: Test database connection pool must have max: 1 for this to work.
	 */
	async setFakeTime(date: Date, signal?: AbortSignal): Promise<void> {
		return this.query(
			async (sql) => {
				await sql.unsafe(`set pgconductor.fake_now = '${date.toISOString()}'`);
			},
			{ label: "setFakeTime", signal },
		);
	}

	/**
	 * Clear fake time, returning to real clock_timestamp().
	 */
	async clearFakeTime(signal?: AbortSignal): Promise<void> {
		return this.query(
			async (sql) => {
				await sql.unsafe(`set pgconductor.fake_now = ''`);
			},
			{ label: "clearFakeTime", signal },
		);
	}
}

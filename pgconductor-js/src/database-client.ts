import postgres, { type PendingQuery, type Sql } from "postgres";
import { waitFor } from "./lib/wait-for";
import type { Migration } from "./migration-store";
import {
	QueryBuilder,
	type OrchestratorHeartbeatArgs,
	type RecoverStaleOrchestratorsArgs,
	type SweepOrchestratorsArgs,
	type OrchestratorShutdownArgs,
	type CountActiveOrchestratorsBelowArgs,
	type GetExecutionsArgs,
	type RemoveExecutionsArgs,
	type RegisterWorkerArgs,
	type ScheduleCronExecutionArgs,
	type UnscheduleCronExecutionArgs,
	type LoadStepArgs,
	type SaveStepArgs,
	type ClearWaitingStateArgs,
	type EmitEventArgs,
} from "./query-builder";

export type JsonValue = string | number | boolean | null | Payload | JsonValue[];
export type Payload = { [key: string]: JsonValue };

export interface ExecutionSpec {
	task_key: string;
	queue: string;
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
	removeOnCompleteDays?: number | null;
	removeOnFailDays?: number | null;
	window?: [string, string] | null;
}

export interface Execution {
	id: string;
	task_key: string;
	queue: string;
	payload: Payload;
	waiting_on_execution_id: string | null;
	waiting_step_key: string | null;
	cancelled: boolean;
	last_error: string | null;
	dedupe_key?: string | null;
	cron_expression?: string | null;
}

// todo: move all of this to query-builder too or create new types.ts file

export type ExecutionResult =
	| ExecutionCompleted
	| ExecutionFailed
	| ExecutionReleased
	| ExecutionPermamentlyFailed
	| ExecutionInvokeChild
	| ExecutionWaitForCustomEvent
	| ExecutionWaitForDatabaseEvent;

export type GroupedExecutionResults = {
	completed: ExecutionCompleted[];
	failed: (ExecutionFailed | ExecutionPermamentlyFailed)[];
	released: ExecutionReleased[];
	invokeChild: ExecutionInvokeChild[];
	waitForCustomEvent: ExecutionWaitForCustomEvent[];
	waitForDbEvent: ExecutionWaitForDatabaseEvent[];
	taskKeys: Set<string>;
};

export interface ExecutionCompleted {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "completed";
	result?: Payload;
}

export interface ExecutionFailed {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "failed";
	error: string;
}

export interface ExecutionReleased {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "released";
	reschedule_in_ms?: number | "infinity";
	step_key?: string;
}

export interface ExecutionPermamentlyFailed {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "permanently_failed";
	error: string;
}

export interface ExecutionInvokeChild {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "invoke_child";
	timeout_ms: number | "infinity";
	step_key: string;
	child_task_name: string;
	child_task_queue: string;
	child_payload: Payload | null;
}

export interface ExecutionWaitForDatabaseEvent {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "wait_for_db_event";
	timeout_ms: number | "infinity";
	step_key: string;
	schema_name: string;
	table_name: string;
	operation: "insert" | "update" | "delete";
	columns: string[] | undefined;
}

export interface ExecutionWaitForCustomEvent {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "wait_for_custom_event";
	timeout_ms: number | "infinity";
	step_key: string;
	event_key: string;
}

export interface EventSubscriptionSpec {
	task_key: string;
	queue: string;
	source: "event" | "db";
	event_key?: string;
	schema_name?: string;
	table_name?: string;
	operation?: string;
	columns?: string[];
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
};

type ErrorWithOptionalCode = Error & { code?: string };

export type SetFakeTimeArgs = {
	date: Date;
};

export class DatabaseClient {
	private readonly sql: Sql;
	private readonly builder: QueryBuilder;
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

		this.builder = new QueryBuilder(this.sql);
	}

	async close(): Promise<void> {
		if (!this.ownsSql) return;
		await this.sql.end({ timeout: 0 });
	}

	private async query<T>(
		queryOrCallback:
			| PendingQuery<T extends readonly postgres.MaybeRow[] ? T : never>
			| ((sql: Sql) => Promise<T>),
		options?: QueryOptions,
	): Promise<T> {
		let attempt = 0;
		while (true) {
			try {
				if (typeof queryOrCallback === "function") {
					return await queryOrCallback(this.sql);
				}
				return await queryOrCallback;
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
				});
			}
		}
	}

	private isRetryableError(err: ErrorWithOptionalCode): boolean {
		const code = err.code;
		if (!code) {
			return false;
		}
		return RETRYABLE_SQLSTATE_CODES.has(code) || RETRYABLE_SYSTEM_ERROR_CODES.has(code);
	}

	async orchestratorHeartbeat(args: OrchestratorHeartbeatArgs): Promise<
		{
			signal_type: string | null;
			signal_execution_id: string | null;
			signal_payload: Record<string, any> | null;
		}[]
	> {
		return this.query(this.builder.buildOrchestratorHeartbeat(args), {
			label: "orchestratorHeartbeat",
		});
	}

	async cancelExecution(executionId: string, options?: { reason?: string }): Promise<boolean> {
		const result = await this.query(
			this.sql<{ cancel_execution: boolean }[]>`
				select pgconductor.cancel_execution(
					${executionId}::uuid,
					${options?.reason || "Cancelled by user"}::text
				) as cancel_execution
			`,
			{ label: "cancelExecution" },
		);
		return result[0]?.cancel_execution || false;
	}

	async recoverStaleOrchestrators(args: RecoverStaleOrchestratorsArgs): Promise<void> {
		await this.query(this.builder.buildRecoverStaleOrchestrators(args), {
			label: "recoverStaleOrchestrators",
		});
	}

	async sweepOrchestrators(args: SweepOrchestratorsArgs): Promise<void> {
		await this.query(this.builder.buildSweepOrchestrators(args), {
			label: "sweepOrchestrators",
		});
	}

	/**
	 * Get the currently installed migration version
	 * Returns -1 if schema doesn't exist (not installed)
	 * Returns the highest migration version from schema_migrations table
	 */
	async getInstalledMigrationNumber(): Promise<number> {
		try {
			const result = await this.query(this.builder.buildGetInstalledMigrationNumber(), {
				label: "getInstalledVersion",
			});
			return result[0]?.version || -1;
		} catch (err) {
			const pgErr = err as { code?: string };
			if (pgErr?.code === "42P01" || pgErr?.code === "3F000") {
				return -1;
			}
			throw err;
		}
	}

	async applyMigration(migration: Migration): Promise<"applied" | "busy"> {
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
			{ label: "applyMigration" },
		);
	}

	async countActiveOrchestratorsBelow(args: CountActiveOrchestratorsBelowArgs): Promise<number> {
		try {
			const result = await this.query(this.builder.buildCountActiveOrchestratorsBelow(args), {
				label: "countActiveOrchestratorsBelow",
			});
			return Number(result[0]?.count || 0);
		} catch (err) {
			const pgErr = err as { code?: string };
			if (pgErr?.code === "42P01") {
				return 0;
			}
			throw err;
		}
	}

	async orchestratorShutdown(args: OrchestratorShutdownArgs): Promise<void> {
		await this.query(this.builder.buildOrchestratorShutdown(args), {
			label: "orchestratorShutdown",
		});
	}

	async cleanupTriggers(): Promise<void> {
		await this.query(this.builder.buildCleanupTriggers(), {
			label: "getExecutions",
		});
	}

	async getExecutions(args: GetExecutionsArgs): Promise<Execution[]> {
		return this.query(this.builder.buildGetExecutions(args), {
			label: "getExecutions",
		});
	}

	async returnExecutions(grouped: GroupedExecutionResults): Promise<void> {
		const query = this.builder.buildReturnExecutions(grouped);

		if (!query) {
			return;
		}

		await this.query(query, { label: "returnExecutions" });
	}

	async removeExecutions(args: RemoveExecutionsArgs): Promise<boolean> {
		const query = this.builder.buildRemoveExecutions(args);

		const result = await this.query(query, {
			label: "removeExecutions",
		});

		const deletedCount = result[0]?.deleted_count ?? 0;
		return deletedCount >= args.batchSize;
	}

	async registerWorker(args: RegisterWorkerArgs): Promise<void> {
		await this.query(this.builder.buildRegisterWorker(args), {
			label: "registerWorker",
		});
	}

	async scheduleCronExecution(args: ScheduleCronExecutionArgs): Promise<string> {
		const result = await this.query(this.builder.buildScheduleCronExecution(args), {
			label: "scheduleCronExecution",
		});
		return result[0]!.id;
	}

	async unscheduleCronExecution(args: UnscheduleCronExecutionArgs): Promise<void> {
		await this.query(this.builder.buildUnscheduleCronExecution(args), {
			label: "unscheduleCronExecution",
		});
	}

	async invoke(spec: ExecutionSpec): Promise<string> {
		const result = await this.query(this.builder.buildInvoke(spec), {
			label: "invoke",
		});
		return result[0]!.id;
	}

	async invokeBatch(specs: ExecutionSpec[]): Promise<string[]> {
		const result = await this.query(this.builder.buildInvokeBatch(specs), {
			label: "invokeBatch",
		});
		return result.map((r) => r.id);
	}

	async loadStep(args: LoadStepArgs): Promise<Payload | null | undefined> {
		const rows = await this.query(this.builder.buildLoadStep(args), {
			label: "loadStep",
		});

		if (!rows[0]) {
			return undefined;
		}
		return rows[0].result;
	}

	async saveStep(args: SaveStepArgs): Promise<void> {
		await this.query(this.builder.buildSaveStep(args), { label: "saveStep" });
	}

	async clearWaitingState(args: ClearWaitingStateArgs): Promise<void> {
		await this.query(this.builder.buildClearWaitingState(args), {
			label: "clearWaitingState",
		});
	}

	/**
	 * Set fake time for testing purposes.
	 * All calls to pgconductor.current_time() will return this value.
	 *
	 * IMPORTANT: Test database connection pool must have max: 1 for this to work.
	 */
	async setFakeTime({ date }: SetFakeTimeArgs): Promise<void> {
		await this.query(
			async (sql) => {
				await sql.unsafe(`set pgconductor.fake_now = '${date.toISOString()}'`);
			},
			{ label: "setFakeTime" },
		);
	}

	/**
	 * Clear fake time, returning to real clock_timestamp().
	 */
	async clearFakeTime(): Promise<void> {
		await this.query(
			async (sql) => {
				await sql.unsafe(`set pgconductor.fake_now = ''`);
			},
			{ label: "clearFakeTime" },
		);
	}

	/**
	 * Emit a custom event.
	 * Returns the event id.
	 */
	async emitEvent({ eventKey, payload }: EmitEventArgs): Promise<string> {
		const result = await this.query(
			this.sql`
				select pgconductor.emit_event(
					${eventKey},
					${this.sql.json(payload || null)}
				) as id
			`,
			{ label: "emitEvent" },
		);
		return result[0]!.id;
	}
}

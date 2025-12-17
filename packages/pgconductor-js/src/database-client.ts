import postgres, { type Sql } from "postgres";
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
	// type EmitEventArgs,
} from "./query-builder";
import { makeChildLogger, type Logger } from "./lib/logger";

export type JsonValue = string | number | boolean | null | Payload | JsonValue[];
export type Payload = { [key: string]: JsonValue };

export interface ExecutionSpec {
	task_key: string;
	queue: string;
	payload?: Payload | null;
	run_at?: Date | null;
	dedupe_key?: string | null;
	throttle?: { seconds: number } | null;
	debounce?: { seconds: number } | null;
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
	concurrency?: number | null;
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
	slot_group_number?: number | null;
}

// todo: move all of this to query-builder too or create new types.ts file

export type ExecutionResult =
	| ExecutionCompleted
	| ExecutionFailed
	| ExecutionReleased
	| ExecutionPermamentlyFailed
	| ExecutionInvokeChild;
// | ExecutionWaitForCustomEvent
// | ExecutionWaitForDatabaseEvent;

export type GroupedExecutionResults = {
	count: number;
	completed: ExecutionCompleted[];
	failed: (ExecutionFailed | ExecutionPermamentlyFailed)[];
	released: ExecutionReleased[];
	invokeChild: ExecutionInvokeChild[];
	// waitForCustomEvent: ExecutionWaitForCustomEvent[];
	// waitForDbEvent: ExecutionWaitForDatabaseEvent[];
	taskKeys: Set<string>;
};

export interface ExecutionCompleted {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "completed";
	result?: Payload;
	slot_group_number?: number | null;
}

export interface ExecutionFailed {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "failed";
	error: string;
	slot_group_number?: number | null;
}

export interface ExecutionReleased {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "released";
	reschedule_in_ms?: number | "infinity";
	step_key?: string;
	slot_group_number?: number | null;
}

export interface ExecutionPermamentlyFailed {
	execution_id: string;
	queue: string;
	task_key: string;
	status: "permanently_failed";
	error: string;
	slot_group_number?: number | null;
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
	slot_group_number?: number | null;
}

// export interface ExecutionWaitForDatabaseEvent {
// 	execution_id: string;
// 	queue: string;
// 	task_key: string;
// 	status: "wait_for_db_event";
// 	timeout_ms: number | "infinity";
// 	step_key: string;
// 	schema_name: string;
// 	table_name: string;
// 	operation: "insert" | "update" | "delete";
// 	columns: string[] | undefined;
// 	slot_group_number?: number | null;
// }

// export interface ExecutionWaitForCustomEvent {
// 	execution_id: string;
// 	queue: string;
// 	task_key: string;
// 	status: "wait_for_custom_event";
// 	timeout_ms: number | "infinity";
// 	step_key: string;
// 	event_key: string;
// 	slot_group_number?: number | null;
// }

// export interface EventSubscriptionSpec {
// 	task_key: string;
// 	queue: string;
// 	source: "event" | "db";
// 	event_key?: string;
// 	schema_name?: string;
// 	table_name?: string;
// 	operation?: string;
// 	columns?: string[];
// }

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
	| { connectionString: string; sql?: never; logger: Logger }
	| { sql: Sql; connectionString?: never; logger: Logger };

type QueryOptions = {
	label?: string;
	expectError?: boolean;
	signal?: AbortSignal;
};

type QueryMethodOptions = Pick<QueryOptions, "signal">;

type ErrorWithOptionalCode = Error & { code?: string };

export type SetFakeTimeArgs = {
	date: Date;
};

export class DatabaseClient {
	private readonly sql: Sql;
	private readonly builder: QueryBuilder;
	private readonly ownsSql: boolean;
	private readonly logger: Logger;

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

		this.logger = makeChildLogger(options.logger, {
			component: "DatabaseClient",
		});
		this.builder = new QueryBuilder(this.sql);
	}

	async close(): Promise<void> {
		if (!this.ownsSql) return;
		await this.sql.end({ timeout: 0 });
	}

	private async query<T>(callback: (sql: Sql) => Promise<T>, options?: QueryOptions): Promise<T> {
		const logger = makeChildLogger(this.logger, { label: options?.label });

		const label = options?.label ? ` (${options.label})` : "";
		let attempt = 0;
		while (true) {
			try {
				return await callback(this.sql);
			} catch (error) {
				const err = error as ErrorWithOptionalCode;
				if (!this.isRetryableError(err)) {
					if (!options?.expectError) {
						logger.error(`Non-retryable database error${label}: ${err.message}`);
					}
					throw err;
				}
				if (!options?.signal) {
					logger.warn(
						`Retryable database error, but no signal provided. Not retrying: ${err.message}`,
					);
					throw err;
				}

				if (options?.signal?.aborted) {
					logger.warn(`Aborting retries due to signal${label}`);
					throw err;
				}

				logger.warn(`Retryable database error${label}: ${err.message}`);

				attempt += 1;
				const delay = Math.min(
					MAX_RETRY_DELAY_MS,
					MIN_RETRY_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, attempt - 1),
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

	async orchestratorHeartbeat(
		args: OrchestratorHeartbeatArgs,
		opts?: QueryMethodOptions,
	): Promise<
		{
			signal_type: string | null;
			signal_execution_id: string | null;
			signal_payload: Record<string, any> | null;
		}[]
	> {
		return this.query(() => this.builder.buildOrchestratorHeartbeat(args), {
			label: "orchestratorHeartbeat",
			...opts,
		});
	}

	async cancelExecution(
		executionId: string,
		options?: { reason?: string } & QueryMethodOptions,
	): Promise<boolean> {
		const result = await this.query(
			(sql) =>
				sql<{ cancel_execution: boolean }[]>`
				select pgconductor.cancel_execution(
					${executionId}::uuid,
					${options?.reason || "Cancelled by user"}::text
				) as cancel_execution
			`,
			{ label: "cancelExecution", ...options },
		);
		return result[0]?.cancel_execution || false;
	}

	async getCurrentTime(options?: QueryMethodOptions): Promise<Date> {
		const result = await this.query(
			(sql) =>
				sql<{ now: Date }[]>`
				select pgconductor._private_current_time() as now
			`,
			{ label: "getCurrentTime", ...options },
		);
		const row = result[0];
		if (!row) {
			throw new Error("getCurrentTime returned no rows");
		}
		return row.now;
	}

	async recoverStaleOrchestrators(
		args: RecoverStaleOrchestratorsArgs,
		opts?: QueryMethodOptions,
	): Promise<void> {
		await this.query(() => this.builder.buildRecoverStaleOrchestrators(args), {
			label: "recoverStaleOrchestrators",
			...opts,
		});
	}

	async sweepOrchestrators(args: SweepOrchestratorsArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildSweepOrchestrators(args), {
			label: "sweepOrchestrators",
			...opts,
		});
	}

	/**
	 * Get the currently installed migration version
	 * Returns -1 if schema doesn't exist (not installed)
	 * Returns the highest migration version from schema_migrations table
	 */
	async getInstalledMigrationNumber(opts?: QueryMethodOptions): Promise<number> {
		try {
			const result = await this.query(() => this.builder.buildGetInstalledMigrationNumber(), {
				label: "buildGetInstalledMigrationNumber",
				expectError: true,
				...opts,
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

	async applyMigration(
		migration: Migration,
		opts?: QueryMethodOptions,
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
			{ label: "applyMigration", ...opts },
		);
	}

	async countActiveOrchestratorsBelow(
		args: CountActiveOrchestratorsBelowArgs,
		opts?: QueryMethodOptions,
	): Promise<number> {
		try {
			const result = await this.query(() => this.builder.buildCountActiveOrchestratorsBelow(args), {
				label: "countActiveOrchestratorsBelow",
				...opts,
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

	async orchestratorShutdown(
		args: OrchestratorShutdownArgs,
		opts?: QueryMethodOptions,
	): Promise<void> {
		await this.query(() => this.builder.buildOrchestratorShutdown(args), {
			label: "orchestratorShutdown",
			...opts,
		});
	}

	async getExecutions(args: GetExecutionsArgs, opts?: QueryMethodOptions): Promise<Execution[]> {
		return this.query(() => this.builder.buildGetExecutions(args), {
			label: "getExecutions",
			...opts,
		});
	}

	async returnExecutions(
		grouped: GroupedExecutionResults,
		opts?: QueryMethodOptions,
	): Promise<void> {
		const query = this.builder.buildReturnExecutions(grouped);

		if (!query) {
			return;
		}

		await this.query(() => query, { label: "returnExecutions", ...opts });
	}

	async removeExecutions(args: RemoveExecutionsArgs, opts?: QueryMethodOptions): Promise<boolean> {
		const result = await this.query(() => this.builder.buildRemoveExecutions(args), {
			label: "removeExecutions",
			...opts,
		});

		const deletedCount = result[0]?.deleted_count ?? 0;
		return deletedCount >= args.batchSize;
	}

	async registerWorker(args: RegisterWorkerArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildRegisterWorker(args), {
			label: "registerWorker",
			...opts,
		});
	}

	async scheduleCronExecution(
		args: ScheduleCronExecutionArgs,
		opts?: QueryMethodOptions,
	): Promise<string> {
		const result = await this.query(() => this.builder.buildScheduleCronExecution(args), {
			label: "scheduleCronExecution",
			...opts,
		});
		return result[0]!.id;
	}

	async unscheduleCronExecution(
		args: UnscheduleCronExecutionArgs,
		opts?: QueryMethodOptions,
	): Promise<void> {
		await this.query(() => this.builder.buildUnscheduleCronExecution(args), {
			label: "unscheduleCronExecution",
			...opts,
		});
	}

	async invoke(spec: ExecutionSpec, opts?: QueryMethodOptions): Promise<string | null> {
		const result = await this.query(() => this.builder.buildInvoke(spec), {
			label: "invoke",
			...opts,
		});
		return result[0]?.id || null;
	}

	async invokeBatch(specs: ExecutionSpec[], opts?: QueryMethodOptions): Promise<string[]> {
		const result = await this.query(() => this.builder.buildInvokeBatch(specs), {
			label: "invokeBatch",
			...opts,
		});
		return result.map((r) => r.id);
	}

	async loadStep(
		args: LoadStepArgs,
		opts?: QueryMethodOptions,
	): Promise<Payload | null | undefined> {
		const rows = await this.query(() => this.builder.buildLoadStep(args), {
			label: "loadStep",
			...opts,
		});

		if (!rows[0]) {
			return undefined;
		}
		return rows[0].result;
	}

	async saveStep(args: SaveStepArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildSaveStep(args), {
			label: "saveStep",
			...opts,
		});
	}

	async clearWaitingState(args: ClearWaitingStateArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildClearWaitingState(args), {
			label: "clearWaitingState",
			...opts,
		});
	}

	/**
	 * Set fake time for testing purposes.
	 * All calls to pgconductor._private_current_time() will return this value.
	 *
	 * IMPORTANT: Test database connection pool must have max: 1 for this to work.
	 */
	async setFakeTime({ date }: SetFakeTimeArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query((sql) => sql.unsafe(`set pgconductor.fake_now = '${date.toISOString()}'`), {
			label: "setFakeTime",
			...opts,
		});
	}

	/**
	 * Clear fake time, returning to real clock_timestamp().
	 */
	async clearFakeTime(opts?: QueryMethodOptions): Promise<void> {
		await this.query((sql) => sql.unsafe(`set pgconductor.fake_now = ''`), {
			label: "clearFakeTime",
			...opts,
		});
	}

	// /**
	//  * Emit a custom event.
	//  * Returns the event id.
	//  */
	// async emitEvent(
	// 	{ eventKey, payload }: EmitEventArgs,
	// 	opts?: QueryMethodOptions,
	// ): Promise<string> {
	// 	const result = await this.query(
	// 		(sql) =>
	// 			sql`
	// 			select pgconductor.emit_event(
	// 				${eventKey},
	// 				${sql.json(payload || null)}
	// 			) as id
	// 		`,
	// 		{ label: "emitEvent", ...opts },
	// 	);
	// 	return result[0]!.id;
	// }
}

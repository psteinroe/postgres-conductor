import postgres, { type PendingQuery, type Sql } from "postgres";
import * as assert from "./lib/assert";
import { waitFor } from "./lib/wait-for";
import type { Migration } from "./migration-store";
import type { TaskConfiguration } from "./task";
import { QueryBuilder } from "./query-builder";

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
			if (options?.signal?.aborted) {
				throw new Error("Operation aborted");
			}
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
		const result = await this.query(
			this.builder.buildOrchestratorHeartbeat(
				orchestratorId,
				version,
				migrationNumber,
			),
			{ label: "orchestratorHeartbeat", signal },
		);

		const row = result[0];
		assert.ok(row, "No result returned from orchestrators_heartbeat");

		return row.shutdown_signal;
	}

	async recoverStaleOrchestrators(
		maxAge: string,
		signal: AbortSignal,
	): Promise<void> {
		await this.query(this.builder.buildRecoverStaleOrchestrators(maxAge), {
			label: "recoverStaleOrchestrators",
			signal,
		});
	}

	async sweepOrchestrators(
		migrationNumber: number,
		signal: AbortSignal,
	): Promise<void> {
		await this.query(this.builder.buildSweepOrchestrators(migrationNumber), {
			label: "sweepOrchestrators",
			signal,
		});
	}

	/**
	 * Get the currently installed migration version
	 * Returns -1 if schema doesn't exist (not installed)
	 * Returns the highest migration version from schema_migrations table
	 */
	async getInstalledMigrationNumber(signal: AbortSignal): Promise<number> {
		try {
			const result = await this.query(
				this.builder.buildGetInstalledMigrationNumber(),
				{ label: "getInstalledVersion", signal },
			);
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
		try {
			const result = await this.query(
				this.builder.buildCountActiveOrchestratorsBelow(version),
				{ label: "countActiveOrchestratorsBelow", signal },
			);
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
		orchestratorId: string,
		signal: AbortSignal,
	): Promise<void> {
		await this.query(this.builder.buildOrchestratorShutdown(orchestratorId), {
			label: "orchestratorShutdown",
			signal,
		});
	}

	async getExecutions(
		orchestratorId: string,
		queueName: string,
		batchSize: number,
		filterTaskKeys: string[],
		signal: AbortSignal,
	): Promise<Execution[]> {
		return this.query(
			this.builder.buildGetExecutions(
				orchestratorId,
				queueName,
				batchSize,
				filterTaskKeys,
			),
			{ label: "getExecutions", signal },
		);
	}

	async returnExecutions(
		results: ExecutionResult[],
		signal?: AbortSignal,
	): Promise<void> {
		const query = this.builder.buildReturnExecutions(results);

		if (!query) return;

		await this.query(query, { label: "returnExecutions", signal });
	}

	async removeExecutions(
		queueName: string,
		batchSize: number,
		signal?: AbortSignal,
	): Promise<boolean> {
		const query = this.builder.buildRemoveExecutions(queueName, batchSize);

		const result = await this.query(query, {
			label: "removeExecutions",
			signal,
		});

		const deletedCount = result[0]?.deleted_count ?? 0;
		return deletedCount >= batchSize;
	}

	async registerWorker(
		queueName: string,
		taskSpecs: TaskSpec[],
		cronSchedules: ExecutionSpec[],
		signal?: AbortSignal,
	): Promise<void> {
		await this.query(
			this.builder.buildRegisterWorker(queueName, taskSpecs, cronSchedules),
			{ label: "registerWorker", signal },
		);
	}

	async invoke(spec: ExecutionSpec, signal?: AbortSignal): Promise<string> {
		const result = await this.query(this.builder.buildInvoke(spec), {
			label: "invoke",
			signal,
		});
		return result[0]!.id;
	}

	async invokeChild(
		spec: ExecutionSpec,
		signal?: AbortSignal,
	): Promise<string> {
		const result = await this.query(this.builder.buildInvokeChild(spec), {
			label: "invokeChild",
			signal,
		});
		return result[0]!.id;
	}

	async invokeBatch(
		specs: ExecutionSpec[],
		signal?: AbortSignal,
	): Promise<string[]> {
		const result = await this.query(this.builder.buildInvokeBatch(specs), {
			label: "invokeBatch",
			signal,
		});
		return result.map((r) => r.id);
	}

	async loadStep(
		executionId: string,
		key: string,
		signal?: AbortSignal,
	): Promise<Payload | null | undefined> {
		const rows = await this.query(
			this.builder.buildLoadStep(executionId, key),
			{ label: "loadStep", signal },
		);

		if (!rows[0]) {
			return undefined;
		}
		return rows[0].result;
	}

	async saveStep(
		executionId: string,
		queue: string,
		key: string,
		result: Payload | null,
		runAtMs?: number,
		signal?: AbortSignal,
	): Promise<void> {
		await this.query(
			this.builder.buildSaveStep(executionId, queue, key, result, runAtMs),
			{ label: "saveStep", signal },
		);
	}

	async clearWaitingState(
		executionId: string,
		signal?: AbortSignal,
	): Promise<void> {
		await this.query(this.builder.buildClearWaitingState(executionId), {
			label: "clearWaitingState",
			signal,
		});
	}

	/**
	 * Set fake time for testing purposes.
	 * All calls to pgconductor.current_time() will return this value.
	 *
	 * IMPORTANT: Test database connection pool must have max: 1 for this to work.
	 */
	async setFakeTime(date: Date, signal?: AbortSignal): Promise<void> {
		await this.query(
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
		await this.query(
			async (sql) => {
				await sql.unsafe(`set pgconductor.fake_now = ''`);
			},
			{ label: "clearFakeTime", signal },
		);
	}
}

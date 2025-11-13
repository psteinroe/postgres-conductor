import postgres, { type Sql } from "postgres";
import assert from "./lib/assert";
import { waitFor } from "./lib/wait-for";
import type { Migration } from "./migration-store";
import { PACKAGE_VERSION, MIGRATION_NUMBER } from "./versions";

type JsonValue = string | number | boolean | null | Payload | JsonValue[];
export type Payload = { [key: string]: JsonValue };

export interface ExecutionSpec {
	task_key: string;
	payload?: Payload | null;
	run_at?: Date | null;
	key?: string | null;
	priority?: number | null;
}

export interface Execution {
	id: string;
	payload: Payload;
}

export interface ExecutionResult {
	execution_id: string;
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
				SELECT pgconductor.orchestrators_heartbeat(
					v_orchestrator_id := ${orchestratorId}::uuid,
					v_version := ${version}::text,
					v_migration_number := ${migrationNumber}::integer
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
				SELECT pgconductor.recover_stale_orchestrators(
					v_max_age := ${maxAge}::interval
				)
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
				SELECT pgconductor.sweep_orchestrators(
					v_migration_number := ${migrationNumber}::integer
				)
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
					SELECT MAX(version) as version
					FROM pgconductor.schema_migrations
				`;
					return result[0]?.version ?? -1;
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
					SELECT pg_try_advisory_xact_lock(hashtext('pgconductor:migrations')) AS locked
				`;

					if (!lockResult[0]?.locked) {
						return "busy" as const;
					}

					const applied =
						migration.version > 0
							? (
									await tx<{ exists: boolean }[]>`
					SELECT EXISTS (
						SELECT 1
						FROM pgconductor.schema_migrations
						WHERE version = ${migration.version}
					) as exists
				`
								).shift()?.exists
							: false;

					if (applied) {
						return "applied" as const;
					}

					await tx.unsafe(migration.sql);
					await tx`
					INSERT INTO pgconductor.schema_migrations (version, name, breaking)
					VALUES (${migration.version}, ${migration.name}, ${migration.breaking})
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
					SELECT COUNT(*) as count
					FROM pgconductor.orchestrators
					WHERE migration_number < ${version}::integer
					  AND shutdown_signal = false
				`;
					return Number(result[0]?.count ?? 0);
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
				SELECT pgconductor.orchestrator_shutdown(
					v_orchestrator_id := ${orchestratorId}::uuid
				)
			`;
			},
			{ label: "orchestratorShutdown", signal },
		);
	}

	async getExecutions(
		orchestratorId: string,
		taskKey: string,
		batchSize: number,
		signal: AbortSignal,
	): Promise<Execution[]> {
		return this.query(
			async (sql) => {
				return await sql<Execution[]>`
				SELECT * FROM pgconductor.get_executions(
					v_task_key := ${taskKey}::text,
					v_orchestrator_id := ${orchestratorId}::uuid,
					v_batch_size := ${batchSize}::integer
				)
			`;
			},
			{ label: "getExecutions", signal },
		);
	}

	async returnExecutions(
		results: ExecutionResult[],
		signal: AbortSignal,
	): Promise<void> {
		return this.query(
			async (sql) => {
				const mappedResults = results.map((r) => ({
					execution_id: r.execution_id,
					status: r.status,
					result: r.result ?? null,
					error: r.error ?? null,
				}));

				await sql`
				SELECT pgconductor.return_executions(
					v_results := array(
						SELECT json_populate_recordset(null::pgconductor.execution_result, ${sql.json(mappedResults)})
					)
				)
			`;
			},
			{ label: "returnExecutions", signal },
		);
	}

	async invoke(spec: ExecutionSpec, signal?: AbortSignal): Promise<string> {
		return this.query(
			async (sql) => {
				const result = await sql<[{ id: string }]>`
				SELECT pgconductor.invoke(
					task_key := ${spec.task_key}::text,
					payload := ${spec.payload ? sql.json(spec.payload) : null}::jsonb,
					run_at := ${spec.run_at ? spec.run_at.toISOString() : null}::timestamptz,
					key := ${spec.key ?? null}::text,
					priority := ${spec.priority ?? null}::integer
				) as id
			`;

				return result[0].id;
			},
			{ label: "invoke", signal },
		);
	}

	async invokeBatch(
		specs: ExecutionSpec[],
		signal: AbortSignal,
	): Promise<string[]> {
		const specsArray = specs.map((spec) => ({
			task_key: spec.task_key,
			payload: spec.payload ?? null,
			run_at: spec.run_at,
			key: spec.key,
			priority: spec.priority,
		}));

		return this.query(
			async (sql) => {
				const result = await sql<{ id: string }[]>`
				SELECT id FROM pgconductor.invoke(
					specs := array(
						SELECT json_populate_recordset(null::pgconductor.execution_spec, ${sql.json(specsArray)})
					)
				)
			`;

				return result.map((r) => r.id);
			},
			{ label: "invokeBatch", signal },
		);
	}
}

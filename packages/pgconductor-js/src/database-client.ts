import { type Sql } from "postgres";
import assert from "./lib/assert";
import type { Migration } from "./migration-store";

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

export class DatabaseClient {
	constructor(
		private readonly orchestratorId: string,
		private readonly sql: Sql,
	) {}

	// todo make retry optional and auto retry on known transient errors only
	private async query<T>(
		fn: () => Promise<T>,
		options: { retries?: number; retryDelay?: number } = {},
	): Promise<T> {
		const { retries = 3, retryDelay = 100 } = options;
		let lastError: Error | undefined;

		for (let attempt = 0; attempt <= retries; attempt++) {
			try {
				return await fn();
			} catch (error) {
				lastError = error as Error;

				if (attempt === retries) break;

				// Exponential backoff
				const delay = retryDelay * Math.pow(2, attempt);
				await new Promise((resolve) => setTimeout(resolve, delay));
			}
		}

		throw lastError;
	}

	async orchestratorHeartbeat(
		orchestratorId: string,
		version: string,
		migrationNumber: number,
	): Promise<boolean> {
		return this.query(async () => {
			const result = await this.sql<[{ shutdown_signal: boolean }]>`
				SELECT pgconductor.orchestrators_heartbeat(
					v_orchestrator_id := ${orchestratorId}::uuid,
					v_version := ${version}::text,
					v_migration_number := ${migrationNumber}::integer
				) as shutdown_signal
			`;

			const row = result[0];

			assert.ok(row, "No result returned from orchestrators_heartbeat");

			return row.shutdown_signal;
		});
	}

	async recoverStaleOrchestrators(maxAge: string): Promise<void> {
		return this.query(async () => {
			await this.sql`
				SELECT pgconductor.recover_stale_orchestrators(
					v_max_age := ${maxAge}::interval
				)
			`;
		});
	}

	async sweepOrchestrators(migrationNumber: number): Promise<void> {
		return this.query(async () => {
			await this.sql`
				SELECT pgconductor.sweep_orchestrators(
					v_migration_number := ${migrationNumber}::integer
				)
			`;
		});
	}

	/**
	 * Get the currently installed migration version
	 * Returns -1 if schema doesn't exist (not installed)
	 * Returns the highest migration version from schema_migrations table
	 */
	async getInstalledVersion(): Promise<number> {
		return this.query(async () => {
			try {
				const result = await this.sql<{ version: number | null }[]>`
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
		});
	}

	async applyMigration(migration: Migration): Promise<"applied" | "busy"> {
		return this.query(async () => {
			return this.sql.begin(async (sql) => {
				const lockResult = await sql<{ locked: boolean }[]>`
					SELECT pg_try_advisory_xact_lock(hashtext('pgconductor:migrations')) AS locked
				`;

				if (!lockResult[0]?.locked) {
					return "busy" as const;
				}

				const alreadyApplied = await sql<{ exists: boolean }[]>`
					SELECT EXISTS (
						SELECT 1
						FROM pgconductor.schema_migrations
						WHERE version = ${migration.version}
					) as exists
				`;

				if (alreadyApplied[0]?.exists) {
					return "applied" as const;
				}

				await sql.unsafe(migration.sql);
				await sql`
					INSERT INTO pgconductor.schema_migrations (version, name, breaking)
					VALUES (${migration.version}, ${migration.name}, ${migration.breaking})
				`;

				return "applied" as const;
			});
		});
	}

	async countActiveOrchestratorsBelow(version: number): Promise<number> {
		return this.query(async () => {
			try {
				const result = await this.sql<{ count: number }[]>`
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
		});
	}

	async orchestratorShutdown(orchestratorId: string): Promise<void> {
		return this.query(async () => {
			await this.sql`
				SELECT pgconductor.orchestrator_shutdown(
					v_orchestrator_id := ${orchestratorId}::uuid
				)
			`;
		});
	}

	async getExecutions(
		taskKey: string,
		batchSize: number,
	): Promise<Execution[]> {
		return this.query(async () => {
			return await this.sql<Execution[]>`
				SELECT * FROM pgconductor.get_executions(
					v_task_key := ${taskKey}::text,
					v_orchestrator_id := ${this.orchestratorId}::uuid,
					v_batch_size := ${batchSize}::integer
				)
			`;
		});
	}

	async returnExecutions(results: ExecutionResult[]): Promise<void> {
		return this.query(async () => {
			const mappedResults = results.map((r) => ({
				execution_id: r.execution_id,
				status: r.status,
				result: r.result ?? null,
				error: r.error ?? null,
			}));

			await this.sql`
				SELECT pgconductor.return_executions(
					v_results := array(
						SELECT json_populate_recordset(null::pgconductor.execution_result, ${this.sql.json(mappedResults)})
					)
				)
			`;
		});
	}

	async invoke(
		taskKey: string,
		payload?: Payload | null,
		runAt?: Date | null,
		key?: string | null,
		priority?: number | null,
	): Promise<string> {
		return this.query(async () => {
			const result = await this.sql<[{ id: string }]>`
				SELECT pgconductor.invoke(
					task_key := ${taskKey}::text,
					payload := ${payload ? this.sql.json(payload) : null}::jsonb,
					run_at := ${runAt ? runAt.toISOString() : null}::timestamptz,
					key := ${key || null}::text,
					priority := ${priority || null}::integer
				) as id
			`;
			return result[0].id;
		});
	}

	async invokeBatch(specs: ExecutionSpec[]): Promise<string[]> {
		return this.query(async () => {
			const specsArray = specs.map((spec) => ({
				task_key: spec.task_key,
				payload: spec.payload ?? null,
				run_at: spec.run_at,
				key: spec.key,
				priority: spec.priority,
			}));

			const result = await this.sql<{ id: string }[]>`
				SELECT id FROM pgconductor.invoke(
					specs := array(
						SELECT json_populate_recordset(null::pgconductor.execution_spec, ${this.sql.json(specsArray)})
					)
				)
			`;

			return result.map((r) => r.id);
		});
	}
}

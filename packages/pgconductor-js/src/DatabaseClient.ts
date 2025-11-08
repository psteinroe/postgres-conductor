import { type Sql } from "postgres";

export interface ExecutionSpec {
	workflow_key: string;
	payload?: Record<string, unknown> | null;
	run_at?: Date | null;
	key?: string | null;
	priority?: number | null;
}

export interface Execution {
	id: string;
	payload: Record<string, unknown>;
}

export class DatabaseClient {
	constructor(private sql: Sql) {}

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

				// Don't retry on last attempt
				if (attempt === retries) break;

				// Exponential backoff
				const delay = retryDelay * Math.pow(2, attempt);
				await new Promise((resolve) => setTimeout(resolve, delay));
			}
		}

		throw lastError;
	}

	async workerHeartbeat(workerId: string): Promise<boolean> {
		return this.query(async () => {
			const result = await this.sql<[{ shutdown_signal: boolean }]>`
				SELECT pgconductor.worker_heartbeat(${workerId}::uuid) as shutdown_signal
			`;
			return result[0]?.shutdown_signal ?? false;
		});
	}

	async getExecutions(
		workflowKey: string,
		workerId: string,
		batchSize = 100,
	): Promise<Execution[]> {
		return this.query(async () => {
			return await this.sql<Execution[]>`
				SELECT * FROM pgconductor.get_executions(
					${workflowKey}::text,
					${workerId}::uuid,
					${batchSize}::integer
				)
			`;
		});
	}

	async completeExecutions(executionIds: string[]): Promise<void> {
		return this.query(async () => {
			await this.sql`
				SELECT pgconductor.complete_executions(${executionIds}::uuid[])
			`;
		});
	}

	async releaseExecutions(executionIds: string[]): Promise<void> {
		return this.query(async () => {
			await this.sql`
				SELECT pgconductor.release_executions(${executionIds}::uuid[])
			`;
		});
	}

	async failExecutions(executionIds: string[]): Promise<void> {
		return this.query(async () => {
			await this.sql`
				SELECT pgconductor.fail_executions(${executionIds}::uuid[])
			`;
		});
	}

	async invoke(
		workflowKey: string,
		payload?: Record<string, unknown> | null,
		runAt?: Date | null,
		key?: string | null,
		priority?: number | null,
	): Promise<string> {
		return this.query(async () => {
			const result = await this.sql<[{ id: string }]>`
				SELECT pgconductor.invoke(
					${workflowKey}::text,
					${payload ? this.sql.json(payload) : null}::jsonb,
					${runAt}::timestamptz,
					${key}::text,
					${priority}::integer
				) as id
			`;
			return result[0].id;
		});
	}

	async invokeBatch(specs: ExecutionSpec[]): Promise<string[]> {
		return this.query(async () => {
			// Convert specs to the composite type format
			const specsArray = specs.map((spec) => ({
				workflow_key: spec.workflow_key,
				payload: spec.payload ? this.sql.json(spec.payload) : null,
				run_at: spec.run_at,
				key: spec.key,
				priority: spec.priority,
			}));

			const result = await this.sql<{ id: string }[]>`
				SELECT id FROM pgconductor.invoke(${this.sql.array(specsArray, "execution_spec")}::pgconductor.execution_spec[])
			`;
			return result.map((r) => r.id);
		});
	}
}

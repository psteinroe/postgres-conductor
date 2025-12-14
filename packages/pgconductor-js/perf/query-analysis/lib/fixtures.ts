import type { TestDatabase } from "../../../tests/fixtures/test-database";
import { SchemaManager } from "../../../src/schema-manager";
import type { TaskSpec } from "../../../src/database-client";

export class Fixtures {
	private readonly schemaManager;

	constructor(private readonly db: TestDatabase) {
		this.schemaManager = new SchemaManager(db.client);
	}

	async install() {
		const controller = new AbortController();
		await this.schemaManager.ensureLatest(controller.signal);
	}

	async createTasks(specs: TaskSpec[]) {
		// Upsert queues first
		const queues = [...new Set(specs.map((s) => s.queue || "default"))];
		await this.db.sql`
			INSERT INTO pgconductor.queues (name)
			SELECT unnest(${queues}::text[])
			ON CONFLICT (name) DO NOTHING
		`;

		// Upsert tasks
		for (const s of specs) {
			const windowStart = s.window?.[0] || null;
			const windowEnd = s.window?.[1] || null;

			await this.db.sql`
				INSERT INTO pgconductor.tasks (key, queue, max_attempts, window_start, window_end)
				VALUES (
					${s.key},
					${s.queue || "default"},
					${s.maxAttempts || 3},
					${windowStart},
					${windowEnd}
				)
				ON CONFLICT (key) DO UPDATE SET
					queue = EXCLUDED.queue,
					max_attempts = EXCLUDED.max_attempts,
					window_start = EXCLUDED.window_start,
					window_end = EXCLUDED.window_end
			`;
		}
	}

	async createPendingExecutions(
		desired: {
			queue: string;
			task_key: string;
			count: number;
			steps?: number;
		}[],
	) {
		for (const spec of desired) {
			// Generate executions using generate_series for performance
			const executionIds = await this.db.sql<{ id: string }[]>`
				INSERT INTO pgconductor.executions (
					id,
					task_key,
					queue,
					payload,
					run_at,
					priority,
					attempts
				)
				SELECT
					pgconductor.portable_uuidv7(),
					${spec.task_key},
					${spec.queue},
					'{}'::jsonb,
					pgconductor.current_time(),
					0,
					0
				FROM generate_series(1, ${spec.count})
				RETURNING id
			`;

			// Create steps for each execution if requested
			if (spec.steps && spec.steps > 0) {
				for (const e of executionIds) {
					const stepRows = Array.from({ length: spec.steps }, (_, i) => ({
						execution_id: e.id,
						queue: spec.queue,
						key: `step_${i}`,
						result: { result: `value_${i}` },
					}));

					await this.db.sql`
						INSERT INTO pgconductor.steps (execution_id, queue, key, result)
						SELECT
							(r->>'execution_id')::uuid,
							r->>'queue',
							r->>'key',
							(r->'result')::jsonb
						FROM jsonb_array_elements(${this.db.sql.json(stepRows)}::jsonb) r
					`;
				}
			}
		}
	}

	async createCompletedExecutions(
		desired: {
			queue: string;
			task_key: string;
			count: number;
			steps?: number;
		}[],
	) {
		for (const spec of desired) {
			const executionIds = await this.db.sql<{ id: string }[]>`
				INSERT INTO pgconductor.executions (
					id,
					task_key,
					queue,
					payload,
					run_at,
					priority,
					attempts,
					completed_at
				)
				SELECT
					pgconductor.portable_uuidv7(),
					${spec.task_key},
					${spec.queue},
					'{}'::jsonb,
					pgconductor.current_time() - interval '1 hour',
					0,
					1,
					pgconductor.current_time()
				FROM generate_series(1, ${spec.count})
				RETURNING id
			`;

			// Create steps for completed executions
			if (spec.steps && spec.steps > 0 && executionIds.length > 0) {
				for (const e of executionIds) {
					const stepRows = Array.from({ length: spec.steps }, (_, i) => ({
						execution_id: e.id,
						queue: spec.queue,
						key: `step_${i}`,
						result: { result: `completed_value_${i}` },
					}));

					await this.db.sql`
						INSERT INTO pgconductor.steps (execution_id, queue, key, result)
						SELECT
							(r->>'execution_id')::uuid,
							r->>'queue',
							r->>'key',
							(r->'result')::jsonb
						FROM jsonb_array_elements(${this.db.sql.json(stepRows)}::jsonb) r
					`;
				}
			}
		}
	}

	async createFailedExecutions(
		desired: {
			queue: string;
			task_key: string;
			count: number;
			steps?: number;
		}[],
	) {
		for (const spec of desired) {
			const executionIds = await this.db.sql<{ id: string }[]>`
				INSERT INTO pgconductor.executions (
					id,
					task_key,
					queue,
					payload,
					attempts,
					last_error,
					failed_at,
					run_at,
					priority
				)
				SELECT
					pgconductor.portable_uuidv7(),
					${spec.task_key},
					${spec.queue},
					'{}'::jsonb,
					3,
					'Benchmark fixture error',
					pgconductor.current_time(),
					pgconductor.current_time() - interval '1 hour',
					0
				FROM generate_series(1, ${spec.count})
				RETURNING id
			`;

			// Create steps for failed executions
			if (spec.steps && spec.steps > 0 && executionIds.length > 0) {
				for (const e of executionIds) {
					const stepRows = Array.from({ length: spec.steps }, (_, i) => ({
						execution_id: e.id,
						queue: spec.queue,
						key: `step_${i}`,
						result: { result: `failed_value_${i}` },
					}));

					await this.db.sql`
						INSERT INTO pgconductor.steps (execution_id, queue, key, result)
						SELECT
							(r->>'execution_id')::uuid,
							r->>'queue',
							r->>'key',
							(r->'result')::jsonb
						FROM jsonb_array_elements(${this.db.sql.json(stepRows)}::jsonb) r
					`;
				}
			}
		}
	}

	async clear() {
		await this.db.sql`DELETE FROM pgconductor.steps`;
		await this.db.sql`DELETE FROM pgconductor.executions`;
		await this.db.sql`DELETE FROM pgconductor.tasks`;
		await this.db.sql`DELETE FROM pgconductor.queues`;
	}
}

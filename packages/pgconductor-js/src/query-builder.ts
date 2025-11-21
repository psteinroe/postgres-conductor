import type { PendingQuery, Row, RowList, Sql } from "postgres";
import * as assert from "./lib/assert";
import type {
	Execution,
	ExecutionResult,
	ExecutionSpec,
	Payload,
	TaskSpec,
} from "./database-client";

export class QueryBuilder {
	constructor(private readonly sql: Sql) {}

	buildOrchestratorHeartbeat(
		orchestratorId: string,
		version: string,
		migrationNumber: number,
	): PendingQuery<[{ shutdown_signal: boolean }]> {
		return this.sql<[{ shutdown_signal: boolean }]>`
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
	}

	buildRecoverStaleOrchestrators(maxAge: string): PendingQuery<RowList<Row[]>> {
		return this.sql`
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
	}

	buildSweepOrchestrators(
		migrationNumber: number,
	): PendingQuery<RowList<Row[]>> {
		return this.sql`
			update pgconductor.orchestrators
			set shutdown_signal = true
			where migration_number < ${migrationNumber}::integer
		`;
	}

	buildGetInstalledMigrationNumber(): PendingQuery<
		{ version: number | null }[]
	> {
		return this.sql<{ version: number | null }[]>`
			select max(version) as version
			from pgconductor.schema_migrations
		`;
	}

	buildOrchestratorShutdown(
		orchestratorId: string,
	): PendingQuery<RowList<Row[]>> {
		return this.sql`
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
	}

	buildGetExecutions(
		orchestratorId: string,
		queueName: string,
		batchSize: number,
		filterTaskKeys: string[] | null,
	): PendingQuery<Execution[] | any> {
		return this.sql<Execution[]>`
			with e as (
				select
					e.id,
					e.task_key
				from pgconductor.executions e
				where e.queue = ${queueName}::text
                    ${filterTaskKeys && filterTaskKeys.length > 0 ? this.sql`and e.task_key != any(${this.sql.array(filterTaskKeys)}::text[])` : this.sql``}
					and e.run_at <= pgconductor.current_time()
                    and e.is_available = true
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
				and executions.queue = ${queueName}::text
			returning
				executions.id,
				executions.task_key,
				executions.queue,
				executions.payload,
				executions.waiting_on_execution_id,
				executions.dedupe_key,
				executions.cron_expression
		`;
	}

	buildReturnExecutions(
		results: ExecutionResult[],
		explain?: boolean,
	): PendingQuery<any> | null {
		if (results.length === 0) return null;

		const completed = results.filter((r) => r.status === "completed");
		const failed = results.filter((r) => r.status === "failed");
		const released = results.filter((r) => r.status === "released");

		const ctes: PendingQuery<any>[] = [];

		// Precompute timestamp once
		ctes.push(this.sql`now_ts as (select pgconductor.current_time() as ts)`);

		// Load task configs once for all task_keys we're processing
		const allTaskKeys = [...new Set(results.map((r) => r.task_key))];
		ctes.push(this.sql`task_configs as (
			select key, max_attempts, remove_on_complete_days, remove_on_fail_days
			from pgconductor.tasks
			where key = any(${this.sql.array(allTaskKeys)}::text[])
		)`);

		// Completed results
		if (completed.length > 0) {
			const completedData = completed.map((r) => ({
				execution_id: r.execution_id,
				task_key: r.task_key,
				result: r.result || null,
			}));

			ctes.push(this.sql`completed_results as (
				select * from jsonb_to_recordset(${this.sql.json(completedData)}::jsonb)
				as x(execution_id uuid, task_key text, result jsonb)
			)`);

			// Insert parent steps for all completed
			ctes.push(this.sql`parent_steps_all as (
				insert into pgconductor.steps (execution_id, queue, key, result)
				select
					parent_e.id,
					parent_e.queue,
					parent_e.waiting_step_key,
					r.result
				from completed_results r
				join pgconductor.executions parent_e on parent_e.waiting_on_execution_id = r.execution_id
				on conflict (execution_id, key) do nothing
			)`);

			// Update parents for all completed
			ctes.push(this.sql`updated_parents_all as (
				update pgconductor.executions e
				set
					run_at = nt.ts,
					waiting_on_execution_id = null,
					waiting_step_key = null
				from now_ts nt, completed_results r
				where e.waiting_on_execution_id = r.execution_id
			)`);

			// Delete completed where remove_on_complete_days = 0
			ctes.push(this.sql`deleted_completed as (
				delete from pgconductor.executions e
				using completed_results r, task_configs tc
				where e.id = r.execution_id
					and tc.key = r.task_key
					and tc.remove_on_complete_days = 0
			)`);

			// Update completed where remove_on_complete_days != 0 (keep)
			ctes.push(this.sql`updated_completed as (
				update pgconductor.executions e
				set
					completed_at = nt.ts,
					locked_by = null,
					locked_at = null
				from now_ts nt, completed_results r, task_configs tc
				where e.id = r.execution_id
					and tc.key = r.task_key
					and (tc.remove_on_complete_days is null or tc.remove_on_complete_days != 0)
			)`);
		}

		// Failed results
		if (failed.length > 0) {
			const failedData = failed.map((r) => ({
				execution_id: r.execution_id,
				task_key: r.task_key,
				error: r.error || "unknown error",
			}));

			ctes.push(this.sql`failed_results as (
				select * from jsonb_to_recordset(${this.sql.json(failedData)}::jsonb)
				as x(execution_id uuid, task_key text, error text)
			)`);

			// Permanently failed children (attempts >= max_attempts)
			ctes.push(this.sql`permanently_failed_children as (
				select
					r.execution_id,
					r.task_key,
					r.error as child_error,
					tc.remove_on_fail_days = 0 as should_remove
				from failed_results r, pgconductor.executions e, task_configs tc
				where e.id = r.execution_id
					and tc.key = r.task_key
					and e.attempts >= tc.max_attempts
			)`);

			// Delete permanently failed children and their parents
			ctes.push(this.sql`deleted_failed as (
				delete from pgconductor.executions e
				using permanently_failed_children p
				where
					(e.id = p.execution_id and p.should_remove)
					or (
						e.waiting_on_execution_id = p.execution_id
						and exists (
							select 1 from pgconductor.tasks t
							where t.key = e.task_key and t.remove_on_fail_days = 0
						)
					)
			)`);

			// Failed updates (permanently failed children + parents)
			ctes.push(this.sql`failed_updates as (
				select
					e.id as target_id,
					p.child_error as error,
					true as is_child
				from permanently_failed_children p
				join pgconductor.executions e on e.id = p.execution_id
				where p.should_remove = false
				union all
				select
					e.id as target_id,
					p.child_error as error,
					false as is_child
				from permanently_failed_children p
				join pgconductor.executions e on e.waiting_on_execution_id = p.execution_id
				where not exists (
					select 1 from pgconductor.tasks t
					where t.key = e.task_key and t.remove_on_fail_days = 0
				)
			)`);

			// Update all permanently failed
			ctes.push(this.sql`updated_failed_all as (
				update pgconductor.executions e
				set
					failed_at = nt.ts,
					last_error = case
						when f.is_child then coalesce(f.error, 'unknown error')
						else 'Child execution failed: ' || coalesce(f.error, 'unknown error')
					end,
					waiting_on_execution_id = null,
					waiting_step_key = null,
					locked_by = null,
					locked_at = null
				from now_ts nt, failed_updates f
				where e.id = f.target_id
			)`);

			// Retry failed (not permanently failed)
			ctes.push(this.sql`retried as (
				update pgconductor.executions e
				set
					last_error = coalesce(r.error, 'unknown error'),
					run_at = greatest(nt.ts, coalesce(e.run_at, nt.ts))
						+ ((array[15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200])[least(greatest(e.attempts, 1), 10)] * interval '1 second'),
					locked_by = null,
					locked_at = null
				from now_ts nt, failed_results r, task_configs tc
				where e.id = r.execution_id
					and tc.key = r.task_key
					and e.attempts < tc.max_attempts
			)`);
		}

		// Released
		if (released.length > 0) {
			const releasedIds = released.map((r) => r.execution_id);

			ctes.push(this.sql`updated_released as (
				update pgconductor.executions
				set
					attempts = greatest(attempts - 1, 0),
					locked_by = null,
					locked_at = null
				where id = any(${this.sql.array(releasedIds)}::uuid[])
			)`);
		}

		if (ctes.length <= 1) return null; // Only now_ts

		const combined = ctes.reduce((acc, cte, i) =>
			i === 0 ? cte : this.sql`${acc}, ${cte}`,
		);

		if (explain) {
			return this
				.sql`explain (analyze, costs, verbose, buffers, format json) with ${combined} select 1 as result`;
		}

		return this.sql<[{ result: number }]>`with ${combined} select 1 as result`;
	}

	buildRemoveExecutions(
		queueName: string,
		batchSize: number,
	): PendingQuery<{ deleted_count: number }[]> {
		return this.sql<{ deleted_count: number }[]>`
			with batch as (
				select e.id
				from pgconductor.executions e
				join pgconductor.tasks t on t.key = e.task_key
				where e.queue = ${queueName}
					and (
						(e.completed_at is not null and t.remove_on_complete_days > 0 and e.completed_at < pgconductor.current_time() - t.remove_on_complete_days * interval '1 day')
						or
						(e.failed_at is not null and t.remove_on_fail_days > 0 and e.failed_at < pgconductor.current_time() - t.remove_on_fail_days * interval '1 day')
					)
				limit ${batchSize}
			),
			deleted as (
				delete from pgconductor.executions
				using batch
				where executions.id = batch.id
				returning 1
			)
			select count(*)::int as deleted_count from deleted
		`;
	}

	buildRegisterWorker(
		queueName: string,
		taskSpecs: TaskSpec[],
		cronSchedules: ExecutionSpec[],
	): PendingQuery<RowList<Row[]>> {
		const taskSpecRows = taskSpecs.map((spec) => ({
			key: spec.key,
			queue: spec.queue || null,
			max_attempts: spec.maxAttempts || null,
			remove_on_complete_days: spec.removeOnCompleteDays ?? null,
			remove_on_fail_days: spec.removeOnFailDays ?? null,
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
				queue: spec.queue,
				payload: spec.payload || null,
				run_at: spec.run_at,
				dedupe_key: spec.dedupe_key,
				cron_expression: spec.cron_expression,
				priority: spec.priority || null,
			};
		});

		return this.sql`
			select pgconductor.register_worker(
				p_queue_name := ${queueName}::text,
				p_task_specs := array(
					select json_populate_recordset(null::pgconductor.task_spec, ${this.sql.json(taskSpecRows)}::json)::pgconductor.task_spec
				),
				p_cron_schedules := array(
					select json_populate_recordset(null::pgconductor.execution_spec, ${this.sql.json(cronScheduleRows)}::json)::pgconductor.execution_spec
				)
			)
		`;
	}

	buildInvoke(spec: ExecutionSpec): PendingQuery<[{ id: string }]> {
		return this.sql<[{ id: string }]>`
			select id from pgconductor.invoke(
				task_key := ${spec.task_key}::text,
				queue := ${spec.queue}::text,
				payload := ${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
				run_at := ${spec.run_at ? spec.run_at.toISOString() : null}::timestamptz,
				dedupe_key := ${spec.dedupe_key || null}::text,
				cron_expression := ${spec.cron_expression || null}::text,
				priority := ${spec.priority || null}::integer
			)
		`;
	}

	buildInvokeChild(spec: ExecutionSpec): PendingQuery<[{ id: string }]> {
		return this.sql<[{ id: string }]>`
			select pgconductor.invoke_child(
				task_key := ${spec.task_key}::text,
				queue := ${spec.queue}::text,
				payload := ${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
				run_at := ${spec.run_at ? spec.run_at.toISOString() : null}::timestamptz,
				dedupe_key := ${spec.dedupe_key || null}::text,
				cron_expression := ${spec.cron_expression || null}::text,
				priority := ${spec.priority || null}::integer,
				parent_execution_id := ${spec.parent_execution_id || null}::uuid,
				parent_step_key := ${spec.parent_step_key || null}::text,
				parent_timeout_ms := ${spec.parent_timeout_ms || null}::integer
			) as id
		`;
	}

	buildInvokeBatch(specs: ExecutionSpec[]): PendingQuery<{ id: string }[]> {
		const specsArray = specs.map((spec) => ({
			task_key: spec.task_key,
			queue: spec.queue,
			payload: spec.payload || null,
			run_at: spec.run_at,
			dedupe_key: spec.dedupe_key,
			cron_expression: spec.cron_expression || null,
			priority: spec.priority,
		}));

		return this.sql<{ id: string }[]>`
			select id from pgconductor.invoke_batch(
				array(
					select json_populate_recordset(null::pgconductor.execution_spec, ${this.sql.json(specsArray)})
				)
			)
		`;
	}

	buildLoadStep(
		executionId: string,
		key: string,
	): PendingQuery<[{ result: Payload | null }]> {
		return this.sql<[{ result: Payload | null }]>`
			select result from pgconductor.steps
			where execution_id = ${executionId}::uuid and key = ${key}::text
		`;
	}

	buildSaveStep(
		executionId: string,
		queue: string,
		key: string,
		result: Payload | null,
		runAtMs?: number,
	): PendingQuery<RowList<Row[]>> {
		if (runAtMs) {
			return this.sql`
				with inserted as (
					insert into pgconductor.steps (execution_id, queue, key, result)
					values (${executionId}::uuid, ${queue}::text, ${key}::text, ${this.sql.json(result)}::jsonb)
					on conflict (execution_id, key) do nothing
					returning id
				)
				update pgconductor.executions
				set run_at = pgconductor.current_time() + (${runAtMs}::integer || ' milliseconds')::interval
				where id = ${executionId}::uuid
                    and queue = ${queue}::text
					and exists (select 1 from inserted)
			`;
		}

		return this.sql`
			insert into pgconductor.steps (execution_id, queue, key, result)
			values (${executionId}::uuid, ${queue}::text, ${key}::text, ${this.sql.json(result)}::jsonb)
			on conflict (execution_id, key) do nothing
		`;
	}

	buildClearWaitingState(executionId: string): PendingQuery<RowList<Row[]>> {
		return this.sql`
			update pgconductor.executions
			set
				waiting_on_execution_id = null,
				waiting_step_key = null
			where id = ${executionId}::uuid
		`;
	}

	buildCountActiveOrchestratorsBelow(
		version: number,
	): PendingQuery<{ count: number }[]> {
		return this.sql<{ count: number }[]>`
			select count(*) as count
			from pgconductor.orchestrators
			where migration_number < ${version}::integer
			  and shutdown_signal = false
		`;
	}
}

import type { PendingQuery, Row, RowList, Sql } from "postgres";
import * as assert from "./lib/assert";
import type { TaskConfiguration } from "./task";
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
		taskKeys: string[],
		taskMaxAttempts: Record<string, number>,
	): PendingQuery<Execution[]> {
		const maxAttemptsCases = taskKeys.map((key) => {
			const attempts = taskMaxAttempts[key];
			assert.ok(attempts, `Missing max_attempts for task ${key}`);
			return this.sql`when e.task_key = ${key} then ${attempts}`;
		});

		return this.sql<Execution[]>`
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
					and e.completed_at is null
					and e.failed_at is null
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
				executions.queue,
				executions.payload,
				executions.waiting_on_execution_id,
				executions.dedupe_key,
				executions.cron_expression
		`;
	}

	buildReturnExecutions(
		results: ExecutionResult[],
		taskMaxAttempts: Record<string, number>,
		taskRemoveOnComplete: Record<string, boolean>,
		taskRemoveOnFail: Record<string, boolean>,
	): PendingQuery<[{ result: number }]> | null {
		if (results.length === 0) return null;

		const completed = results.filter((r) => r.status === "completed");
		const completedToDelete = completed.filter(
			(r) => taskRemoveOnComplete[r.task_key] === true,
		);
		const completedToKeep = completed.filter(
			(r) => taskRemoveOnComplete[r.task_key] !== true,
		);

		const failed = results.filter((r) => r.status === "failed");
		const released = results.filter((r) => r.status === "released");

		const ctes: PendingQuery<any>[] = [];

		if (completedToDelete.length > 0) {
			const completedData = completedToDelete.map((r) => ({
				execution_id: r.execution_id,
				result: r.result || null,
			}));

			ctes.push(this.sql`completed_delete_results as (
				select
					(r->>'execution_id')::uuid as execution_id,
					(r->'result')::jsonb as result
				from jsonb_array_elements(${this.sql.json(completedData)}::jsonb) r
			)`);

			ctes.push(this.sql`parent_steps_completed_delete as (
				insert into pgconductor.steps (execution_id, queue, key, result)
				select
					parent_e.id,
					parent_e.queue,
					parent_e.waiting_step_key,
					r.result
				from completed_delete_results r
				join pgconductor.executions parent_e on parent_e.waiting_on_execution_id = r.execution_id
				on conflict (execution_id, key) do nothing
				returning execution_id
			)`);

			ctes.push(this.sql`updated_parents_completed_delete as (
				update pgconductor.executions
				set
					run_at = pgconductor.current_time(),
					waiting_on_execution_id = null,
					waiting_step_key = null
				from parent_steps_completed_delete
				where id = parent_steps_completed_delete.execution_id
				returning id
			)`);

			ctes.push(this.sql`deleted_completed as (
				delete from pgconductor.executions
				where id in (select execution_id from completed_delete_results)
				returning id
			)`);
		}

		if (completedToKeep.length > 0) {
			const completedData = completedToKeep.map((r) => ({
				execution_id: r.execution_id,
				result: r.result || null,
			}));

			ctes.push(this.sql`completed_keep_results as (
				select
					(r->>'execution_id')::uuid as execution_id,
					(r->'result')::jsonb as result
				from jsonb_array_elements(${this.sql.json(completedData)}::jsonb) r
			)`);

			ctes.push(this.sql`updated_completed as (
				update pgconductor.executions e
				set
					completed_at = pgconductor.current_time(),
					locked_by = null,
					locked_at = null
				from completed_keep_results r
				where e.id = r.execution_id
				returning e.id
			)`);

			ctes.push(this.sql`parent_steps_completed_keep as (
				insert into pgconductor.steps (execution_id, queue, key, result)
				select
					parent_e.id,
					parent_e.queue,
					parent_e.waiting_step_key,
					r.result
				from completed_keep_results r
				join pgconductor.executions parent_e on parent_e.waiting_on_execution_id = r.execution_id
				on conflict (execution_id, key) do nothing
				returning execution_id
			)`);

			ctes.push(this.sql`updated_parents_completed_keep as (
				update pgconductor.executions
				set
					run_at = pgconductor.current_time(),
					waiting_on_execution_id = null,
					waiting_step_key = null
				from parent_steps_completed_keep
				where id = parent_steps_completed_keep.execution_id
				returning id
			)`);
		}

		const allTaskKeys = Object.keys(taskRemoveOnFail);
		const allRemoveOnFailCases = allTaskKeys.map((key) => {
			const remove = taskRemoveOnFail[key] === true;
			return this.sql`when e.task_key = ${key} then ${remove}`;
		});

		if (failed.length > 0) {
			const failedData = failed.map((r) => ({
				execution_id: r.execution_id,
				error: r.error || "unknown error",
			}));

			const failedTaskKeys = [...new Set(failed.map((r) => r.task_key))];
			const maxAttemptsCases = failedTaskKeys.map((key) => {
				const attempts = taskMaxAttempts[key];
				assert.ok(attempts, `Missing max_attempts for task ${key}`);
				return this.sql`when e.task_key = ${key} then ${attempts}`;
			});

			const removeOnFailCases = failedTaskKeys.map((key) => {
				const remove = taskRemoveOnFail[key] === true;
				return this.sql`when e.task_key = ${key} then ${remove}`;
			});

			ctes.push(this.sql`failed_results as (
				select
					(r->>'execution_id')::uuid as execution_id,
					r->>'error' as error
				from jsonb_array_elements(${this.sql.json(failedData)}::jsonb) r
			)`);

			ctes.push(this.sql`permanently_failed_children as (
				select
					r.execution_id,
					r.error as child_error,
					(case ${removeOnFailCases} end)::boolean as should_remove
				from failed_results r
				join pgconductor.executions e on e.id = r.execution_id
				where e.attempts >= (case ${maxAttemptsCases} end)::integer
			)`);

			ctes.push(this.sql`deleted_failed as (
				delete from pgconductor.executions e
				where (
					e.id in (select execution_id from permanently_failed_children where should_remove)
					or
					(
						e.waiting_on_execution_id in (select execution_id from permanently_failed_children)
						and (case ${allRemoveOnFailCases} else false end)::boolean = true
					)
				)
				returning
					e.id, e.task_key, e.last_error,
					coalesce(
						(select error from failed_results where execution_id = e.id),
						(select 'Child execution failed: ' || coalesce(child_error, 'unknown error')
							from permanently_failed_children
							where execution_id = e.waiting_on_execution_id)
					) as error
			)`);

			ctes.push(this.sql`updated_failed as (
				update pgconductor.executions e
				set
					failed_at = pgconductor.current_time(),
					last_error = coalesce(r.error, 'unknown error'),
					locked_by = null,
					locked_at = null
				from permanently_failed_children p, failed_results r
				where e.id = p.execution_id
					and e.id = r.execution_id
					and p.should_remove = false
				returning e.id
			)`);

			ctes.push(this.sql`updated_failed_parents as (
				update pgconductor.executions e
				set
					failed_at = pgconductor.current_time(),
					last_error = 'Child execution failed: ' || coalesce(p.child_error, 'unknown error'),
					waiting_on_execution_id = null,
					waiting_step_key = null,
					locked_by = null,
					locked_at = null
				from permanently_failed_children p
				where e.waiting_on_execution_id = p.execution_id
					and (case ${allRemoveOnFailCases} else false end)::boolean = false
				returning e.id
			)`);

			ctes.push(this.sql`retried as (
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
				returning e.id
			)`);
		}

		if (released.length > 0) {
			const releasedIds = released.map((r) => r.execution_id);

			ctes.push(this.sql`updated_released as (
				update pgconductor.executions
				set
					attempts = greatest(attempts - 1, 0),
					locked_by = null,
					locked_at = null
				where id = any(${this.sql.array(releasedIds)}::uuid[])
				returning id
			)`);
		}

		if (ctes.length === 0) return null;

		const combined = ctes.reduce((acc, cte, i) =>
			i === 0 ? cte : this.sql`${acc}, ${cte}`,
		);

		return this.sql<[{ result: number }]>`with ${combined} select 1 as result`;
	}

	buildRemoveExecutions(
		queueName: string,
		tasks: Pick<
			TaskConfiguration,
			"name" | "removeOnComplete" | "removeOnFail"
		>[],
		batchSize: number,
	): PendingQuery<{ deleted_count: number }[]> | null {
		const tasksWithRetention = tasks.filter(
			(t) =>
				(typeof t.removeOnComplete === "object" && t.removeOnComplete.days) ||
				(typeof t.removeOnFail === "object" && t.removeOnFail.days),
		);

		if (tasksWithRetention.length === 0) {
			return null;
		}

		const conditions = tasksWithRetention.flatMap((task) => {
			const clauses = [];
			if (
				typeof task.removeOnComplete === "object" &&
				task.removeOnComplete.days
			) {
				clauses.push(
					this
						.sql`(task_key = ${task.name} and completed_at is not null and completed_at < pgconductor.current_time() - ${task.removeOnComplete.days} * interval '1 day')`,
				);
			}
			if (typeof task.removeOnFail === "object" && task.removeOnFail.days) {
				clauses.push(
					this
						.sql`(task_key = ${task.name} and failed_at is not null and failed_at < pgconductor.current_time() - ${task.removeOnFail.days} * interval '1 day')`,
				);
			}
			return clauses;
		});

		if (conditions.length === 0) {
			return null;
		}

		const combinedConditions = conditions.reduce((acc, cond, i) =>
			i === 0 ? cond : this.sql`${acc} or ${cond}`,
		);

		return this.sql<{ deleted_count: number }[]>`
			with batch as (
				select id from pgconductor.executions
				where queue = ${queueName}
					and (${combinedConditions})
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
			select pgconductor.invoke(
				task_key := ${spec.task_key}::text,
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
			payload: spec.payload || null,
			run_at: spec.run_at,
			dedupe_key: spec.dedupe_key,
			cron_expression: spec.cron_expression || null,
			priority: spec.priority,
		}));

		return this.sql<{ id: string }[]>`
			select id from pgconductor.invoke(
				specs := array(
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

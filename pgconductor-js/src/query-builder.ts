import type { PendingQuery, Row, RowList, Sql } from "postgres";
import type { GroupedExecutionResults } from "./database-client";
import * as assert from "./lib/assert";
import type {
	Execution,
	ExecutionSpec,
	EventSubscriptionSpec,
	Payload,
	TaskSpec,
	JsonValue,
} from "./database-client";

export type OrchestratorHeartbeatArgs = {
	orchestratorId: string;
	version: string;
	migrationNumber: number;
};

export type RecoverStaleOrchestratorsArgs = {
	maxAge: string;
};

export type SweepOrchestratorsArgs = {
	migrationNumber: number;
};

export type OrchestratorShutdownArgs = {
	orchestratorId: string;
};

export type CountActiveOrchestratorsBelowArgs = {
	version: number;
};

export type GetExecutionsArgs = {
	orchestratorId: string;
	queueName: string;
	batchSize: number;
	filterTaskKeys: string[];
};

export type RemoveExecutionsArgs = {
	queueName: string;
	batchSize: number;
};

export type RegisterWorkerArgs = {
	queueName: string;
	taskSpecs: TaskSpec[];
	cronSchedules: ExecutionSpec[];
	eventSubscriptions: EventSubscriptionSpec[];
};

export type ScheduleCronExecutionArgs = {
	spec: ExecutionSpec;
	scheduleName: string;
};

export type UnscheduleCronExecutionArgs = {
	taskKey: string;
	queue: string;
	scheduleName: string;
};

export type LoadStepArgs = {
	executionId: string;
	key: string;
};

export type SaveStepArgs = {
	executionId: string;
	queue: string;
	key: string;
	result: Payload | null;
	runAtMs?: number;
};

export type ClearWaitingStateArgs = {
	executionId: string;
};

export type EmitEventArgs = {
	eventKey: string;
	payload?: JsonValue;
};

export class QueryBuilder {
	constructor(private readonly sql: Sql) {}

	buildOrchestratorHeartbeat({
		orchestratorId,
		version,
		migrationNumber,
	}: OrchestratorHeartbeatArgs): PendingQuery<
		{
			signal_type: string | null;
			signal_execution_id: string | null;
			signal_payload: Record<string, any> | null;
		}[]
	> {
		return this.sql<
			{
				signal_type: string | null;
				signal_execution_id: string | null;
				signal_payload: Record<string, any> | null;
			}[]
		>`
			with latest as (
				select coalesce(max(version), -1) as db_version
				from pgconductor.schema_migrations
			),
			-- Insert/update orchestrator record
			upserted_orchestrator as (
				insert into pgconductor.orchestrators as o (
					id,
					version,
					migration_number,
					last_heartbeat_at
				)
				select
					${orchestratorId}::uuid,
					${version}::text,
					${migrationNumber}::integer,
					pgconductor.current_time()
				on conflict (id)
				do update
				set
					last_heartbeat_at = pgconductor.current_time(),
					version = excluded.version,
					migration_number = excluded.migration_number
				returning id
			),
			-- Signal shutdown if newer migration exists
			shutdown_signal_inserted as (
				insert into pgconductor.orchestrator_signals (orchestrator_id, type, payload)
				select
					${orchestratorId}::uuid,
					'shutdown',
					jsonb_build_object('reason', 'newer_migration_detected')
				from latest
				where latest.db_version > ${migrationNumber}::integer
				on conflict (orchestrator_id) where type = 'shutdown' do nothing
			),
			-- Read and delete all signals for this orchestrator (ordered by creation)
			deleted_signals as (
				delete from pgconductor.orchestrator_signals
				where orchestrator_id = ${orchestratorId}::uuid
				returning type, execution_id, payload, created_at
			)
			select
				type as signal_type,
				execution_id as signal_execution_id,
				payload as signal_payload
			from deleted_signals
			order by created_at asc
		`;
	}

	buildRecoverStaleOrchestrators({
		maxAge,
	}: RecoverStaleOrchestratorsArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			with expired as (
				delete from pgconductor.orchestrators o
				where o.last_heartbeat_at < pgconductor.current_time() - ${maxAge}::interval
				returning o.id
			),
			-- fail cancelled executions from expired orchestrators
			failed_cancelled as (
				update pgconductor.executions e
				set
					failed_at = pgconductor.current_time(),
					locked_by = null,
					locked_at = null
				from expired
				where e.locked_by = expired.id
					and e.cancelled = true
					and e.failed_at is null
					and e.completed_at is null
			)
			-- unlock remaining (non-cancelled) executions
			update pgconductor.executions e
			set
				locked_by = null,
				locked_at = null
			from expired
			where e.locked_by = expired.id
				and e.cancelled = false
		`;
	}

	buildCleanupTriggers(): PendingQuery<RowList<Row[]>> {
		return this.sql`
            delete from pgconductor.triggers t
            where not exists (
                select 1
                from pgconductor.subscriptions s
                where s.schema_name = t.schema_name
                  and s.table_name = t.table_name
                  and s.operation = t.operation
            )
		`;
	}

	buildSweepOrchestrators({
		migrationNumber,
	}: SweepOrchestratorsArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			insert into pgconductor.orchestrator_signals (orchestrator_id, type, payload)
			select
				id,
				'shutdown',
				jsonb_build_object('reason', 'breaking_migration')
			from pgconductor.orchestrators
			where migration_number < ${migrationNumber}::integer
			on conflict (orchestrator_id) where type = 'shutdown' do nothing
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

	buildOrchestratorShutdown({
		orchestratorId,
	}: OrchestratorShutdownArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			with deleted as (
				delete from pgconductor.orchestrators
				where id = ${orchestratorId}::uuid
				returning id
			),
			-- fail cancelled executions from this orchestrator
			failed_cancelled as (
				update pgconductor.executions e
				set
					failed_at = pgconductor.current_time(),
					locked_by = null,
					locked_at = null
				from deleted
				where e.locked_by = deleted.id
					and e.cancelled = true
					and e.failed_at is null
					and e.completed_at is null
			)
			-- unlock remaining (non-cancelled) executions
			update pgconductor.executions e
			set
				locked_by = null,
				locked_at = null
			from deleted
			where e.locked_by = deleted.id
				and e.cancelled = false
		`;
	}

	buildGetExecutions({
		orchestratorId,
		queueName,
		batchSize,
		filterTaskKeys,
	}: GetExecutionsArgs): PendingQuery<Execution[] | any> {
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
				executions.waiting_step_key,
				executions.cancelled,
				executions.last_error,
				executions.dedupe_key,
				executions.cron_expression
		`;
	}

	buildReturnExecutions(
		grouped: GroupedExecutionResults,
	): PendingQuery<any> | null {
		const completed = grouped.completed;
		const failed = grouped.failed;
		const released = grouped.released;
		const invokeChild = grouped.invokeChild;
		const waitForCustomEvent = grouped.waitForCustomEvent;
		const waitForDbEvent = grouped.waitForDbEvent;

		if (
			completed.length === 0 &&
			failed.length === 0 &&
			released.length === 0 &&
			invokeChild.length === 0 &&
			waitForCustomEvent.length === 0 &&
			waitForDbEvent.length === 0
		) {
			return null;
		}

		const ctes: PendingQuery<any>[] = [];

		// Precompute timestamp once
		ctes.push(this.sql`now_ts as (select pgconductor.current_time() as ts)`);

		// Load task configs once for all task_keys we're processing
		ctes.push(this.sql`task_configs as (
			select key, max_attempts, remove_on_complete_days, remove_on_fail_days
			from pgconductor.tasks
			where key = any(${this.sql.array(Array.from(grouped.taskKeys))}::text[])
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
				returning execution_id
			)`);

			// Mark orphaned children (completed but no parent waiting) as failed
			ctes.push(this.sql`orphaned_children as (
				update pgconductor.executions e
				set
					failed_at = nt.ts,
					completed_at = null,
					last_error = 'Parent timed out before child completed',
					locked_by = null,
					locked_at = null
				from now_ts nt, completed_results r
				where e.id = r.execution_id
					-- No parent is waiting for this child
					and not exists (
						select 1 from pgconductor.executions parent
						where parent.waiting_on_execution_id = r.execution_id
					)
				returning e.id
			)`);

			// Update parents for all completed
			ctes.push(this.sql`updated_parents_all as (
				update pgconductor.executions e
				set
					run_at = nt.ts,
					waiting_on_execution_id = null,
					waiting_step_key = null,
					locked_by = null,
					locked_at = null
				from now_ts nt, completed_results r
				where e.waiting_on_execution_id = r.execution_id
			)`);

			// Delete completed where remove_on_complete_days = 0 (excluding orphaned)
			ctes.push(this.sql`deleted_completed as (
				delete from pgconductor.executions e
				using completed_results r, task_configs tc
				where e.id = r.execution_id
					and tc.key = r.task_key
					and tc.remove_on_complete_days = 0
					and not exists (select 1 from orphaned_children oc where oc.id = e.id)
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
					and not exists (select 1 from orphaned_children oc where oc.id = e.id)
			)`);
		}

		// Failed results
		if (failed.length > 0) {
			ctes.push(this.sql`failed_results as (
				select * from jsonb_to_recordset(${this.sql.json(failed as unknown as JsonValue)}::jsonb)
				as x(execution_id uuid, task_key text, status text, error text)
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
					and (e.attempts >= tc.max_attempts or r.status = 'permanently_failed')
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
			// Save steps for released executions with step_key (e.g., sleep)
			const releasedWithSteps = released.filter(
				(r) => r.step_key !== undefined,
			);
			if (releasedWithSteps.length > 0) {
				ctes.push(this.sql`released_steps as (
					insert into pgconductor.steps (execution_id, queue, key, result)
					select
						r.execution_id,
						r.queue,
						r.step_key,
						null::jsonb
					from jsonb_to_recordset(${this.sql.json(releasedWithSteps as unknown as JsonValue)}::jsonb)
						as r(execution_id uuid, queue text, step_key text)
					on conflict (execution_id, key) do nothing
					returning id
				)`);
			}

			const shouldRescheduleSome = released.some(
				(r) => r.reschedule_in_ms !== undefined,
			);

			if (shouldRescheduleSome) {
				const releasedData = released.map((r) => ({
					execution_id: r.execution_id,
					reschedule_in_ms:
						r.reschedule_in_ms === "infinity" ? -1 : r.reschedule_in_ms,
				}));

				ctes.push(this.sql`updated_released as (
                update pgconductor.executions e
                set
                    attempts = greatest(attempts - 1, 0),
                    run_at = case
                        when r.reschedule_in_ms = -1 then
                            'infinity'::timestamptz
                        when r.reschedule_in_ms is not null then
                            nt.ts + (r.reschedule_in_ms::integer || ' milliseconds')::interval
                        else
                            nt.ts
                    end,
                    locked_by = null,
                    locked_at = null
                from now_ts nt, jsonb_to_recordset(${this.sql.json(releasedData)}::jsonb)
                    as r(execution_id uuid, reschedule_in_ms integer)
                where e.id = r.execution_id
            )`);
			} else {
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
		}

		if (invokeChild.length > 0) {
			ctes.push(this.sql`invoke_child_data as (
				select * from jsonb_to_recordset(${this.sql.json(invokeChild as unknown as JsonValue)}::jsonb)
				as x(
					execution_id uuid,
					task_key text,
					queue text,
					step_key text,
					timeout_ms text,
					child_task_name text,
					child_task_queue text,
					child_payload jsonb
				)
			)`);

			// Insert child executions with parent reference
			ctes.push(this.sql`inserted_children as (
				insert into pgconductor.executions (
					id,
					task_key,
					queue,
					payload,
					run_at,
					parent_execution_id
				)
				select
					pgconductor.portable_uuidv7(),
					icd.child_task_name,
					icd.child_task_queue,
					icd.child_payload,
					nt.ts,
					icd.execution_id
				from invoke_child_data icd, now_ts nt
				returning id, parent_execution_id
			)`);

			// Update parent executions
			ctes.push(this.sql`updated_invoke_parents as (
				update pgconductor.executions e
				set
					waiting_on_execution_id = ic.id,
					waiting_step_key = icd.step_key,
					run_at = case
						when icd.timeout_ms = 'infinity' then 'infinity'::timestamptz
						else nt.ts + (icd.timeout_ms::bigint || ' milliseconds')::interval
					end,
					locked_by = null,
					locked_at = null
				from now_ts nt, inserted_children ic
				join invoke_child_data icd on icd.execution_id = ic.parent_execution_id
				where e.id = ic.parent_execution_id
			)`);
		}

		if (waitForCustomEvent.length > 0) {
			ctes.push(this.sql`wait_custom_event_data as (
				select * from jsonb_to_recordset(${this.sql.json(waitForCustomEvent as unknown as JsonValue)}::jsonb)
				as x(
					execution_id uuid,
					task_key text,
					queue text,
					step_key text,
					timeout_ms text,
					event_key text
				)
			)`);

			// Insert subscriptions
			ctes.push(this.sql`inserted_custom_event_subscriptions as (
				insert into pgconductor.subscriptions (source, event_key, execution_id, queue, step_key)
				select
					'event',
					wce.event_key,
					wce.execution_id,
					wce.queue,
					wce.step_key
				from wait_custom_event_data wce
				returning id
			)`);

			// Update executions to wait
			ctes.push(this.sql`updated_wait_custom_event as (
				update pgconductor.executions e
				set
					waiting_step_key = wce.step_key,
					run_at = case
						when wce.timeout_ms = 'infinity' then 'infinity'::timestamptz
						else nt.ts + (wce.timeout_ms::bigint || ' milliseconds')::interval
					end,
					locked_by = null,
					locked_at = null
				from now_ts nt, wait_custom_event_data wce
				where e.id = wce.execution_id
			)`);
		}

		if (waitForDbEvent.length > 0) {
			ctes.push(this.sql`wait_db_event_data as (
				select * from jsonb_to_recordset(${this.sql.json(waitForDbEvent as unknown as JsonValue)}::jsonb)
				as x(
					execution_id uuid,
					task_key text,
					queue text,
					step_key text,
					timeout_ms text,
					schema_name text,
					table_name text,
					operation text,
					columns jsonb
				)
			)`);

			// Insert subscriptions
			ctes.push(this.sql`inserted_db_event_subscriptions as (
				insert into pgconductor.subscriptions (
					source,
					schema_name,
					table_name,
					operation,
					execution_id,
					queue,
					step_key,
					columns
				)
				select
					'db',
					wdb.schema_name,
					wdb.table_name,
					wdb.operation,
					wdb.execution_id,
					wdb.queue,
					wdb.step_key,
					case
						when wdb.columns is not null then
							array(select jsonb_array_elements_text(wdb.columns))
						else null
					end
				from wait_db_event_data wdb
				returning id
			)`);

			// Insert triggers (on conflict do nothing)
			ctes.push(this.sql`inserted_db_triggers as (
				insert into pgconductor.triggers (schema_name, table_name, operation)
				select distinct
					wdb.schema_name,
					wdb.table_name,
					wdb.operation
				from wait_db_event_data wdb
				on conflict (schema_name, table_name, operation) do nothing
			)`);

			// Update executions to wait
			ctes.push(this.sql`updated_wait_db_event as (
				update pgconductor.executions e
				set
					waiting_step_key = wdb.step_key,
					run_at = case
						when wdb.timeout_ms = 'infinity' then 'infinity'::timestamptz
						else nt.ts + (wdb.timeout_ms::bigint || ' milliseconds')::interval
					end,
					locked_by = null,
					locked_at = null
				from now_ts nt, wait_db_event_data wdb
				where e.id = wdb.execution_id
			)`);
		}

		if (ctes.length <= 1) return null; // Only now_ts

		const combined = ctes.reduce((acc, cte, i) =>
			i === 0 ? cte : this.sql`${acc}, ${cte}`,
		);

		return this.sql<[{ result: number }]>`with ${combined} select 1 as result`;
	}

	buildRemoveExecutions({
		queueName,
		batchSize,
	}: RemoveExecutionsArgs): PendingQuery<{ deleted_count: number }[]> {
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

	buildRegisterWorker({
		queueName,
		taskSpecs,
		cronSchedules,
		eventSubscriptions,
	}: RegisterWorkerArgs): PendingQuery<RowList<Row[]>> {
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

		const eventSubscriptionRows = eventSubscriptions.map((spec) => ({
			task_key: spec.task_key,
			queue: spec.queue,
			source: spec.source,
			event_key: spec.event_key || null,
			schema_name: spec.schema_name || null,
			table_name: spec.table_name || null,
			operation: spec.operation || null,
			columns: spec.columns || null,
		}));

		return this.sql`
			select pgconductor.register_worker(
				p_queue_name := ${queueName}::text,
				p_task_specs := array(
					select json_populate_recordset(null::pgconductor.task_spec, ${this.sql.json(taskSpecRows)}::json)::pgconductor.task_spec
				),
				p_cron_schedules := array(
					select json_populate_recordset(null::pgconductor.execution_spec, ${this.sql.json(cronScheduleRows)}::json)::pgconductor.execution_spec
				),
				p_event_subscriptions := array(
					select json_populate_recordset(null::pgconductor.subscription_spec, ${this.sql.json(eventSubscriptionRows)}::json)::pgconductor.subscription_spec
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

	buildScheduleCronExecution({
		spec,
		scheduleName,
	}: ScheduleCronExecutionArgs): PendingQuery<[{ id: string }]> {
		assert.ok(spec.run_at, "scheduleCronExecution requires run_at");
		assert.ok(
			spec.cron_expression,
			"scheduleCronExecution requires cron_expression",
		);

		const runAt = spec.run_at as Date;
		const cronExpression = spec.cron_expression as string;
		const timestampSeconds = Math.floor(runAt.getTime() / 1000);
		const dedupeKey = `dynamic::${scheduleName}::${timestampSeconds}`;

		return this.sql<[{ id: string }]>`
			with removed as (
				delete from pgconductor.executions
				where task_key = ${spec.task_key}::text
					and queue = ${spec.queue}::text
					and dedupe_key like 'dynamic::%'
					and split_part(dedupe_key, '::', 2) = ${scheduleName}::text
					and cron_expression is not null
					and run_at > pgconductor.current_time()
			)
			select id from pgconductor.invoke(
				task_key := ${spec.task_key}::text,
				queue := ${spec.queue}::text,
				payload := ${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
				run_at := ${runAt.toISOString()}::timestamptz,
				dedupe_key := ${dedupeKey}::text,
				cron_expression := ${cronExpression}::text,
				priority := ${spec.priority || 0}::integer
			)
		`;
	}

	buildUnscheduleCronExecution({
		taskKey,
		queue,
		scheduleName,
	}: UnscheduleCronExecutionArgs): PendingQuery<[{ deleted_count: number }]> {
		return this.sql<[{ deleted_count: number }]>`
			with deleted as (
				delete from pgconductor.executions
				where task_key = ${taskKey}::text
					and queue = ${queue}::text
					and dedupe_key like 'dynamic::%'
					and split_part(dedupe_key, '::', 2) = ${scheduleName}::text
					and cron_expression is not null
					and run_at > pgconductor.current_time()
				returning 1
			)
			select count(*)::int as deleted_count from deleted
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

	buildLoadStep({
		executionId,
		key,
	}: LoadStepArgs): PendingQuery<[{ result: Payload | null }]> {
		return this.sql<[{ result: Payload | null }]>`
			select result from pgconductor.steps
			where execution_id = ${executionId}::uuid and key = ${key}::text
		`;
	}

	buildSaveStep({
		executionId,
		queue,
		key,
		result,
		runAtMs,
	}: SaveStepArgs): PendingQuery<RowList<Row[]>> {
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

	buildClearWaitingState({
		executionId,
	}: ClearWaitingStateArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			with child_info as (
				select
					e.waiting_on_execution_id as child_id,
					c.locked_by as child_locked_by
				from pgconductor.executions e
				left join pgconductor.executions c on c.id = e.waiting_on_execution_id
				where e.id = ${executionId}::uuid
			),
			-- Fail pending (not locked) children immediately
			failed_pending_child as (
				update pgconductor.executions e
				set
					failed_at = pgconductor.current_time(),
					last_error = 'Cancelled: parent timed out',
					locked_by = null,
					locked_at = null
				from child_info ci
				where e.id = ci.child_id
					and ci.child_locked_by is null   -- not currently executing
					and e.completed_at is null
					and e.failed_at is null
			),
			-- Signal executing (locked) children to cancel
			signaled_executing_child as (
				update pgconductor.executions e
				set cancelled = true
				from child_info ci
				where e.id = ci.child_id
					and ci.child_locked_by is not null  -- currently executing
					and e.completed_at is null
					and e.failed_at is null
			),
			-- Always clear parent's waiting state
			cleared_parent as (
				update pgconductor.executions
				set
					waiting_on_execution_id = null,
					waiting_step_key = null
				where id = ${executionId}::uuid
				returning id
			)
			select id from cleared_parent
		`;
	}

	buildCountActiveOrchestratorsBelow({
		version,
	}: CountActiveOrchestratorsBelowArgs): PendingQuery<{ count: number }[]> {
		return this.sql<{ count: number }[]>`
			select count(*) as count
			from pgconductor.orchestrators
			where migration_number < ${version}::integer
			  and shutdown_signal = false
		`;
	}
}

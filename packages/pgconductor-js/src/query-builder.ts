import type { PendingQuery, Row, RowList, Sql } from "postgres";
import type { GroupedExecutionResults } from "./database-client";
import * as assert from "./lib/assert";
import type {
	Execution,
	ExecutionSpec,
	EventSubscriptionSpec,
	Payload,
	TaskSpec,
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

export type DispatchCustomEventsArgs = {
	eventIds: string[];
	orchestratorId: string;
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
	queue: string;
	orchestratorId: string;
	key: string;
};

export type SaveStepArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	key: string;
	result: Payload | null;
	runAtMs?: number;
};

export type ClearWaitingStateArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
};

export type EmitEventArgs = {
	eventKey: string;
	payload?: Payload;
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
				insert into pgconductor._private_orchestrators as o (
					id,
					version,
					migration_number,
					last_heartbeat_at
				)
				select
					${orchestratorId}::uuid,
					${version}::text,
					${migrationNumber}::integer,
					pgconductor._private_current_time()
				on conflict (id)
				do update
				set
					last_heartbeat_at = pgconductor._private_current_time(),
					version = excluded.version,
					migration_number = excluded.migration_number
				returning id
			),
			-- Signal shutdown if newer migration exists
			shutdown_signal_inserted as (
				insert into pgconductor._private_orchestrator_signals (orchestrator_id, type, payload)
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
				delete from pgconductor._private_orchestrator_signals
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
				delete from pgconductor._private_orchestrators o
				where o.last_heartbeat_at < pgconductor._private_current_time() - ${maxAge}::interval
				returning o.id
			),
			-- fail cancelled executions from expired orchestrators
			failed_cancelled as (
				update pgconductor._private_executions e
				set
					failed_at = pgconductor._private_current_time(),
					locked_by = null,
					locked_at = null
				from expired
				where e.locked_by = expired.id
					and e.cancelled = true
					and e.failed_at is null
					and e.completed_at is null
				returning e.id
			)
			-- unlock remaining (non-cancelled) executions
			update pgconductor._private_executions e
			set
				locked_by = null,
				locked_at = null
			from expired
			where e.locked_by = expired.id
				and e.cancelled = false
		`;
	}

	buildSweepOrchestrators({
		migrationNumber,
	}: SweepOrchestratorsArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			insert into pgconductor._private_orchestrator_signals (orchestrator_id, type, payload)
			select
				id,
				'shutdown',
				jsonb_build_object('reason', 'breaking_migration')
			from pgconductor._private_orchestrators
			where migration_number < ${migrationNumber}::integer
			on conflict (orchestrator_id) where type = 'shutdown' do nothing
		`;
	}

	buildGetInstalledMigrationNumber(): PendingQuery<{ version: number | null }[]> {
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
				delete from pgconductor._private_orchestrators
				where id = ${orchestratorId}::uuid
				returning id
			),
			-- fail cancelled executions from this orchestrator
			failed_cancelled as (
				update pgconductor._private_executions e
				set
					failed_at = pgconductor._private_current_time(),
					locked_by = null,
					locked_at = null
				from deleted
				where e.locked_by = deleted.id
					and e.cancelled = true
					and e.failed_at is null
					and e.completed_at is null
				returning e.id
			)
			-- unlock remaining (non-cancelled) executions
			update pgconductor._private_executions e
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
	}: GetExecutionsArgs): PendingQuery<Execution[]> {
		return this.sql<Execution[]>`
			-- Read limit mode from the database so rolling workers cannot bypass new limits.
			with task_limits as materialized (
				select exists (
					select 1
					from pgconductor._private_tasks t
					where t.queue = ${queueName}::text
						and (t.concurrency_limit is not null or t.group_concurrency_limit is not null)
				) as enabled
			), unconstrained_candidates as (
				select e.id
				from pgconductor._private_executions e
				where not (select enabled from task_limits)
					and e.queue = ${queueName}::text
					and e.run_at <= pgconductor._private_current_time()
					and e.is_available = true
					${filterTaskKeys?.length ? this.sql`and not (e.task_key = any(${this.sql.array(filterTaskKeys)}::text[]))` : this.sql``}
				order by e.priority asc, e.run_at asc, e.created_at asc, e.id asc
				limit ${batchSize}::integer
				for update of e skip locked
			), active_executions as materialized (
				select e.task_key, e."group"
				from pgconductor._private_executions e
				where (select enabled from task_limits)
					and e.queue = ${queueName}::text
					and e.locked_at is not null
					and e.failed_at is null
					and e.completed_at is null
			), active_tasks as (
				select e.task_key, count(*)::integer as active_count
				from active_executions e
				group by e.task_key
			), active_groups as (
				select e.task_key, e."group", count(*)::integer as active_count
				from active_executions e
				where e."group" is not null
				group by e.task_key, e."group"
			),
			-- A full task makes the candidate branch a no-op instead of scanning its backlog.
			available_tasks as materialized (
				select
					t.key,
					t.queue,
					t.concurrency_limit,
					t.group_concurrency_limit,
					coalesce(at.active_count, 0) as active_task_count
				from pgconductor._private_tasks t
				left join active_tasks at on at.task_key = t.key
				where t.queue = ${queueName}::text
					and (t.concurrency_limit is null
						or coalesce(at.active_count, 0) < t.concurrency_limit)
			), candidates as (
				select
					e.id,
					e.task_key,
					e.queue,
					e.priority,
					e.run_at,
					e.created_at,
					e."group",
					t.concurrency_limit,
					t.group_concurrency_limit,
					t.active_task_count,
					coalesce(ag.active_count, 0) as active_group_count
				from pgconductor._private_executions e
				join available_tasks t on t.key = e.task_key and t.queue = e.queue
				left join active_groups ag on ag.task_key = e.task_key and ag."group" = e."group"
				where (select enabled from task_limits)
					and (select exists (select 1 from available_tasks))
					and e.queue = ${queueName}::text
					and e.run_at <= pgconductor._private_current_time()
					and e.is_available = true
					and (t.group_concurrency_limit is null
						or e."group" is null
						or coalesce(ag.active_count, 0) < t.group_concurrency_limit)
					${filterTaskKeys?.length ? this.sql`and not (e.task_key = any(${this.sql.array(filterTaskKeys)}::text[]))` : this.sql``}
				-- Bound both window functions to one locked candidate batch.
				order by e.priority asc, e.run_at asc, e.created_at asc, e.id asc
				limit ${batchSize}::integer
				for update of e skip locked
			), group_ranked as (
				select c.*,
					row_number() over (
						partition by c.task_key, c."group"
						order by c.priority asc, c.run_at asc, c.created_at asc, c.id asc
					) as group_rank
				from candidates c
			), group_eligible as (
				select r.*,
					row_number() over (
						partition by r.task_key
						order by r.priority asc, r.run_at asc, r.created_at asc, r.id asc
					) as task_rank
				from group_ranked r
				where r.group_concurrency_limit is null
					or r."group" is null
					or r.active_group_count + r.group_rank <= r.group_concurrency_limit
			), eligible as (
				select r.id
				from group_eligible r
				where r.concurrency_limit is null
					or r.active_task_count + r.task_rank <= r.concurrency_limit
			), claimable as (
				select id from unconstrained_candidates
				union all
				select id from eligible
			), claimed as (
				update pgconductor._private_executions e
				set
					attempts = e.attempts + 1,
					locked_by = ${orchestratorId}::uuid,
					locked_at = pgconductor._private_current_time()
				from claimable c
				where e.id = c.id and e.queue = ${queueName}::text and e.is_available = true
				returning e.id, e.task_key, e.queue, e.payload, e.waiting_on_execution_id,
					e.waiting_step_key, e.cancelled, e.last_error, e.dedupe_key, e.cron_expression,
					e.locked_by, e."group", e.priority, e.run_at, e.created_at,
					e.subscription_id,
					e.dead_letter_source_execution_id, e.dead_letter_source_queue,
					e.dead_letter_source_task_key, e.dead_letter_error,
					e.dead_letter_attempts, e.dead_letter_failed_at
			)
			select id, task_key, queue, payload, waiting_on_execution_id, waiting_step_key,
				cancelled, last_error, dedupe_key, cron_expression, locked_by, "group",
				subscription_id,
				dead_letter_source_execution_id, dead_letter_source_queue,
				dead_letter_source_task_key, dead_letter_error, dead_letter_attempts, dead_letter_failed_at
			from claimed
			order by priority asc, run_at asc, created_at asc, id asc
		`;
	}

	buildReturnExecutions(grouped: GroupedExecutionResults): PendingQuery<any> | null {
		const allResults = [
			...grouped.completed,
			...grouped.failed,
			...grouped.released,
			...grouped.invokeChild,
		];

		if (allResults.length === 0) return null;

		const ctes: PendingQuery<any>[] = [];
		ctes.push(this.sql`now_ts as (select pgconductor._private_current_time() as ts)`);
		ctes.push(this.sql`result_data as (
			select * from jsonb_to_recordset(${this.sql.json(JSON.parse(JSON.stringify(allResults)))}::jsonb)
			as r(
				execution_id uuid, queue text, task_key text, status text,
				orchestrator_id uuid, result jsonb, error text,
				reschedule_in_ms text, step_key text, timeout_ms text,
				child_task_name text, child_task_queue text, child_payload jsonb,
				"group" text
			)
		)`);
		// Lock the claimed rows for the whole statement. This prevents recovery or a
		// new claim from racing the side effects below.
		ctes.push(this.sql`valid_results as materialized (
			select r.*,
				e.cancelled as execution_cancelled, e.last_error as execution_last_error,
				e.subscription_id
			from result_data r
			join pgconductor._private_executions e
				on e.id = r.execution_id
				and e.queue = r.queue
				and e.task_key = r.task_key
				and e.locked_by = r.orchestrator_id
			for update of e
		)`);
		ctes.push(this.sql`task_configs as materialized (
			select t.queue, t.key, t.max_attempts, t.remove_on_complete_days, t.remove_on_fail_days,
				t.dead_letter_queue, t.dead_letter_task_key
			from (
				select distinct queue, task_key from valid_results
			) r
			join pgconductor._private_tasks t on t.queue = r.queue and t.key = r.task_key
		)`);
		ctes.push(this.sql`completed_results as (
			select * from valid_results where status = 'completed' and not execution_cancelled
		)`);
		ctes.push(this.sql`failed_results as (
			select * from valid_results
			where status in ('failed', 'permanently_failed')
				or (status = 'completed' and execution_cancelled)
		)`);
		ctes.push(this.sql`released_results as (
			select * from valid_results where status = 'released'
		)`);
		ctes.push(this.sql`invoke_child_data as (
			select * from valid_results where status = 'invoke_child'
		)`);

		// A completed child may wake only a parent which is still waiting and is not
		// currently claimed. The row lock makes this check race-safe.
		ctes.push(this.sql`completed_parents as materialized (
			select parent.id as parent_id, parent.queue, parent.waiting_step_key, r.result
			from completed_results r
			join pgconductor._private_executions parent
				on parent.waiting_on_execution_id = r.execution_id
			where r.subscription_id is null
				and parent.completed_at is null
				and parent.failed_at is null
				and parent.locked_by is null
			for update of parent
		)`);
		ctes.push(this.sql`parent_steps_all as (
			insert into pgconductor._private_steps (execution_id, queue, key, result)
			select parent_id, queue, waiting_step_key, result
			from completed_parents
			where waiting_step_key is not null
			on conflict (execution_id, key) do nothing
			returning execution_id
		)`);
		ctes.push(this.sql`orphaned_children as (
			update pgconductor._private_executions e
			set failed_at = nt.ts, completed_at = null,
				last_error = 'Parent timed out before child completed',
				locked_by = null, locked_at = null
			from now_ts nt, completed_results r
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id
				and e.parent_execution_id is not null
				and e.subscription_id is null
				and not exists (
					select 1 from pgconductor._private_executions parent
					where parent.waiting_on_execution_id = r.execution_id
				)
			returning e.id
		)`);
		ctes.push(this.sql`updated_parents_all as (
			update pgconductor._private_executions e
			set run_at = nt.ts, waiting_on_execution_id = null, waiting_step_key = null,
				locked_by = null, locked_at = null
			from now_ts nt, completed_parents p
			where e.id = p.parent_id and e.queue = p.queue
			returning e.id
		)`);
		ctes.push(this.sql`deleted_completed as (
			delete from pgconductor._private_executions e
			using completed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id
				and tc.key = r.task_key and tc.queue = r.queue and tc.remove_on_complete_days = 0
				and not exists (select 1 from orphaned_children oc where oc.id = e.id)
			returning e.id
		)`);
		ctes.push(this.sql`updated_completed as (
			update pgconductor._private_executions e
			set completed_at = nt.ts, locked_by = null, locked_at = null
			from now_ts nt, completed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id
				and tc.key = r.task_key and tc.queue = r.queue
				and (tc.remove_on_complete_days is null or tc.remove_on_complete_days != 0)
				and not exists (select 1 from orphaned_children oc where oc.id = e.id)
			returning e.id
		)`);

		ctes.push(this.sql`permanently_failed_children as materialized (
			select r.execution_id, r.queue, r.task_key, r.orchestrator_id,
				r.execution_cancelled,
				coalesce(r.error, r.execution_last_error, 'unknown error') as child_error,
				e."group" as execution_group,
				e.payload as execution_payload,
				e.attempts as execution_attempts,
				e.subscription_id,
				tc.remove_on_fail_days = 0 as should_remove,
				tc.dead_letter_queue, tc.dead_letter_task_key
			from failed_results r
			join pgconductor._private_executions e on e.id = r.execution_id and e.queue = r.queue
			join task_configs tc on tc.key = r.task_key and tc.queue = r.queue
			where e.attempts >= tc.max_attempts
				or r.status = 'permanently_failed'
				or r.execution_cancelled
		)`);
		ctes.push(this.sql`failed_parent_targets as materialized (
			select p.execution_id as child_id, p.queue as child_queue, p.child_error,
				p.execution_cancelled as child_cancelled,
				parent.id as parent_id, parent.queue as parent_queue,
				parent.task_key as parent_task_key, parent."group" as parent_group,
				parent.payload as parent_payload, parent.attempts as parent_attempts,
				pt.remove_on_fail_days = 0 as parent_should_remove,
				pt.dead_letter_queue as parent_dead_letter_queue,
				pt.dead_letter_task_key as parent_dead_letter_task_key
			from permanently_failed_children p
			join pgconductor._private_executions parent
				on parent.waiting_on_execution_id = p.execution_id
			join pgconductor._private_tasks pt
				on pt.key = parent.task_key and pt.queue = parent.queue
			where p.subscription_id is null
				and parent.completed_at is null and parent.failed_at is null and parent.locked_by is null
			for update of parent
		)`);
		ctes.push(this.sql`terminal_failures as materialized (
			select p.execution_id, p.queue, p.task_key, p.execution_group, p.execution_payload as payload,
				p.child_error as failure_error, p.execution_attempts as failure_attempts,
				p.execution_cancelled, p.dead_letter_queue, p.dead_letter_task_key
			from permanently_failed_children p
			union all
			select p.parent_id, p.parent_queue, p.parent_task_key, p.parent_group, p.parent_payload,
				'Child execution failed: ' || p.child_error, p.parent_attempts, p.child_cancelled,
				p.parent_dead_letter_queue, p.parent_dead_letter_task_key
			from failed_parent_targets p
		)`);
		ctes.push(this.sql`dead_lettered as materialized (
			insert into pgconductor._private_executions (
				task_key, queue, payload, run_at, "group",
				dead_letter_source_execution_id, dead_letter_source_queue,
				dead_letter_source_task_key, dead_letter_error,
				dead_letter_attempts, dead_letter_failed_at
			)
			select
				coalesce(p.dead_letter_task_key, p.task_key),
				coalesce(p.dead_letter_queue, p.queue),
				p.payload, nt.ts, p.execution_group,
				p.execution_id, p.queue, p.task_key, p.failure_error,
				p.failure_attempts, nt.ts
			from terminal_failures p
			cross join now_ts nt
			where p.dead_letter_queue is not null
				and not p.execution_cancelled
			on conflict (dead_letter_source_execution_id, queue, task_key)
				where dead_letter_source_execution_id is not null
				do update set dead_letter_source_execution_id = excluded.dead_letter_source_execution_id
			returning id, dead_letter_source_execution_id
		)`);
		ctes.push(this.sql`failed_updates as (
			select p.execution_id as target_id, p.queue, p.child_error, true as is_child
			from permanently_failed_children p
			where p.should_remove is not true
			union all
			select p.parent_id, p.parent_queue, p.child_error, false
			from failed_parent_targets p
			where p.parent_should_remove is not true
		)`);
		ctes.push(this.sql`failed_delete_targets as materialized (
			select p.execution_id as target_id, p.queue, p.orchestrator_id as expected_locked_by
			from permanently_failed_children p
			where p.should_remove is true
				and (
					p.execution_cancelled
					or p.dead_letter_queue is null
					or exists (select 1 from dead_lettered d where d.dead_letter_source_execution_id = p.execution_id)
				)
			union all
			select p.parent_id, p.parent_queue, null::uuid
			from failed_parent_targets p
			where p.parent_should_remove is true
				and (
					p.child_cancelled
					or p.parent_dead_letter_queue is null
					or exists (select 1 from dead_lettered d where d.dead_letter_source_execution_id = p.parent_id)
				)
		)`);
		ctes.push(this.sql`deleted_failed as (
			delete from pgconductor._private_executions e
			using failed_delete_targets f
			where e.id = f.target_id and e.queue = f.queue
				and e.locked_by is not distinct from f.expected_locked_by
			returning e.id
		)`);
		ctes.push(this.sql`updated_failed as (
			update pgconductor._private_executions e
			set failed_at = nt.ts,
				last_error = case when f.is_child then coalesce(f.child_error, 'unknown error')
					else 'Child execution failed: ' || coalesce(f.child_error, 'unknown error') end,
				waiting_on_execution_id = null, waiting_step_key = null,
				locked_by = null, locked_at = null
			from now_ts nt, failed_updates f
			where e.id = f.target_id and e.queue = f.queue
			returning e.id
		)`);
		ctes.push(this.sql`retried as (
			update pgconductor._private_executions e
			set last_error = coalesce(r.error, 'unknown error'),
				run_at = greatest(nt.ts, coalesce(e.run_at, nt.ts)) +
					((array[15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200])[least(greatest(e.attempts, 1), 10)] * interval '1 second'),
				locked_by = null, locked_at = null
			from now_ts nt, failed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id
				and tc.key = r.task_key and tc.queue = r.queue
				and r.status <> 'permanently_failed'
				and not r.execution_cancelled
				and e.attempts < tc.max_attempts
			returning e.id
		)`);

		ctes.push(this.sql`released_steps as (
			insert into pgconductor._private_steps (execution_id, queue, key, result)
			select execution_id, queue, step_key, null::jsonb from released_results
			where step_key is not null
			on conflict (execution_id, key) do nothing
			returning id
		)`);
		ctes.push(this.sql`updated_released as (
			update pgconductor._private_executions e
			set attempts = greatest(e.attempts - 1, 0),
				run_at = case when lower(nullif(trim(r.reschedule_in_ms), '')) = 'infinity' then 'infinity'::timestamptz
					when nullif(trim(r.reschedule_in_ms), '') is not null then
						nt.ts + (nullif(trim(r.reschedule_in_ms), '')::bigint || ' milliseconds')::interval
					else nt.ts end,
				locked_by = null, locked_at = null
			from now_ts nt, released_results r
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id
			returning e.id
		)`);

		ctes.push(this.sql`inserted_children as (
			insert into pgconductor._private_executions (id, task_key, queue, payload, run_at, parent_execution_id, "group")
			select pgconductor._private_portable_uuidv7(), r.child_task_name, r.child_task_queue, r.child_payload, nt.ts, r.execution_id, r."group"
			from invoke_child_data r, now_ts nt
			where exists (
				select 1 from pgconductor._private_executions parent
				where parent.id = r.execution_id and parent.queue = r.queue
					and parent.locked_by = r.orchestrator_id
			)
			returning id, parent_execution_id
		)`);
		ctes.push(this.sql`updated_invoke_parents as (
			update pgconductor._private_executions e
			set waiting_on_execution_id = ic.id, waiting_step_key = r.step_key,
				run_at = case when lower(nullif(trim(r.timeout_ms), '')) = 'infinity' then 'infinity'::timestamptz
					when nullif(trim(r.timeout_ms), '') is not null then
						nt.ts + (nullif(trim(r.timeout_ms), '')::bigint || ' milliseconds')::interval
					else nt.ts end,
				locked_by = null, locked_at = null
			from now_ts nt, inserted_children ic
			join invoke_child_data r on r.execution_id = ic.parent_execution_id
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id
			returning e.id
		)`);

		const combined = ctes.reduce((acc, cte, i) => (i === 0 ? cte : this.sql`${acc}, ${cte}`));
		return this.sql<[{ result: number }]>`with ${combined} select 1 as result`;
	}
	buildRemoveExecutions({
		queueName,
		batchSize,
	}: RemoveExecutionsArgs): PendingQuery<{ deleted_count: number }[]> {
		return this.sql<{ deleted_count: number }[]>`
			with batch as (
				select e.id
				from pgconductor._private_executions e
				join pgconductor._private_tasks t on t.key = e.task_key and t.queue = e.queue
				where e.queue = ${queueName}
					and (
						(e.completed_at is not null and t.remove_on_complete_days > 0 and e.completed_at < pgconductor._private_current_time() - t.remove_on_complete_days * interval '1 day')
						or
						(e.failed_at is not null and t.remove_on_fail_days > 0 and e.failed_at < pgconductor._private_current_time() - t.remove_on_fail_days * interval '1 day')
					)
				limit ${batchSize}
			),
			deleted as (
				delete from pgconductor._private_executions
				using batch
				where pgconductor._private_executions.id = batch.id
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
			concurrency_limit: spec.concurrency || null,
			group_concurrency_limit: spec.groupConcurrency || null,
			dead_letter_queue: spec.deadLetterQueue || null,
			dead_letter_task_key: spec.deadLetterTaskKey || null,
		}));

		const cronScheduleRows = cronSchedules.map((spec) => {
			assert.ok(spec.cron_expression, "cron_expression is required for cron schedules");

			return {
				task_key: spec.task_key,
				queue: spec.queue,
				payload: spec.payload || null,
				run_at: spec.run_at,
				dedupe_key: spec.dedupe_key,
				cron_expression: spec.cron_expression,
				priority: spec.priority || null,
				group: spec.group || null,
			};
		});

		const eventSubscriptionRows = eventSubscriptions.map((spec) => ({
			task_key: spec.task_key,
			event_key: spec.event_key,
			payload_fields: spec.payload_fields,
			required_field_count: spec.required_field_count,
			terms: spec.terms,
		}));

		return this.sql`
			select pgconductor._private_register_worker(
				p_queue_name := ${queueName}::text,
				p_task_specs := array(
					select json_populate_recordset(null::pgconductor.task_spec, ${this.sql.json(taskSpecRows)}::json)::pgconductor.task_spec
				),
				p_cron_schedules := array(
					select json_populate_recordset(null::pgconductor.execution_spec, ${this.sql.json(cronScheduleRows)}::json)::pgconductor.execution_spec
				),
				p_event_subscriptions := array(
					select json_populate_recordset(null::pgconductor.event_subscription_spec, ${this.sql.json(eventSubscriptionRows)}::json)::pgconductor.event_subscription_spec
				)
			)
		`;
	}

	buildInvoke(spec: ExecutionSpec): PendingQuery<[{ id: string | null }]> {
		if (spec.throttle && spec.debounce) {
			throw new Error("Cannot use both throttle and debounce - choose one");
		}

		let dedupe_seconds: number | null = null;
		let dedupe_next_slot = false;

		if (spec.throttle) {
			dedupe_seconds = spec.throttle.seconds;
			dedupe_next_slot = false;
		} else if (spec.debounce) {
			dedupe_seconds = spec.debounce.seconds;
			dedupe_next_slot = true;
		}

		return this.sql<[{ id: string | null }]>`
			select id from pgconductor.invoke(
				p_task_key := ${spec.task_key}::text,
				p_queue := ${spec.queue}::text,
				p_payload := ${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
				p_run_at := ${spec.run_at ? spec.run_at.toISOString() : null}::timestamptz,
				p_dedupe_key := ${spec.dedupe_key || null}::text,
				p_dedupe_seconds := ${dedupe_seconds}::integer,
				p_dedupe_next_slot := ${dedupe_next_slot}::boolean,
				p_cron_expression := ${spec.cron_expression || null}::text,
				p_priority := ${spec.priority || null}::integer,
				p_group := ${spec.group || null}::text
			)
		`;
	}

	buildScheduleCronExecution({
		spec,
		scheduleName,
	}: ScheduleCronExecutionArgs): PendingQuery<[{ id: string }]> {
		assert.ok(spec.run_at, "scheduleCronExecution requires run_at");
		assert.ok(spec.cron_expression, "scheduleCronExecution requires cron_expression");

		const runAt = spec.run_at as Date;
		const cronExpression = spec.cron_expression as string;
		const timestampSeconds = Math.floor(runAt.getTime() / 1000);
		const dedupeKey = `scheduled::${scheduleName}::${timestampSeconds}`;

		return this.sql<[{ id: string }]>`
			with removed as (
				delete from pgconductor._private_executions
				where task_key = ${spec.task_key}::text
					and queue = ${spec.queue}::text
					and dedupe_key like 'scheduled::%'
					and split_part(dedupe_key, '::', 2) = ${scheduleName}::text
					and cron_expression is not null
					and run_at > pgconductor._private_current_time()
			)
			select id from pgconductor.invoke(
				p_task_key := ${spec.task_key}::text,
				p_queue := ${spec.queue}::text,
				p_payload := ${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
				p_run_at := ${runAt.toISOString()}::timestamptz,
				p_dedupe_key := ${dedupeKey}::text,
				p_dedupe_seconds := null::integer,
				p_dedupe_next_slot := false::boolean,
				p_cron_expression := ${cronExpression}::text,
				p_priority := ${spec.priority || 0}::integer,
				p_group := ${spec.group || null}::text
			)
		`;
	}

	buildUnscheduleCronExecution({
		taskKey,
		queue,
		scheduleName,
	}: UnscheduleCronExecutionArgs): PendingQuery<[]> {
		return this.sql<[]>`
			with
			-- Delete future pending executions
			deleted_future as (
				delete from pgconductor._private_executions
				where task_key = ${taskKey}::text
					and queue = ${queue}::text
					and dedupe_key like 'scheduled::%'
					and split_part(dedupe_key, '::', 2) = ${scheduleName}::text
					and cron_expression is not null
					and run_at > pgconductor._private_current_time()
				returning 1
			),
			-- Mark running executions as cancelled (no signal needed, they'll fail naturally)
			cancelled_running as (
				update pgconductor._private_executions e
				set cancelled = true
				where e.task_key = ${taskKey}::text
					and e.queue = ${queue}::text
					and e.dedupe_key like 'scheduled::%'
					and split_part(e.dedupe_key, '::', 2) = ${scheduleName}::text
					and e.cron_expression is not null
					and e.locked_by is not null
					and e.completed_at is null
					and e.failed_at is null
					and e.cancelled = false
				returning 1
			)
			select where false
		`;
	}

	buildInvokeBatch(specs: ExecutionSpec[]): PendingQuery<{ id: string }[]> {
		const specsArray = specs.map((spec) => {
			if (spec.throttle && spec.debounce) {
				throw new Error("Cannot use both throttle and debounce - choose one");
			}

			if (spec.debounce) {
				throw new Error("Batch invoke only supports throttle, not debounce");
			}

			let dedupe_seconds: number | null = null;
			let dedupe_next_slot = false;

			if (spec.throttle) {
				dedupe_seconds = spec.throttle.seconds;
				dedupe_next_slot = false;
			}

			return {
				task_key: spec.task_key,
				queue: spec.queue,
				payload: spec.payload || null,
				run_at: spec.run_at,
				dedupe_key: spec.dedupe_key || null,
				dedupe_seconds,
				dedupe_next_slot,
				cron_expression: spec.cron_expression || null,
				priority: spec.priority,
				group: spec.group || null,
			};
		});

		return this.sql<{ id: string }[]>`
			select id from pgconductor.invoke_batch(
				array(
					select jsonb_populate_recordset(null::pgconductor.execution_spec, ${this.sql.json(specsArray)}::jsonb)
				)
			)
		`;
	}

	buildLoadStep({
		executionId,
		queue,
		orchestratorId,
		key,
	}: LoadStepArgs): PendingQuery<[{ result: Payload | null }]> {
		return this.sql<[{ result: Payload | null }]>`
			select result from pgconductor._private_steps
			where execution_id = ${executionId}::uuid
				and queue = ${queue}::text
				and exists (
					select 1 from pgconductor._private_executions e
					where e.id = ${executionId}::uuid and e.queue = ${queue}::text
						and e.locked_by = ${orchestratorId}::uuid
				)
				and key = ${key}::text
		`;
	}

	buildSaveStep({
		executionId,
		queue,
		key,
		result,
		runAtMs,
		orchestratorId,
	}: SaveStepArgs): PendingQuery<RowList<Row[]>> {
		if (runAtMs) {
			return this.sql<RowList<Row[]>>`
				with claimed_execution as materialized (
					select e.id, e.queue
					from pgconductor._private_executions e
					where e.id = ${executionId}::uuid
						and e.queue = ${queue}::text
						and e.locked_by = ${orchestratorId}::uuid
					for update
				), inserted as (
					insert into pgconductor._private_steps (execution_id, queue, key, result)
					select e.id, e.queue, ${key}::text, ${this.sql.json(result)}::jsonb
					from claimed_execution e
					on conflict (execution_id, key) do nothing
					returning id
				)
				update pgconductor._private_executions e
				set run_at = pgconductor._private_current_time() + (${runAtMs}::integer || ' milliseconds')::interval
				from claimed_execution c
				where e.id = c.id and e.queue = c.queue
					and exists (select 1 from inserted)
			`;
		}

		return this.sql<RowList<Row[]>>`
			with claimed_execution as materialized (
				select e.id, e.queue
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
				for update
			)
			insert into pgconductor._private_steps (execution_id, queue, key, result)
			select e.id, e.queue, ${key}::text, ${this.sql.json(result)}::jsonb
			from claimed_execution e
			on conflict (execution_id, key) do nothing
		`;
	}

	buildClearWaitingState({
		executionId,
		queue,
		orchestratorId,
	}: ClearWaitingStateArgs): PendingQuery<RowList<Row[]>> {
		return this.sql<RowList<Row[]>>`
			with claimed_parent as materialized (
				select e.id, e.queue, e.waiting_on_execution_id
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
				for update
			), child_info as (
				select
					p.waiting_on_execution_id as child_id,
					c.locked_by as child_locked_by
				from claimed_parent p
				left join pgconductor._private_executions c on c.id = p.waiting_on_execution_id
			),
			-- Fail pending (not locked) children immediately
			failed_pending_child as (
				update pgconductor._private_executions e
				set
					failed_at = pgconductor._private_current_time(),
					last_error = 'Cancelled: parent timed out',
					locked_by = null,
					locked_at = null
				from child_info ci
				where e.id = ci.child_id
					and ci.child_locked_by is null   -- not currently executing
					and e.completed_at is null
					and e.failed_at is null
				returning e.id
			),
			-- Signal executing (locked) children to cancel
			signaled_executing_child as (
				update pgconductor._private_executions e
				set cancelled = true
				from child_info ci
				where e.id = ci.child_id
					and ci.child_locked_by is not null  -- currently executing
					and e.completed_at is null
					and e.failed_at is null
				returning e.id
			),
			-- Always clear parent's waiting state
			cleared_parent as (
				update pgconductor._private_executions e
				set
					waiting_on_execution_id = null,
					waiting_step_key = null
				from claimed_parent p
				where e.id = p.id
				and e.queue = p.queue
				returning e.id
			)
			select id from cleared_parent
			union all
			select id from failed_pending_child
			union all
			select id from signaled_executing_child
		`;
	}

	buildCountActiveOrchestratorsBelow({
		version,
	}: CountActiveOrchestratorsBelowArgs): PendingQuery<{ count: number }[]> {
		return this.sql<{ count: number }[]>`
			select count(*) as count
			from pgconductor._private_orchestrators
			where migration_number < ${version}::integer
			  and shutdown_signal = false
		`;
	}

	buildLockEventDispatchSources({
		eventIds,
		orchestratorId,
	}: DispatchCustomEventsArgs): PendingQuery<{ event_id: string }[]> {
		return this.sql<{ event_id: string }[]>`
			select source.id as event_id
			from pgconductor._private_executions source
			where source.id = any(${this.sql.array(eventIds, 2951)}::uuid[])
				and source.queue = 'pgconductor.internal'
				and source.task_key = 'pgconductor.event-dispatch'
				and source.locked_by = ${orchestratorId}::uuid
				and source.completed_at is null
				and source.failed_at is null
				and not source.cancelled
			order by source.id
			for update of source
		`;
	}

	buildCommitEventDispatches({
		eventIds,
		orchestratorId,
	}: DispatchCustomEventsArgs): PendingQuery<{ event_id: string }[]> {
		return this.sql<{ event_id: string }[]>`
			with sources as materialized (
				select
					source.id as event_id,
					source.payload ->> 'eventKey' as event_key,
					source.payload -> 'payload' as event_payload
				from pgconductor._private_executions source
				where source.id = any(${this.sql.array(eventIds, 2951)}::uuid[])
					and source.queue = 'pgconductor.internal'
					and source.task_key = 'pgconductor.event-dispatch'
					and source.locked_by = ${orchestratorId}::uuid
					and source.completed_at is null
					and source.failed_at is null
					and not source.cancelled
			), pending as materialized (
				select source.*
				from sources source
				where not exists (
					select 1
					from pgconductor._private_steps marker
					where marker.execution_id = source.event_id
						and marker.queue = 'pgconductor.internal'
						and marker.key = 'pgconductor.internal.event-fanout.v1'
				)
			), event_values as materialized (
				select source.event_id, source.event_key, field.key as field_name,
					field.value, jsonb_typeof(field.value) as scalar_type
				from pending source
				cross join lateral jsonb_each(source.event_payload) field
			), event_prefixes as materialized (
				select distinct
					event_value.event_id,
					event_value.event_key,
					event_value.field_name,
					term.prefix_length,
					left(event_value.value #>> '{}', term.prefix_length) as prefix_value
				from event_values event_value
				cross join lateral (
					select distinct candidate.prefix_length
					from pgconductor._private_event_filter_terms candidate
					where candidate.operator = 'prefix'
						and candidate.event_key collate "C" = event_value.event_key collate "C"
						and candidate.field_name collate "C" = event_value.field_name collate "C"
				) term
				where event_value.scalar_type = 'string'
			), raw_matched_fields as (
				select event_value.event_id, term.subscription_id, term.field_name
				from event_values event_value
				join pgconductor._private_event_filter_terms term
					on term.operator = 'exact'
					and term.scalar_type = 'string'
					and event_value.scalar_type = 'string'
					and term.event_key collate "C" = event_value.event_key collate "C"
					and term.field_name collate "C" = event_value.field_name collate "C"
					and term.text_value collate "C" = (event_value.value #>> '{}') collate "C"
				union all
				select event_value.event_id, term.subscription_id, term.field_name
				from event_values event_value
				join pgconductor._private_event_filter_terms term
					on term.operator = 'exact'
					and term.scalar_type = 'number'
					and event_value.scalar_type = 'number'
					and term.event_key = event_value.event_key
					and term.field_name = event_value.field_name
					and term.number_value = case when event_value.scalar_type = 'number'
						then (event_value.value #>> '{}')::numeric end
				union all
				select event_value.event_id, term.subscription_id, term.field_name
				from event_values event_value
				join pgconductor._private_event_filter_terms term
					on term.operator = 'exact'
					and term.scalar_type = 'boolean'
					and event_value.scalar_type = 'boolean'
					and term.event_key = event_value.event_key
					and term.field_name = event_value.field_name
					and term.boolean_value = case when event_value.scalar_type = 'boolean'
						then (event_value.value #>> '{}')::boolean end
				union all
				select event_value.event_id, term.subscription_id, term.field_name
				from event_values event_value
				join pgconductor._private_event_filter_terms term
					on term.operator = 'exact'
					and term.scalar_type = 'null'
					and event_value.scalar_type = 'null'
					and term.event_key = event_value.event_key
					and term.field_name = event_value.field_name
				union all
				select event_prefix.event_id, term.subscription_id, term.field_name
				from event_prefixes event_prefix
				join pgconductor._private_event_filter_terms term
					on term.operator = 'prefix'
					and term.event_key collate "C" = event_prefix.event_key collate "C"
					and term.field_name collate "C" = event_prefix.field_name collate "C"
					and term.prefix_length = event_prefix.prefix_length
					and term.text_value collate "C" = event_prefix.prefix_value collate "C"
				union all
				select event_value.event_id, term.subscription_id, term.field_name
				from event_values event_value
				join pgconductor._private_event_filter_terms term
					on term.operator = 'numeric_range'
					and event_value.scalar_type = 'number'
					and term.event_key = event_value.event_key
					and term.field_name = event_value.field_name
					and term.number_range @> case when event_value.scalar_type = 'number'
						then (event_value.value #>> '{}')::numeric end
				union all
				select event_value.event_id, term.subscription_id, term.field_name
				from event_values event_value
				join pgconductor._private_event_filter_terms term
					on term.event_key = event_value.event_key
					and term.field_name = event_value.field_name
					and term.operator = 'exists'
					and term.boolean_value
				union all
				select source.event_id, term.subscription_id, term.field_name
				from pending source
				join pgconductor._private_event_filter_terms term
					on term.event_key = source.event_key
					and term.operator = 'exists'
					and not term.boolean_value
				where not source.event_payload ? term.field_name
				union all
				select event_value.event_id, term.subscription_id, term.field_name
				from event_values event_value
				join pgconductor._private_event_filter_terms term
					on term.event_key = event_value.event_key
					and term.field_name = event_value.field_name
					and term.operator = 'anything_but'
				where event_value.scalar_type in ('string', 'number', 'boolean', 'null')
					and case term.scalar_type
					when 'string' then event_value.scalar_type <> 'string'
						or term.text_value collate "C" <> (event_value.value #>> '{}') collate "C"
					when 'number' then event_value.scalar_type <> 'number'
						or term.number_value <> case when event_value.scalar_type = 'number'
							then (event_value.value #>> '{}')::numeric end
					when 'boolean' then event_value.scalar_type <> 'boolean'
						or term.boolean_value <> case when event_value.scalar_type = 'boolean'
							then (event_value.value #>> '{}')::boolean end
					when 'null' then event_value.scalar_type <> 'null'
					else false
				end
			), matched_subscriptions as materialized (
				select matched.event_id, matched.subscription_id
				from raw_matched_fields matched
				join pgconductor._private_custom_event_subscriptions subscription
					on subscription.id = matched.subscription_id
				group by matched.event_id, matched.subscription_id,
					subscription.required_field_count
				having count(distinct matched.field_name) = subscription.required_field_count
				union all
				select source.event_id, subscription.id
				from pending source
				join pgconductor._private_custom_event_subscriptions subscription
					on subscription.event_key = source.event_key
					and subscription.required_field_count = 0
			), candidates as materialized (
				select matched.event_id, subscription.id as subscription_id,
					subscription.task_key, subscription.queue, subscription.payload_fields
				from matched_subscriptions matched
				join pgconductor._private_custom_event_subscriptions subscription
					on subscription.id = matched.subscription_id
				join pgconductor._private_tasks task
					on task.key = subscription.task_key and task.queue = subscription.queue
			), inserted_destinations as (
				insert into pgconductor._private_executions (
					id, task_key, queue, payload, parent_execution_id, subscription_id
				)
				select
					pgconductor._private_portable_uuidv7(),
					candidate.task_key,
					candidate.queue,
					jsonb_build_object(
						'event', source.event_key,
						'payload', case
							when candidate.payload_fields is null then source.event_payload
							else coalesce((
								select jsonb_object_agg(field_name, source.event_payload -> field_name)
								from unnest(candidate.payload_fields) field_name
								where source.event_payload ? field_name
							), '{}'::jsonb)
						end
					),
					candidate.event_id,
					candidate.subscription_id
				from candidates candidate
				join pending source on source.event_id = candidate.event_id
				order by candidate.event_id, candidate.subscription_id
				on conflict (parent_execution_id, subscription_id, queue)
				where subscription_id is not null
				do nothing
				returning parent_execution_id
			), inserted_markers as (
				insert into pgconductor._private_steps (execution_id, queue, key, result)
				select source.event_id, 'pgconductor.internal',
					'pgconductor.internal.event-fanout.v1', null::jsonb
				from pending source
				cross join (select count(*) from inserted_destinations) destination_barrier
				on conflict (execution_id, key) do nothing
				returning execution_id
			)
			select source.event_id
			from sources source
			cross join (select count(*) from inserted_markers) marker_barrier
			order by source.event_id
		`;
	}

	buildEmitEvent({ eventKey, payload }: EmitEventArgs): PendingQuery<{ id: string }[]> {
		return this.sql<{ id: string }[]>`
			select pgconductor.emit_event(
				${eventKey}::text,
				${this.sql.json(payload || {})}::jsonb
			) as id
		`;
	}
}

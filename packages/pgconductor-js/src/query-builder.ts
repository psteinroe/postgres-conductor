import type { PendingQuery, Row, RowList, Sql } from "postgres";
import type {
	CronRegistration,
	GroupedExecutionResults,
	ReturnExecutionsRow,
} from "./database-client";
import { boundedCarrier } from "./telemetry";
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

export type RemoveProcessedEventsArgs = {
	before: Date;
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
	payload?: JsonValue;
	trace_context?: import("./internal-types").TraceContextCarrier | null;
};

export type RegisterEventWaitArgs = {
	executionId: string;
	queue: string;
	taskKey: string;
	eventKey: string;
	stepKey: string;
	filter: Record<string, JsonValue[]> | null;
	timeoutMs: number | null;
	orchestratorId: string;
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
			with active_tasks as (
				select e.task_key, count(*)::integer as active_count
				from pgconductor._private_executions e
				where e.queue = ${queueName}::text
					and e.locked_at is not null
					and e.failed_at is null
					and e.completed_at is null
				group by e.task_key
			), active_groups as (
				select e.task_key, e."group", count(*)::integer as active_count
				from pgconductor._private_executions e
				where e.queue = ${queueName}::text
					and e."group" is not null
					and e.locked_at is not null
					and e.failed_at is null
					and e.completed_at is null
				group by e.task_key, e."group"
			), ranked as (
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
					coalesce(at.active_count, 0) as active_task_count,
					coalesce(ag.active_count, 0) as active_group_count,
					row_number() over (
						partition by e.task_key
						order by e.priority asc, e.run_at asc, e.created_at asc, e.id asc
					) as task_rank,
					row_number() over (
						partition by e.task_key, e."group"
						order by e.priority asc, e.run_at asc, e.created_at asc, e.id asc
					) as group_rank
				from pgconductor._private_executions e
				left join pgconductor._private_tasks t
					on t.key = e.task_key and t.queue = e.queue
				left join active_tasks at on at.task_key = e.task_key
				left join active_groups ag on ag.task_key = e.task_key and ag."group" = e."group"
				where e.queue = ${queueName}::text
					and e.run_at <= pgconductor._private_current_time()
					and e.is_available = true
					${filterTaskKeys?.length ? this.sql`and not (e.task_key = any(${this.sql.array(filterTaskKeys)}::text[]))` : this.sql``}
			), group_eligible as (
				select r.*,
					row_number() over (
						partition by r.task_key
						order by r.priority asc, r.run_at asc, r.created_at asc, r.id asc
					) as available_task_rank
				from ranked r
				where r.group_concurrency_limit is null
					or r."group" is null
					or r.active_group_count + r.group_rank <= r.group_concurrency_limit
			), eligible as (
				select r.id, r.priority, r.run_at, r.created_at
				from group_eligible r
				where r.concurrency_limit is null
					or r.active_task_count + r.available_task_rank <= r.concurrency_limit
				order by r.priority asc, r.run_at asc, r.created_at asc, r.id asc
				-- Keep a bounded candidate pool so SKIP LOCKED can backfill a batch.
				limit greatest(${batchSize}::integer * 4, ${batchSize}::integer)
			), locked_candidates as (
				select e.id, e.trace_link_context
				from pgconductor._private_executions e
				join eligible c on c.id = e.id
				where e.queue = ${queueName}::text
				order by c.priority asc, c.run_at asc, c.created_at asc, c.id asc
				limit ${batchSize}::integer
				for update of e skip locked
			), claimed as (
				update pgconductor._private_executions e
				set
					attempts = e.attempts + 1,
					locked_by = ${orchestratorId}::uuid,
					locked_at = pgconductor._private_current_time(),
					trace_link_context = null
				from locked_candidates c
				where e.id = c.id and e.queue = ${queueName}::text and e.is_available = true
				returning e.id, e.task_key, e.queue, e.payload, e.waiting_on_execution_id,
					e.waiting_step_key, e.attempts, e.cancelled, e.last_error, e.dedupe_key,
					e.cron_expression,
					e.locked_by, e."group", e.trace_context, c.trace_link_context,
					e.dead_letter_source_execution_id,
					e.dead_letter_source_queue, e.dead_letter_source_task_key, e.dead_letter_error,
					e.dead_letter_attempts, e.dead_letter_failed_at, e.parent_execution_id,
					e.priority, e.run_at, e.created_at
			)
			select c.id, c.task_key, c.queue, c.payload, c.waiting_on_execution_id,
				c.waiting_step_key, c.attempts, c.cancelled, c.last_error, c.dedupe_key,
				c.cron_expression, c.locked_by, c."group", c.trace_context, c.trace_link_context,
				c.dead_letter_source_execution_id, c.dead_letter_source_queue,
				c.dead_letter_source_task_key, c.dead_letter_error, c.dead_letter_attempts,
				c.dead_letter_failed_at, c.parent_execution_id, p.queue as parent_queue,
				p.task_key as parent_task_key, pt.dead_letter_queue as parent_dead_letter_queue,
				pt.dead_letter_task_key as parent_dead_letter_task_key
			from claimed c
			left join pgconductor._private_executions p on p.id = c.parent_execution_id
			left join pgconductor._private_tasks pt on pt.queue = p.queue and pt.key = p.task_key
			order by c.priority asc, c.run_at asc, c.created_at asc, c.id asc
		`;
	}

	buildReturnExecutions(
		grouped: GroupedExecutionResults,
	): PendingQuery<ReturnExecutionsRow[]> | null {
		const allResults = [
			...grouped.completed,
			...grouped.failed,
			...grouped.released,
			...grouped.invokeChild,
		];

		if (allResults.length === 0) return null;

		const ctes: PendingQuery<RowList<Row[]>>[] = [];
		ctes.push(this.sql`now_ts as (select pgconductor._private_current_time() as ts)`);
		ctes.push(this.sql`result_data as (
			select * from jsonb_to_recordset(${this.sql.json(JSON.parse(JSON.stringify(allResults)))}::jsonb)
			as r(
				execution_id uuid, queue text, task_key text, status text,
				orchestrator_id uuid, result jsonb, error text,
				reschedule_in_ms text, step_key text, timeout_ms text,
				child_task_name text, child_task_queue text, child_payload jsonb,
				"group" text, trace_context jsonb, dead_letter_trace_contexts jsonb
			)
		)`);
		// Lock the claimed rows for the whole statement. This prevents recovery or a
		// new claim from racing the side effects below.
		ctes.push(this.sql`valid_results as materialized (
			select r.*,
				e.cancelled as execution_cancelled, e.last_error as execution_last_error
			from result_data r
			join pgconductor._private_executions e
				on e.id = r.execution_id
				and e.queue = r.queue
				and e.task_key = r.task_key
				and e.locked_by = r.orchestrator_id
			for update of e
		)`);
		ctes.push(this.sql`task_configs as (
			select queue, key, max_attempts, remove_on_complete_days, remove_on_fail_days,
				dead_letter_queue, dead_letter_task_key
			from pgconductor._private_tasks
			where queue = any(${this.sql.array(Array.from(new Set(allResults.map((r) => r.queue))))}::text[])
		)`);
		ctes.push(this.sql`completed_results as (
			select * from valid_results where status = 'completed' and not execution_cancelled
		)`);
		ctes.push(this.sql`failed_results as (
			select * from valid_results
			where status in ('failed', 'permanently_failed')
				or (status in ('completed', 'released') and execution_cancelled)
		)`);
		ctes.push(this.sql`released_results as (
			select * from valid_results where status = 'released' and not execution_cancelled
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
			where parent.completed_at is null
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
			returning e.id, e.queue
		)`);

		ctes.push(this.sql`permanently_failed_children as materialized (
			select r.execution_id, r.queue, r.task_key, r.orchestrator_id,
				r.trace_context, r.dead_letter_trace_contexts, r.execution_cancelled,
				coalesce(r.error, r.execution_last_error, 'unknown error') as child_error,
				e."group" as execution_group,
				e.attempts as execution_attempts,
				tc.remove_on_fail_days = 0 as should_remove
			from failed_results r
			join pgconductor._private_executions e on e.id = r.execution_id and e.queue = r.queue
			join task_configs tc on tc.key = r.task_key and tc.queue = r.queue
			where e.attempts >= tc.max_attempts
				or r.status = 'permanently_failed'
				or r.execution_cancelled
		)`);
		ctes.push(this.sql`failed_parent_targets as materialized (
			select p.execution_id as child_id, p.queue as child_queue, p.child_error,
				p.dead_letter_trace_contexts, p.execution_cancelled as child_cancelled,
				parent.id as parent_id, parent.queue as parent_queue,
				parent.task_key as parent_task_key, parent."group" as parent_group,
				parent.payload as parent_payload, parent.attempts as parent_attempts,
				pt.remove_on_fail_days = 0 as parent_should_remove
			from permanently_failed_children p
			join pgconductor._private_executions parent
				on parent.waiting_on_execution_id = p.execution_id
			join pgconductor._private_tasks pt
				on pt.key = parent.task_key and pt.queue = parent.queue
			where parent.completed_at is null and parent.failed_at is null and parent.locked_by is null
			for update of parent
		)`);
		ctes.push(this.sql`terminal_failures as materialized (
			select p.execution_id, p.queue, p.task_key, p.execution_group, e.payload,
				p.dead_letter_trace_contexts -> p.execution_id::text as dead_letter_trace_context,
				p.child_error as failure_error, p.execution_attempts as failure_attempts,
				p.execution_cancelled, tc.dead_letter_queue, tc.dead_letter_task_key
			from permanently_failed_children p
			join pgconductor._private_executions e
				on e.id = p.execution_id and e.queue = p.queue
			join task_configs tc on tc.key = p.task_key and tc.queue = p.queue
			union all
			select p.parent_id, p.parent_queue, p.parent_task_key, p.parent_group, p.parent_payload,
				p.dead_letter_trace_contexts -> p.parent_id::text as dead_letter_trace_context,
				'Child execution failed: ' || p.child_error, p.parent_attempts, p.child_cancelled,
				pt.dead_letter_queue, pt.dead_letter_task_key
			from failed_parent_targets p
			join pgconductor._private_tasks pt
				on pt.key = p.parent_task_key and pt.queue = p.parent_queue
		)`);
		ctes.push(this.sql`dead_lettered as materialized (
			insert into pgconductor._private_executions (
				task_key, queue, payload, run_at, "group",
				dead_letter_source_execution_id, dead_letter_source_queue,
				dead_letter_source_task_key, dead_letter_error,
				dead_letter_attempts, dead_letter_failed_at, trace_context
			)
			select
				coalesce(p.dead_letter_task_key, p.task_key),
				coalesce(p.dead_letter_queue, p.queue),
				p.payload, nt.ts, p.execution_group,
				p.execution_id, p.queue, p.task_key, p.failure_error,
				p.failure_attempts, nt.ts, p.dead_letter_trace_context
			from terminal_failures p
			cross join now_ts nt
			where p.dead_letter_queue is not null
				and not p.execution_cancelled
			on conflict (dead_letter_source_execution_id, queue, task_key)
				where dead_letter_source_execution_id is not null
				do update set dead_letter_source_execution_id = excluded.dead_letter_source_execution_id
			returning id, dead_letter_source_execution_id, queue, task_key
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
		ctes.push(this.sql`deleted_failed as (
			delete from pgconductor._private_executions e
			where exists (
				select 1 from permanently_failed_children p
				where e.id = p.execution_id and e.queue = p.queue
					and e.locked_by = p.orchestrator_id
					and p.should_remove is true
					and (
						not exists (
							select 1 from task_configs tc
							where tc.key = p.task_key and tc.queue = p.queue
								and tc.dead_letter_queue is not null
						)
						or exists (
							select 1 from dead_lettered d
							where d.dead_letter_source_execution_id = p.execution_id
						)
					)
			)
			or exists (
				select 1 from failed_parent_targets p
				where e.id = p.parent_id and e.queue = p.parent_queue
					and p.parent_should_remove is true
					and (
						not exists (
							select 1 from pgconductor._private_tasks pt
							where pt.key = p.parent_task_key and pt.queue = p.parent_queue
								and pt.dead_letter_queue is not null
						)
						or exists (
							select 1 from dead_lettered d
							where d.dead_letter_source_execution_id = p.parent_id
						)
					)
			)
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
			returning e.id, e.queue
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
			returning e.id, e.queue, e.task_key
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
			insert into pgconductor._private_executions (id, task_key, queue, payload, run_at, parent_execution_id, "group", trace_context)
			select pgconductor._private_portable_uuidv7(), r.child_task_name, r.child_task_queue, r.child_payload, nt.ts, r.execution_id, r."group", r.trace_context
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

		ctes.push(this.sql`settlement_outcomes as (
			select p.queue, p.task_key,
				case when p.execution_cancelled then 'cancellation' else 'permanent_failure' end as outcome
			from permanently_failed_children p
			union all
			select p.parent_queue, p.parent_task_key,
				case when p.child_cancelled then 'cancellation' else 'permanent_failure' end
			from failed_parent_targets p
			union all
			select queue, task_key, 'retry' from retried
			union all
			select t.queue, t.task_key, 'dead_letter'
			from dead_lettered d
			join terminal_failures t on t.execution_id = d.dead_letter_source_execution_id
		)`);

		const combined = ctes.reduce((acc, cte, i) => (i === 0 ? cte : this.sql`${acc}, ${cte}`));
		return this.sql<ReturnExecutionsRow[]>`with ${combined}
			select queue, task_key, outcome, count(*)::integer as count,
				null::uuid as source_execution_id, null::uuid as destination_execution_id,
				null::text as destination_queue, null::text as destination_task_key
			from settlement_outcomes group by queue, task_key, outcome
			union all
			select null::text, null::text, null::text, null::integer,
				d.dead_letter_source_execution_id, d.id, d.queue, d.task_key
			from dead_lettered d`;
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

	buildRemoveProcessedEvents({
		before,
		batchSize,
	}: RemoveProcessedEventsArgs): PendingQuery<[{ deleted_count: number }]> {
		return this.sql<[{ deleted_count: number }]>`
			select pgconductor._private_remove_processed_events(
				${before.toISOString()}::timestamptz, ${batchSize}::integer
			) as deleted_count
		`;
	}

	buildRegisterWorker({
		queueName,
		taskSpecs,
		cronSchedules,
		eventSubscriptions,
	}: RegisterWorkerArgs): PendingQuery<{ cron_rows: CronRegistration[] }[]> {
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
				trace_context: boundedCarrier(spec.trace_context),
			};
		});

		const eventSubscriptionRows = eventSubscriptions.map((spec) => ({
			task_key: spec.task_key,
			queue: spec.queue,
			event_key: spec.event_key,
			schema_name: spec.schema_name,
			table_name: spec.table_name,
			operation: spec.operation,
			when_clause: spec.when_clause,
			payload_fields: spec.payload_fields,
			column_names: spec.column_names,
			filter: spec.filter,
		}));

		return this.sql<{ cron_rows: CronRegistration[] }[]>`
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
			) as cron_rows
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
				p_trace_context := ${boundedCarrier(spec.trace_context) ? this.sql.json(boundedCarrier(spec.trace_context)) : null}::jsonb,
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
				p_trace_context := ${boundedCarrier(spec.trace_context) ? this.sql.json(boundedCarrier(spec.trace_context)) : null}::jsonb,
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
				trace_context: boundedCarrier(spec.trace_context),
				priority: spec.priority,
				group: spec.group || null,
			};
		});

		return this.sql<{ id: string }[]>`
			select id from pgconductor.invoke_batch(
				array(
					select jsonb_populate_recordset(null::pgconductor.execution_spec, ${this.sql.json(JSON.parse(JSON.stringify(specsArray)))}::jsonb)
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

	buildRegisterEventWait({
		executionId,
		queue,
		taskKey,
		eventKey,
		stepKey,
		filter,
		timeoutMs,
		orchestratorId,
	}: RegisterEventWaitArgs): PendingQuery<RowList<{ registered: boolean }[]>> {
		return this.sql<RowList<{ registered: boolean }[]>>`
			with event_wait_lock as materialized (
				select pg_advisory_xact_lock(hashtext('pgconductor:event-waits')) as locked
			), claimed as materialized (
				select e.id, e.queue
				from pgconductor._private_executions e
				cross join event_wait_lock
				where e.id = ${executionId}::uuid and e.queue = ${queue}::text
				  and e.task_key = ${taskKey}::text
				  and e.locked_by = ${orchestratorId}::uuid
				  and e.completed_at is null and e.failed_at is null and e.cancelled = false
				for update
			), event_position_state as materialized (
				select case when sequence_state.is_called then sequence_state.last_value
					else sequence_state.last_value - 1 end as position
				from pgconductor._private_event_position_seq sequence_state
				cross join event_wait_lock
			), inserted as (
				insert into pgconductor._private_event_subscriptions
					(task_key, queue, event_key, filter, kind, execution_id, step_key, expires_at,
					 wait_after_event_position)
				select ${taskKey}::text, ${queue}::text, ${eventKey}::text,
					${filter ? this.sql.json(filter) : null}::jsonb, 'execution_wait',
					${executionId}::uuid, ${stepKey}::text,
					case when ${timeoutMs}::bigint is null then null
						else pgconductor._private_current_time() + (${timeoutMs}::bigint || ' milliseconds')::interval end,
					event_position_state.position
				from claimed cross join event_position_state
				on conflict (execution_id, step_key) where execution_id is not null do nothing
				returning id
			)
			update pgconductor._private_executions e
			set waiting_on_execution_id = null, waiting_step_key = ${stepKey}::text,
				trace_link_context = null,
				run_at = 'infinity'::timestamptz,
				locked_by = null, locked_at = null
			from claimed c
			where e.id = c.id and e.queue = c.queue and exists (select 1 from inserted)
			returning exists (select 1 from inserted) as registered
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
				returning e.id, e.queue
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

	buildEmitEvent({
		eventKey,
		payload,
		trace_context,
	}: EmitEventArgs): PendingQuery<{ id: string }[]> {
		return this.sql<{ id: string }[]>`
			select pgconductor.emit_event(
				${eventKey}::text,
				${this.sql.json(payload || {})}::jsonb,
				${boundedCarrier(trace_context) ? this.sql.json(boundedCarrier(trace_context)) : null}::jsonb
			) as id
		`;
	}
}

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
	taskKeysWithConcurrency: string[];
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
	queue: string;
	orchestratorId: string;
	claimToken: string;
	key: string;
};

export type SaveStepArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
	key: string;
	result: Payload | null;
	runAtMs?: number;
};

export type ClearWaitingStateArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
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
			released_slots as (
				update pgconductor._private_concurrency_slots cs
				set used = 0
				from pgconductor._private_executions e, expired
				where e.locked_by = expired.id
					and e.slot_group_number is not null
					and cs.queue = e.queue
					and cs.task_key = e.task_key
					and cs.slot_group_number = e.slot_group_number
			),
			-- fail cancelled executions from expired orchestrators
			failed_cancelled as (
				update pgconductor._private_executions e
				set
					failed_at = pgconductor._private_current_time(),
					locked_by = null,
					locked_at = null,
					claim_token = null,
					slot_group_number = null
				from expired
				where e.locked_by = expired.id
					and e.cancelled = true
					and e.failed_at is null
					and e.completed_at is null
			)
			-- unlock remaining (non-cancelled) executions
			update pgconductor._private_executions e
			set
				locked_by = null,
				locked_at = null,
				claim_token = null,
				slot_group_number = null
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
			released_slots as (
				update pgconductor._private_concurrency_slots cs
				set used = 0
				from pgconductor._private_executions e, deleted
				where e.locked_by = deleted.id
					and e.slot_group_number is not null
					and cs.queue = e.queue
					and cs.task_key = e.task_key
					and cs.slot_group_number = e.slot_group_number
			),
			-- fail cancelled executions from this orchestrator
			failed_cancelled as (
				update pgconductor._private_executions e
				set
					failed_at = pgconductor._private_current_time(),
					locked_by = null,
					locked_at = null,
					claim_token = null,
					slot_group_number = null
				from deleted
				where e.locked_by = deleted.id
					and e.cancelled = true
					and e.failed_at is null
					and e.completed_at is null
			)
			-- unlock remaining (non-cancelled) executions
			update pgconductor._private_executions e
			set
				locked_by = null,
				locked_at = null,
				claim_token = null,
				slot_group_number = null
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
		taskKeysWithConcurrency,
	}: GetExecutionsArgs): PendingQuery<Execution[]> {
		// fast path: no tasks have concurrency limits
		if (!taskKeysWithConcurrency.length) {
			return this.sql<Execution[]>`
				with e as (
					select
						e.id,
						e.task_key
					from pgconductor._private_executions e
					where e.queue = ${queueName}::text
						${filterTaskKeys?.length ? this.sql`and not (e.task_key = any(${this.sql.array(filterTaskKeys)}::text[]))` : this.sql``}
						and e.run_at <= pgconductor._private_current_time()
						and e.is_available = true
					order by e.priority asc, e.run_at asc, e.enqueue_position asc
					limit ${batchSize}::integer
					for update skip locked
				),

				claimed as (
					update pgconductor._private_executions
					set
						attempts = _private_executions.attempts + 1,
						locked_by = ${orchestratorId}::uuid,
						claim_token = pgconductor._private_portable_uuidv7(),
						locked_at = pgconductor._private_current_time()
					from e
					where _private_executions.id = e.id
						and _private_executions.queue = ${queueName}::text
					returning
						_private_executions.id,
						_private_executions.task_key,
						_private_executions.queue,
						_private_executions.payload,
						_private_executions.waiting_on_execution_id,
						_private_executions.waiting_step_key,
						_private_executions.cancelled,
						_private_executions.last_error,
						_private_executions.dedupe_key,
						_private_executions.cron_expression,
						_private_executions.locked_by,
						_private_executions.claim_token,
						_private_executions.slot_group_number,
						_private_executions.priority,
						_private_executions.run_at,
						_private_executions.enqueue_position
				)
				select
					c.id,
					c.task_key,
					c.queue,
					c.payload,
					c.waiting_on_execution_id,
					c.waiting_step_key,
					c.cancelled,
					c.last_error,
					c.dedupe_key,
					c.cron_expression,
					c.locked_by,
					c.claim_token,
					c.slot_group_number
				from claimed c
				order by c.priority asc, c.run_at asc, c.enqueue_position asc
			`;
		}

		// slow path: some tasks have concurrency limits (OPTIMIZED)
		return this.sql<Execution[]>`
			with
				-- lock up to batchSize slots per concurrency task
				locked_slots_raw as (
					select t.task_key, ls.slot_group_number
					from unnest(${this.sql.array(taskKeysWithConcurrency)}::text[]) as t(task_key)
					cross join lateral (
						select s.slot_group_number
						from pgconductor._private_concurrency_slots s
						where s.task_key = t.task_key
							and s.queue = ${queueName}::text
							and s.used = 0
						order by s.slot_group_number
						limit ${batchSize}::integer
						for update skip locked
					) as ls
				),

				-- count slots per task
				slots_per_task as (
					select task_key, count(*) as slot_count
					from locked_slots_raw
					group by task_key
				),

				-- lock jobs for concurrency tasks (limit by slot count)
				concurrency_execs as (
					select t.task_key, le.*
					from slots_per_task t
					cross join lateral (
						select
							e.id,
							e.task_key as exec_task_key,
							e.queue,
							e.payload,
							e.waiting_on_execution_id,
							e.waiting_step_key,
							e.cancelled,
							e.last_error,
							e.dedupe_key,
							e.cron_expression,
							e.priority,
							e.run_at,
							e.enqueue_position
						from pgconductor._private_executions e
						where e.is_available = true
							and e.run_at <= pgconductor._private_current_time()
							and e.queue = ${queueName}::text
							and e.task_key = t.task_key
							${filterTaskKeys.length ? this.sql`and not (e.task_key = any(${this.sql.array(filterTaskKeys)}::text[]))` : this.sql``}
						order by e.priority asc, e.run_at asc, e.enqueue_position asc
						limit t.slot_count
						for update skip locked
					) as le
				),

				-- lock jobs from non-concurrency tasks
				unlimited_execs as (
					select
						e.id,
						e.task_key,
						e.queue,
						e.payload,
						e.waiting_on_execution_id,
						e.waiting_step_key,
						e.cancelled,
						e.last_error,
						e.dedupe_key,
						e.cron_expression,
						e.priority,
						e.run_at
					from pgconductor._private_executions e
					where e.is_available = true
						and e.run_at <= pgconductor._private_current_time()
						and e.queue = ${queueName}::text
						and not (e.task_key = any(${this.sql.array(taskKeysWithConcurrency)}::text[]))
						${filterTaskKeys.length > 0 ? this.sql`and not (e.task_key = any(${this.sql.array(filterTaskKeys)}::text[]))` : this.sql``}
					order by e.priority asc, e.run_at asc, e.enqueue_position asc
					limit ${batchSize}::integer
					for update skip locked
				),

				-- row number concurrency executions by task
				concurrency_execs_rn as (
					select
						ce.*,
						row_number() over (partition by ce.task_key order by ce.priority asc, ce.run_at asc, ce.enqueue_position asc) as exec_rn
					from concurrency_execs ce
				),

				-- row number slots by task
				slots_rn as (
					select
						ls.*,
						row_number() over (partition by ls.task_key order by ls.slot_group_number) as slot_rn
					from locked_slots_raw ls
				),

				-- pair concurrency executions with slots
				concurrency_paired as (
					select
						e.id,
						e.exec_task_key as task_key,
						e.queue,
						e.payload,
						e.waiting_on_execution_id,
						e.waiting_step_key,
						e.cancelled,
						e.last_error,
						e.dedupe_key,
						e.cron_expression,
						s.slot_group_number
					from concurrency_execs_rn e
					join slots_rn s
						on e.task_key = s.task_key
						and e.exec_rn = s.slot_rn
				),

				-- unlimited executions don't need slots
				unlimited_paired as (
					select
						id,
						task_key,
						queue,
						payload,
						waiting_on_execution_id,
						waiting_step_key,
						cancelled,
						last_error,
						dedupe_key,
						cron_expression,
						null::integer as slot_group_number
					from unlimited_execs
				),

				-- merge all paired executions
				paired as (
					select * from concurrency_paired
					union all
					select * from unlimited_paired
				),

				-- mark slots as used
				mark_used as (
					update pgconductor._private_concurrency_slots cs
					set used = 1
					from paired p
					where cs.queue = ${queueName}::text
						and cs.task_key = p.task_key
						and cs.slot_group_number = p.slot_group_number
						and p.slot_group_number is not null
				),

			-- update and return executions. The outer select owns result ordering;
			-- update returning order is not defined.
			claimed as (
				update pgconductor._private_executions e
				set
					attempts = e.attempts + 1,
					locked_by = ${orchestratorId}::uuid,
					claim_token = pgconductor._private_portable_uuidv7(),
					slot_group_number = p.slot_group_number,
					locked_at = pgconductor._private_current_time()
				from paired p
				where e.id = p.id
					and e.queue = p.queue
				returning
					e.id,
					e.task_key,
					e.queue,
					e.payload,
					e.waiting_on_execution_id,
					e.waiting_step_key,
					e.cancelled,
					e.last_error,
					e.dedupe_key,
					e.cron_expression,
					e.locked_by,
					e.claim_token,
					e.slot_group_number,
					e.priority,
					e.run_at,
					e.enqueue_position
			)
			select
				c.id,
				c.task_key,
				c.queue,
				c.payload,
				c.waiting_on_execution_id,
				c.waiting_step_key,
				c.cancelled,
				c.last_error,
				c.dedupe_key,
				c.cron_expression,
				c.locked_by,
				c.claim_token,
				c.slot_group_number
			from claimed c
			order by c.priority asc, c.run_at asc, c.enqueue_position asc
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
				orchestrator_id uuid, claim_token uuid, result jsonb, error text,
				reschedule_in_ms text, step_key text, timeout_ms text,
				child_task_name text, child_task_queue text, child_payload jsonb,
				slot_group_number integer
			)
		)`);
		// Lock the claimed rows for the whole statement. This prevents recovery or a
		// new claim from racing the side effects below.
		ctes.push(this.sql`valid_results as materialized (
			select r.*, e.slot_group_number as owned_slot_group_number,
				e.cancelled as execution_cancelled, e.last_error as execution_last_error
			from result_data r
			join pgconductor._private_executions e
				on e.id = r.execution_id
				and e.queue = r.queue
				and e.task_key = r.task_key
				and e.locked_by = r.orchestrator_id
				and e.claim_token = r.claim_token
			for update of e
		)`);
		ctes.push(this.sql`task_configs as (
			select queue, key, max_attempts, remove_on_complete_days, remove_on_fail_days
			from pgconductor._private_tasks
			where queue = any(${this.sql.array(Array.from(new Set(allResults.map((r) => r.queue))))}::text[])
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

		ctes.push(this.sql`released_slots as (
			update pgconductor._private_concurrency_slots cs
			set used = 0
			from valid_results r
			where r.owned_slot_group_number is not null
				and cs.queue = r.queue
				and cs.task_key = r.task_key
				and cs.slot_group_number = r.owned_slot_group_number
				and cs.used > 0
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
				locked_by = null, locked_at = null, claim_token = null, slot_group_number = null
			from now_ts nt, completed_results r
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id and e.claim_token = r.claim_token
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
				locked_by = null, locked_at = null, claim_token = null, slot_group_number = null
			from now_ts nt, completed_parents p
			where e.id = p.parent_id and e.queue = p.queue
			returning e.id
		)`);
		ctes.push(this.sql`deleted_completed as (
			delete from pgconductor._private_executions e
			using completed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id and e.claim_token = r.claim_token
				and tc.key = r.task_key and tc.queue = r.queue and tc.remove_on_complete_days = 0
				and not exists (select 1 from orphaned_children oc where oc.id = e.id)
			returning e.id
		)`);
		ctes.push(this.sql`updated_completed as (
			update pgconductor._private_executions e
			set completed_at = nt.ts, locked_by = null, locked_at = null,
				claim_token = null, slot_group_number = null
			from now_ts nt, completed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id and e.claim_token = r.claim_token
				and tc.key = r.task_key and tc.queue = r.queue
				and (tc.remove_on_complete_days is null or tc.remove_on_complete_days != 0)
				and not exists (select 1 from orphaned_children oc where oc.id = e.id)
			returning e.id
		)`);

		ctes.push(this.sql`permanently_failed_children as materialized (
			select r.execution_id, r.queue, r.task_key, r.orchestrator_id, r.claim_token,
				coalesce(r.error, r.execution_last_error, 'unknown error') as child_error,
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
				parent.id as parent_id, parent.queue as parent_queue,
				pt.remove_on_fail_days = 0 as parent_should_remove
			from permanently_failed_children p
			join pgconductor._private_executions parent
				on parent.waiting_on_execution_id = p.execution_id
			join pgconductor._private_tasks pt
				on pt.key = parent.task_key and pt.queue = parent.queue
			where parent.completed_at is null and parent.failed_at is null and parent.locked_by is null
			for update of parent
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
					and e.locked_by = p.orchestrator_id and e.claim_token = p.claim_token
					and p.should_remove is true
			)
			or exists (
				select 1 from failed_parent_targets p
				where e.id = p.parent_id and e.queue = p.parent_queue
					and p.parent_should_remove is true
			)
			returning e.id
		)`);
		ctes.push(this.sql`updated_failed as (
			update pgconductor._private_executions e
			set failed_at = nt.ts,
				last_error = case when f.is_child then coalesce(f.child_error, 'unknown error')
					else 'Child execution failed: ' || coalesce(f.child_error, 'unknown error') end,
				waiting_on_execution_id = null, waiting_step_key = null,
				locked_by = null, locked_at = null, claim_token = null, slot_group_number = null
			from now_ts nt, failed_updates f
			where e.id = f.target_id and e.queue = f.queue
			returning e.id
		)`);
		ctes.push(this.sql`retried as (
			update pgconductor._private_executions e
			set last_error = coalesce(r.error, 'unknown error'),
				run_at = greatest(nt.ts, coalesce(e.run_at, nt.ts)) +
					((array[15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200])[least(greatest(e.attempts, 1), 10)] * interval '1 second'),
				locked_by = null, locked_at = null, claim_token = null, slot_group_number = null
			from now_ts nt, failed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id and e.claim_token = r.claim_token
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
				locked_by = null, locked_at = null, claim_token = null, slot_group_number = null
			from now_ts nt, released_results r
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id and e.claim_token = r.claim_token
			returning e.id
		)`);

		ctes.push(this.sql`inserted_children as (
			insert into pgconductor._private_executions (id, task_key, queue, payload, run_at, parent_execution_id)
			select pgconductor._private_portable_uuidv7(), r.child_task_name, r.child_task_queue, r.child_payload, nt.ts, r.execution_id
			from invoke_child_data r, now_ts nt
			where exists (
				select 1 from pgconductor._private_executions parent
				where parent.id = r.execution_id and parent.queue = r.queue
					and parent.locked_by = r.orchestrator_id and parent.claim_token = r.claim_token
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
				locked_by = null, locked_at = null, claim_token = null, slot_group_number = null
			from now_ts nt, inserted_children ic
			join invoke_child_data r on r.execution_id = ic.parent_execution_id
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = r.orchestrator_id and e.claim_token = r.claim_token
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
				p_priority := ${spec.priority || null}::integer
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
				p_priority := ${spec.priority || 0}::integer
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
		claimToken,
		key,
	}: LoadStepArgs): PendingQuery<[{ result: Payload | null }]> {
		return this.sql<[{ result: Payload | null }]>`
			select result from pgconductor._private_steps
			where execution_id = ${executionId}::uuid
				and queue = ${queue}::text
				and exists (
					select 1 from pgconductor._private_executions e
					where e.id = ${executionId}::uuid and e.queue = ${queue}::text
						and e.locked_by = ${orchestratorId}::uuid and e.claim_token = ${claimToken}::uuid
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
		claimToken,
	}: SaveStepArgs): PendingQuery<RowList<Row[]>> {
		if (runAtMs) {
			return this.sql<RowList<Row[]>>`
				with claimed_execution as materialized (
					select e.id, e.queue
					from pgconductor._private_executions e
					where e.id = ${executionId}::uuid
						and e.queue = ${queue}::text
						and e.locked_by = ${orchestratorId}::uuid
						and e.claim_token = ${claimToken}::uuid
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
					and e.claim_token = ${claimToken}::uuid
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
		claimToken,
	}: ClearWaitingStateArgs): PendingQuery<RowList<Row[]>> {
		return this.sql<RowList<Row[]>>`
			with claimed_parent as materialized (
				select e.id, e.queue, e.waiting_on_execution_id
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
					and e.claim_token = ${claimToken}::uuid
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
					locked_at = null,
					claim_token = null
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

	buildEmitEvent({ eventKey, payload }: EmitEventArgs): PendingQuery<{ id: string }[]> {
		return this.sql<{ id: string }[]>`
			select pgconductor.emit_event(
				${eventKey}::text,
				${this.sql.json(payload || {})}::jsonb
			) as id
		`;
	}
}

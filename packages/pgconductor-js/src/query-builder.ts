import type { PendingQuery, Row, RowList, Sql } from "postgres";
import type { ReturnExecutionsArgs } from "./database-client.js";
import * as assert from "./lib/assert.js";
import type { TraceContextCarrier } from "./telemetry.js";
import type {
	ExecutionInfo,
	ExecutionSpec,
	EventFilterTerm,
	EventSubscriptionSpec,
	EventWaitResult,
	JsonValue,
	EventRaceBranch,
	EventRaceResult,
	Payload,
	ClaimResult,
	TaskSpec,
} from "./database-client.js";

export type RegisterOrchestratorArgs = {
	orchestratorId: string;
	version: string;
	migrationNumber: number;
};

export type OrchestratorHeartbeatArgs = {
	orchestratorId: string;
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
	taskKeys: string[];
	/** Running executions, to read their cancellation and signals stored after `after_seq` */
	running?: { execution_id: string; claim_token: string; after_seq: number | null }[];
	/** Executions the worker holds; its other claims in the queue are released */
	held?: string[];
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

export type ScheduleNextCronExecutionArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
	runAt: Date;
	dedupeKey: string;
};

export type GetExecutionArgs = {
	executionId: string;
	task?: { key: string; queue: string };
};

export type LoadStepArgs = {
	executionId: string;
	queue: string;
	key: string;
};

export type CountStepsArgs = {
	executionIds: string[];
	key: string;
};

export type SaveStepArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
	key: string;
	result: Payload | null;
};

export type StartExecutionArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
	key: string;
	spec: ExecutionSpec & { dedupe_key: string };
	cancelWithParent: boolean;
};

export type ClearWaitingStateArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
};

export type EmitEventArgs = {
	eventKey: string;
	payload?: Payload;
	traceContext?: TraceContextCarrier | null;
	metadata?: Payload | null;
	dedupeKey?: string;
};

export type AckSignalsArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
	id: string;
};

export type SignalArgs = {
	name: string;
	id: string;
	payload?: JsonValue;
};

export type SignalExecutionArgs = {
	executionId: string;
	signal: SignalArgs;
};

export type SignalWithStartArgs = {
	taskKey: string;
	queue: string;
	key: string;
	signal: SignalArgs;
	input?: Payload | null;
	priority?: number | null;
	traceContext?: TraceContextCarrier | null;
	metadata?: Payload | null;
};

export type SignalWithStartStepArgs = {
	executionId: string;
	queue: string;
	orchestratorId: string;
	claimToken: string;
	key: string;
	target: SignalWithStartArgs;
};

export type RegisterEventWaitArgs = {
	executionId: string;
	queue: string;
	taskKey: string;
	eventKey: string;
	stepKey: string;
	requiredFieldCount: number;
	terms: EventFilterTerm[];
	timeoutMs: number | null;
	orchestratorId: string;
	suspend: boolean;
	claimToken: string;
};

export type RegisterEventRaceArgs = {
	executionId: string;
	queue: string;
	taskKey: string;
	stepKey: string;
	branches: EventRaceBranch[];
	timeoutMs: number | null;
	orchestratorId: string;
	claimToken: string;
	deliveringSignalId: string | null;
};

export class QueryBuilder {
	constructor(private readonly sql: Sql) {}

	buildRegisterOrchestrator({
		orchestratorId,
		version,
		migrationNumber,
	}: RegisterOrchestratorArgs): PendingQuery<[{ shutdown_reason: string | null }]> {
		return this.sql<[{ shutdown_reason: string | null }]>`
			insert into pgconductor._private_orchestrators as o (
				id,
				version,
				migration_number,
				last_heartbeat_at
			)
			values (
				${orchestratorId}::uuid,
				${version}::text,
				${migrationNumber}::integer,
				pgconductor._private_current_time()
			)
			on conflict (id) do update
			set last_heartbeat_at = excluded.last_heartbeat_at
			returning coalesce(
				o.shutdown_reason,
				case when (select max(version) from pgconductor.schema_migrations) > o.migration_number
					then 'newer_migration_detected' end
			) as shutdown_reason
		`;
	}

	/** Update only: no row is returned once the orchestrator was recovered as stale */
	buildOrchestratorHeartbeat({
		orchestratorId,
	}: OrchestratorHeartbeatArgs): PendingQuery<{ shutdown_reason: string | null }[]> {
		return this.sql<{ shutdown_reason: string | null }[]>`
			update pgconductor._private_orchestrators o
			set last_heartbeat_at = pgconductor._private_current_time()
			where o.id = ${orchestratorId}::uuid
			returning coalesce(
				o.shutdown_reason,
				case when (select max(version) from pgconductor.schema_migrations) > o.migration_number
					then 'newer_migration_detected' end
			) as shutdown_reason
		`;
	}

	buildAckSignals({
		executionId,
		queue,
		orchestratorId,
		claimToken,
		id,
	}: AckSignalsArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			with claimed_execution as materialized (
				select e.id
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
					and e.claim_token = ${claimToken}::uuid
				for update
			)
			update pgconductor._private_execution_signals s
			set delivered_at = pgconductor._private_current_time()
			from claimed_execution c
			where s.execution_id = c.id
				and s.id = ${id}::text
				and s.delivered_at is null
		`;
	}

	buildSignalExecution({
		executionId,
		signal,
	}: SignalExecutionArgs): PendingQuery<[{ stored: boolean }]> {
		return this.sql<[{ stored: boolean }]>`
			select pgconductor.signal_execution(
				${executionId}::uuid,
				${signal.name}::text,
				${signal.id}::text,
				${signal.payload === undefined ? null : this.sql.json(signal.payload)}::jsonb
			) as stored
		`;
	}

	buildSignalWithStart({
		taskKey,
		queue,
		key,
		signal,
		input,
		priority,
		traceContext,
		metadata,
	}: SignalWithStartArgs): PendingQuery<[{ id: string }]> {
		return this.sql<[{ id: string }]>`
			select pgconductor.signal_with_start(
				p_task_key := ${taskKey}::text,
				p_queue := ${queue}::text,
				p_key := ${key}::text,
				p_name := ${signal.name}::text,
				p_id := ${signal.id}::text,
				p_payload := ${signal.payload === undefined ? null : this.sql.json(signal.payload)}::jsonb,
				p_input := ${input ? this.sql.json(input) : null}::jsonb,
				p_trace_context := ${traceContext ? this.sql.json(traceContext) : null}::jsonb,
				p_metadata := ${metadata ? this.sql.json(metadata) : null}::jsonb,
				p_priority := ${priority || null}::integer
			) as id
		`;
	}

	// Memoizes the id as a step of the claimed execution in the same statement, so a replay
	// returns the id stored by the first call
	buildSignalWithStartStep({
		executionId,
		queue,
		orchestratorId,
		claimToken,
		key,
		target,
	}: SignalWithStartStepArgs): PendingQuery<{ id: string }[]> {
		const { signal, input, priority, traceContext, metadata } = target;
		return this.sql<{ id: string }[]>`
			with claimed_execution as materialized (
				select e.id, e.queue
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
					and e.claim_token = ${claimToken}::uuid
				for update
			), stored_step as (
				select (s.result->>'result')::uuid as id
				from pgconductor._private_steps s, claimed_execution c
				where s.execution_id = c.id and s.key = ${key}::text
			), signalled as materialized (
				select pgconductor.signal_with_start(
					p_task_key := ${target.taskKey}::text,
					p_queue := ${target.queue}::text,
					p_key := ${target.key}::text,
					p_name := ${signal.name}::text,
					p_id := ${signal.id}::text,
					p_payload := ${signal.payload === undefined ? null : this.sql.json(signal.payload)}::jsonb,
					p_input := ${input ? this.sql.json(input) : null}::jsonb,
					p_trace_context := ${traceContext ? this.sql.json(traceContext) : null}::jsonb,
					p_metadata := ${metadata ? this.sql.json(metadata) : null}::jsonb,
					p_priority := ${priority || null}::integer
				) as id
				from claimed_execution
				where not exists (select from stored_step)
			), saved_step as (
				insert into pgconductor._private_steps (execution_id, queue, key, result)
				select c.id, c.queue, ${key}::text, jsonb_build_object('result', s.id)
				from claimed_execution c, signalled s
			)
			select id from stored_step
			union all
			select id from signalled
		`;
	}

	buildRecoverStaleOrchestrators({
		maxAge,
	}: RecoverStaleOrchestratorsArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			select pgconductor._private_remove_orchestrators(p_max_age := ${maxAge}::interval)
		`;
	}

	buildSweepOrchestrators({
		migrationNumber,
	}: SweepOrchestratorsArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			update pgconductor._private_orchestrators
			set shutdown_reason = 'breaking_migration'
			where migration_number < ${migrationNumber}::integer
		`;
	}

	buildGetInstalledMigrationNumber(): PendingQuery<[{ version: number }]> {
		return this.sql<[{ version: number }]>`
			select coalesce(max(version), -1) as version
			from pgconductor.schema_migrations
		`;
	}

	buildOrchestratorShutdown({
		orchestratorId,
	}: OrchestratorShutdownArgs): PendingQuery<RowList<Row[]>> {
		return this.sql`
			select pgconductor._private_remove_orchestrators(p_id := ${orchestratorId}::uuid)
		`;
	}

	buildGetExecutions({
		orchestratorId,
		queueName,
		batchSize,
		taskKeys,
		running = [],
		held,
	}: GetExecutionsArgs): PendingQuery<[ClaimResult]> {
		return this.sql<[ClaimResult]>`
			-- Read limit mode from the database so rolling workers cannot bypass new limits.
			with task_limits as materialized (
				select exists (
					select 1
					from pgconductor._private_tasks t
					where t.queue = ${queueName}::text
						and (t.concurrency_limit is not null or t.group_concurrency_limit is not null)
				) as enabled
			),
			-- Locking the owner row keeps recovery from deleting it while the claim is in flight. An
			-- owner that was already deleted claims nothing.
			claim_clock as materialized (
				select pgconductor._private_current_time() as now_ts
				from pgconductor._private_orchestrators o
				where o.id = ${orchestratorId}::uuid
				for key share
			), unconstrained_candidates as (
				select e.id
				from pgconductor._private_executions e
				where not (select enabled from task_limits)
					and e.queue = ${queueName}::text
					and e.run_at <= (select now_ts from claim_clock)
					and e.is_available = true
					and e.task_key = any(${this.sql.array(taskKeys, 1009)}::text[])
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
			-- Full tasks are excluded here so their backlog is never scanned.
			available_tasks as materialized (
				select
					t.key,
					t.concurrency_limit,
					t.group_concurrency_limit,
					coalesce(at.active_count, 0) as active_task_count
				from pgconductor._private_tasks t
				left join active_tasks at on at.task_key = t.key
				where (select enabled from task_limits)
					and t.queue = ${queueName}::text
					and (t.concurrency_limit is null
						or coalesce(at.active_count, 0) < t.concurrency_limit)
					and t.key = any(${this.sql.array(taskKeys, 1009)}::text[])
			), candidates as (
				select c.*
				from available_tasks t
				cross join lateral (
					select
						e.id,
						e.task_key,
						e.priority,
						e.run_at,
						e.created_at,
						e."group",
						t.concurrency_limit,
						t.group_concurrency_limit,
						t.active_task_count,
						coalesce(ag.active_count, 0) as active_group_count
					from pgconductor._private_executions e
					left join active_groups ag on ag.task_key = e.task_key and ag."group" = e."group"
					where e.queue = ${queueName}::text
						and e.task_key = t.key
						and e.run_at <= (select now_ts from claim_clock)
						and e.is_available = true
						and (t.group_concurrency_limit is null
							or e."group" is null
							or coalesce(ag.active_count, 0) < t.group_concurrency_limit)
					order by e.priority asc, e.run_at asc, e.created_at asc, e.id asc
					limit least(${batchSize}::integer, t.concurrency_limit - t.active_task_count)
					for update of e skip locked
				) c
				-- Bound both window functions to one locked candidate batch.
				order by c.priority asc, c.run_at asc, c.created_at asc, c.id asc
				limit ${batchSize}::integer
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
					-- A running execution is no longer suspended at an event wait
					waiting_step_key = case when e.waiting_on_execution_id is null then null
						else e.waiting_step_key end,
					locked_by = ${orchestratorId}::uuid,
					locked_at = (select now_ts from claim_clock),
					claim_token = gen_random_uuid()
				from claimable c
				where e.id = c.id and e.queue = ${queueName}::text and e.is_available = true
				returning e.id, e.task_key, e.queue, e.payload, e.waiting_on_execution_id,
					e.waiting_step_key, e.cancelled, e.last_error, e.dedupe_key, e.cron_expression,
					e.locked_by, e.claim_token, e."group", e.priority, e.run_at, e.created_at,
					e.subscription_id, e.trace_context, e.metadata, e.dead_letter, e.signal_count
			),${
				held
					? this.sql`
			-- Release claims the worker does not hold, such as one whose response was lost. A worker
			-- claims one statement at a time, so no claim it does not hold is in flight.
			released_orphans as (
				update pgconductor._private_executions e
				set
					locked_by = null,
					locked_at = null
				from claim_clock
				where e.queue = ${queueName}::text
					and e.locked_by = ${orchestratorId}::uuid
					and not (e.id = any(${this.sql.array(held, 2951)}::uuid[]))
			),`
					: this.sql``
			} running as materialized (
				select e.id, e.cancelled, r.after_seq
				from jsonb_to_recordset(${this.sql.json(running)}::jsonb)
					as r(execution_id uuid, claim_token uuid, after_seq bigint)
				join pgconductor._private_executions e
					on e.id = r.execution_id
					and e.queue = ${queueName}::text
					and e.locked_by = ${orchestratorId}::uuid
					and e.claim_token = r.claim_token
			), running_signals as (
				-- an execution that listens for no signal has a null after_seq
				select s.execution_id, s.seq, s.id, s.name, s.payload
				from running r
				join pgconductor._private_execution_signals s
					on s.execution_id = r.id
					and s.delivered_at is null
					and s.seq > r.after_seq
				where not r.cancelled
			)
			select
				coalesce((
					select jsonb_agg(
						(to_jsonb(c) - 'priority' - 'run_at' - 'created_at' - 'signal_count')
						-- Signals stored before the attempt starts are delivered before the next poll.
						-- The claim holds the row lock senders take, so later ones have a higher seq.
						|| jsonb_build_object('signals', case when c.signal_count > 0 then coalesce((
							select jsonb_agg(
								jsonb_build_object('seq', s.seq, 'id', s.id, 'name', s.name, 'payload', s.payload)
								order by s.seq
							)
							from pgconductor._private_execution_signals s
							where s.execution_id = c.id and s.delivered_at is null
						), '[]'::jsonb) else '[]'::jsonb end)
						order by c.priority asc, c.run_at asc, c.created_at asc, c.id asc
					)
					from claimed c
				), '[]'::jsonb) as executions,
				coalesce((
					select jsonb_agg(to_jsonb(s) order by s.seq) from running_signals s
				), '[]'::jsonb) as signals,
				coalesce((
					select jsonb_agg(r.id) from running r where r.cancelled
				), '[]'::jsonb) as cancelled
		`;
	}

	buildReturnExecutions({
		orchestratorId,
		results,
	}: ReturnExecutionsArgs): PendingQuery<any> | null {
		if (results.length === 0) return null;

		const toMs = (ms: number | "infinity" | undefined) => (ms === "infinity" ? -1 : ms);
		const rows = results.map((r) => {
			if (r.status === "released") return { ...r, reschedule_in_ms: toMs(r.reschedule_in_ms) };
			if (r.status === "invoke_child") return { ...r, timeout_ms: toMs(r.timeout_ms) };
			return r;
		});

		const ctes: PendingQuery<any>[] = [];
		ctes.push(this.sql`now_ts as (select pgconductor._private_current_time() as ts)`);
		ctes.push(this.sql`result_data as (
			select * from jsonb_to_recordset(${this.sql.json(rows)}::jsonb)
			as r(
				execution_id uuid, queue text, task_key text, claim_token uuid, status text,
				result jsonb, error text,
				reschedule_in_ms bigint, step_key text, timeout_ms bigint,
				child_task_name text, child_task_queue text, child_payload jsonb,
				"group" text, trace_context jsonb, metadata jsonb, signal_names jsonb
			)
		)`);
		// Lock the claimed rows for the whole statement. This prevents recovery or a
		// new claim from racing the side effects below.
		ctes.push(this.sql`valid_results as materialized (
			select r.*,
				e.cancelled as execution_cancelled, e.last_error as execution_last_error,
				e.subscription_id, e.signal_count as execution_signal_count, e.result_waiters
			from result_data r
			join pgconductor._private_executions e
				on e.id = r.execution_id
				and e.queue = r.queue
				and e.task_key = r.task_key
				and e.locked_by = ${orchestratorId}::uuid
				and e.claim_token = r.claim_token
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
		ctes.push(this.sql`completed_candidates as (
			select * from valid_results where status = 'completed' and not execution_cancelled
		)`);
		// A completion must not drop signals the handler did not get. signal_count comes from the
		// locked row, so it counts signals committed after this statement's snapshot, which the
		// count below cannot see. Signals named by no listener of the attempt do not re-queue it.
		ctes.push(this.sql`signal_requeued as materialized (
			select c.execution_id, c.queue
			from completed_candidates c
			where c.execution_signal_count > 0
				and (
					c.execution_signal_count > (
						select count(*) from pgconductor._private_execution_signals s
						where s.execution_id = c.execution_id
					)
					or exists (
						select 1 from pgconductor._private_execution_signals s
						where s.execution_id = c.execution_id
							and s.delivered_at is null
							and s.name in (
								select jsonb_array_elements_text(coalesce(c.signal_names, '[]'::jsonb))
							)
					)
				)
		)`);
		ctes.push(this.sql`completed_results as (
			select c.* from completed_candidates c
			where not exists (
				select 1 from signal_requeued q where q.execution_id = c.execution_id
			)
		)`);
		// A new attempt runs right away and receives the signals; the result is discarded
		ctes.push(this.sql`updated_signal_requeued as (
			update pgconductor._private_executions e
			set attempts = greatest(e.attempts - 1, 0), run_at = nt.ts,
				locked_by = null, locked_at = null
			from now_ts nt, signal_requeued q
			where e.id = q.execution_id and e.queue = q.queue
				and e.locked_by = ${orchestratorId}::uuid
			returning e.id
		)`);
		ctes.push(this.sql`failed_results as (
			select * from valid_results
			where status in ('failed', 'permanently_failed') or execution_cancelled
		)`);
		ctes.push(this.sql`released_results as (
			select * from valid_results where status = 'released' and not execution_cancelled
		)`);
		ctes.push(this.sql`invoke_child_data as (
			select * from valid_results where status = 'invoke_child' and not execution_cancelled
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
				and e.locked_by = ${orchestratorId}::uuid
				and e.parent_execution_id is not null
				and e.subscription_id is null
				and not e.cancel_with_parent
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
				and e.locked_by = ${orchestratorId}::uuid
				and tc.key = r.task_key and tc.queue = r.queue and tc.remove_on_complete_days = 0
				and not exists (select 1 from orphaned_children oc where oc.id = e.id)
			returning e.id
		)`);
		ctes.push(this.sql`updated_completed as (
			update pgconductor._private_executions e
			set completed_at = nt.ts, result = r.result, locked_by = null, locked_at = null
			from now_ts nt, completed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = ${orchestratorId}::uuid
				and tc.key = r.task_key and tc.queue = r.queue
				and (tc.remove_on_complete_days is null or tc.remove_on_complete_days != 0)
				and not exists (select 1 from orphaned_children oc where oc.id = e.id)
			returning e.id, e.queue
		)`);

		ctes.push(this.sql`permanently_failed_children as materialized (
			select r.execution_id, r.queue, r.task_key,
				r.execution_cancelled,
				-- the cancellation reason on the locked row, not the attempt's view of it
				coalesce(case when r.execution_cancelled then r.execution_last_error end,
					r.error, r.execution_last_error, 'unknown error') as child_error,
				e."group" as execution_group,
				e.payload as execution_payload,
				e.attempts as execution_attempts,
				e.subscription_id,
				e.trace_context,
				e.metadata,
				r.result_waiters,
				tc.remove_on_fail_days = 0 as should_remove,
				tc.dead_letter_queue, tc.dead_letter_task_key
			from failed_results r
			join pgconductor._private_executions e on e.id = r.execution_id and e.queue = r.queue
			join task_configs tc on tc.key = r.task_key and tc.queue = r.queue
			where e.attempts >= tc.max_attempts
				or r.status = 'permanently_failed'
				or r.execution_cancelled
		)`);
		// A terminal failure fails every workflow still waiting on it. Ancestors are
		// locked child first, then the chain is followed again on the locked rows.
		ctes.push(this.sql`waiting_ancestors as (
			select parent.id, 1 as depth
			from permanently_failed_children p
			join pgconductor._private_executions parent
				on parent.waiting_on_execution_id = p.execution_id
			where p.subscription_id is null
				and parent.completed_at is null and parent.failed_at is null and parent.locked_by is null
			union all
			select parent.id, a.depth + 1
			from waiting_ancestors a
			join pgconductor._private_executions parent
				on parent.waiting_on_execution_id = a.id
			where parent.completed_at is null and parent.failed_at is null and parent.locked_by is null
		)`);
		ctes.push(this.sql`locked_ancestors as materialized (
			select parent.id as parent_id, parent.queue as parent_queue,
				parent.waiting_on_execution_id,
				parent.task_key as parent_task_key, parent."group" as parent_group,
				parent.payload as parent_payload, parent.attempts as parent_attempts,
				parent.trace_context as parent_trace_context,
				parent.metadata as parent_metadata,
				parent.result_waiters as parent_result_waiters,
				pt.remove_on_fail_days = 0 as parent_should_remove,
				pt.dead_letter_queue as parent_dead_letter_queue,
				pt.dead_letter_task_key as parent_dead_letter_task_key
			from waiting_ancestors a
			join pgconductor._private_executions parent on parent.id = a.id
			join pgconductor._private_tasks pt
				on pt.key = parent.task_key and pt.queue = parent.queue
			where parent.completed_at is null and parent.failed_at is null and parent.locked_by is null
			order by a.depth
			for update of parent
		)`);
		ctes.push(this.sql`failed_parent_targets as (
			select p.child_error, p.execution_cancelled as child_cancelled, a.*
			from permanently_failed_children p
			join locked_ancestors a on a.waiting_on_execution_id = p.execution_id
			where p.subscription_id is null
			union all
			select f.child_error, f.child_cancelled, a.*
			from failed_parent_targets f
			join locked_ancestors a on a.waiting_on_execution_id = f.parent_id
		)`);
		ctes.push(this.sql`terminal_failures as materialized (
			select p.execution_id, p.queue, p.task_key, p.execution_group, p.execution_payload as payload,
				p.child_error as failure_error, p.execution_attempts as failure_attempts,
				p.execution_cancelled, p.dead_letter_queue, p.dead_letter_task_key, p.trace_context,
				p.metadata
			from permanently_failed_children p
			union all
			select p.parent_id, p.parent_queue, p.parent_task_key, p.parent_group, p.parent_payload,
				'Child execution failed: ' || p.child_error, p.parent_attempts, p.child_cancelled,
				p.parent_dead_letter_queue, p.parent_dead_letter_task_key, p.parent_trace_context,
				p.parent_metadata
			from failed_parent_targets p
		)`);
		ctes.push(this.sql`dead_lettered as (
			insert into pgconductor._private_executions (task_key, queue, payload, run_at, "group", trace_context, metadata, dead_letter)
			select
				coalesce(p.dead_letter_task_key, p.task_key),
				coalesce(p.dead_letter_queue, p.queue),
				p.payload, nt.ts, p.execution_group, p.trace_context, p.metadata,
				jsonb_build_object(
					'sourceExecutionId', p.execution_id,
					'sourceQueue', p.queue,
					'sourceTaskKey', p.task_key,
					'error', p.failure_error,
					'attempts', p.failure_attempts,
					'failedAt', nt.ts
				)
			from terminal_failures p
			cross join now_ts nt
			where p.dead_letter_queue is not null
				and not p.execution_cancelled
			on conflict ((dead_letter->>'sourceExecutionId'), queue, task_key) where dead_letter is not null
				do nothing
			returning id
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
			select p.execution_id as target_id, p.queue, ${orchestratorId}::uuid as expected_locked_by
			from permanently_failed_children p
			where p.should_remove is true
			union all
			select p.parent_id, p.parent_queue, null::uuid
			from failed_parent_targets p
			where p.parent_should_remove is true
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
			returning e.id, e.queue
		)`);
		ctes.push(this.sql`cleaned_terminal_event_waits as (
			delete from pgconductor._private_custom_event_subscriptions subscription
			using (
				select id, queue from updated_completed
				union all
				select id, queue from updated_failed
			) terminal
			where subscription.kind = 'execution_wait'
				and subscription.execution_id = terminal.id
				and subscription.queue = terminal.queue
			returning subscription.id
		)`);
		// Waits for a settled execution are read from its locked row, which also holds waits
		// registered after this statement's snapshot
		ctes.push(this.sql`resolved_result_waits as (
			select pgconductor._private_resolve_result_waits(jsonb_agg(w.value || jsonb_build_object('outcome', s.outcome)))
			from (
				select r.result_waiters,
					case when oc.id is null
						then jsonb_build_object('status', 'completed', 'result', r.result)
						else jsonb_build_object('status', 'failed',
							'error', 'Parent timed out before child completed')
					end as outcome
				from completed_results r
				left join orphaned_children oc on oc.id = r.execution_id
				union all
				select p.result_waiters,
					case when p.execution_cancelled
						then jsonb_build_object('status', 'cancelled', 'reason', p.child_error)
						else jsonb_build_object('status', 'failed', 'error', p.child_error)
					end
				from permanently_failed_children p
				union all
				select p.parent_result_waiters,
					jsonb_build_object('status', 'failed', 'error', 'Child execution failed: ' || p.child_error)
				from failed_parent_targets p
			) s
			cross join lateral jsonb_array_elements(s.result_waiters) w
			having count(*) > 0
		)`);
		ctes.push(this.sql`retried as (
			update pgconductor._private_executions e
			set last_error = coalesce(r.error, 'unknown error'),
				run_at = greatest(nt.ts, coalesce(e.run_at, nt.ts)) +
					((array[15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200])[least(greatest(e.attempts, 1), 10)] * interval '1 second'),
				locked_by = null, locked_at = null
			from now_ts nt, failed_results r, task_configs tc
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = ${orchestratorId}::uuid
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
			returning execution_id
		)`);
		ctes.push(this.sql`updated_released as (
			update pgconductor._private_executions e
			set attempts = greatest(e.attempts - 1, 0),
				run_at = case when r.reschedule_in_ms = -1 then 'infinity'::timestamptz
					else nt.ts + coalesce(r.reschedule_in_ms, 0) * interval '1 millisecond' end,
				locked_by = null, locked_at = null
			from now_ts nt, released_results r
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = ${orchestratorId}::uuid
			returning e.id
		)`);

		ctes.push(this.sql`inserted_children as (
			insert into pgconductor._private_executions (id, task_key, queue, payload, run_at, parent_execution_id, "group", trace_context, metadata)
			select pgconductor._private_portable_uuidv7(), r.child_task_name, r.child_task_queue, r.child_payload, nt.ts, r.execution_id, r."group", r.trace_context, r.metadata
			from invoke_child_data r, now_ts nt
			where exists (
				select 1 from pgconductor._private_executions parent
				where parent.id = r.execution_id and parent.queue = r.queue
					and parent.locked_by = ${orchestratorId}::uuid
			)
			returning id, parent_execution_id
		)`);
		ctes.push(this.sql`updated_invoke_parents as (
			update pgconductor._private_executions e
			set waiting_on_execution_id = ic.id, waiting_step_key = r.step_key,
				run_at = case when r.timeout_ms = -1 then 'infinity'::timestamptz
					else nt.ts + r.timeout_ms * interval '1 millisecond' end,
				locked_by = null, locked_at = null
			from now_ts nt, inserted_children ic
			join invoke_child_data r on r.execution_id = ic.parent_execution_id
			where e.id = r.execution_id and e.queue = r.queue
				and e.locked_by = ${orchestratorId}::uuid
			returning e.id
		)`);

		ctes.push(this.sql`writes as (
			select 1 from updated_signal_requeued
			union all select 1 from parent_steps_all
			union all select 1 from orphaned_children
			union all select 1 from updated_parents_all
			union all select 1 from deleted_completed
			union all select 1 from updated_completed
			union all select 1 from dead_lettered
			union all select 1 from deleted_failed
			union all select 1 from updated_failed
			union all select 1 from cleaned_terminal_event_waits
			union all select 1 from resolved_result_waits
			union all select 1 from retried
			union all select 1 from released_steps
			union all select 1 from updated_released
			union all select 1 from inserted_children
			union all select 1 from updated_invoke_parents
		)`);

		const combined = ctes.reduce((acc, cte, i) => (i === 0 ? cte : this.sql`${acc}, ${cte}`));
		// Linked children of a terminal failure are cancelled with its error. The cascade reads the
		// graph with fresh snapshots, so it sees children and waits that changed while this
		// statement waited for locks, and it writes rows this statement may write too. It runs
		// last: filtering on the row count of every write runs them to completion first (the
		// planner drops a count nothing reads).
		return this.sql`
			with recursive ${combined}
			select pgconductor._private_cancel_linked_children(jsonb_agg(jsonb_build_object(
				'execution_id', t.execution_id, 'reason', t.failure_error
			)))
			from (select count(*) as writes from writes) w
			left join terminal_failures t on true
			where w.writes >= 0
			having count(t.execution_id) > 0
		`;
	}
	buildRemoveExecutions({
		queueName,
		batchSize,
	}: RemoveExecutionsArgs): PendingQuery<{ deleted_count: number }[]> {
		return this.sql<{ deleted_count: number }[]>`
			with cleanup_clock as materialized (
				select pgconductor._private_current_time() as now_ts
			),
			completed_candidates as (
				select e.id, e.queue, e.completed_at as terminal_at
				from pgconductor._private_executions e
				join pgconductor._private_tasks t on t.key = e.task_key and t.queue = e.queue
				cross join cleanup_clock c
				where e.queue = ${queueName}
					and e.completed_at is not null
					and t.remove_on_complete_days > 0
					and e.completed_at < c.now_ts - t.remove_on_complete_days * interval '1 day'
				order by e.completed_at, e.id
				limit ${batchSize}
				for update of e skip locked
			),
			failed_candidates as (
				select e.id, e.queue, e.failed_at as terminal_at
				from pgconductor._private_executions e
				join pgconductor._private_tasks t on t.key = e.task_key and t.queue = e.queue
				cross join cleanup_clock c
				where e.queue = ${queueName}
					and e.failed_at is not null
					and t.remove_on_fail_days > 0
					and e.failed_at < c.now_ts - t.remove_on_fail_days * interval '1 day'
				order by e.failed_at, e.id
				limit ${batchSize}
				for update of e skip locked
			),
			batch as (
				select id, queue
				from (
					select id, queue, terminal_at from completed_candidates
					union all
					select id, queue, terminal_at from failed_candidates
				) candidates
				order by terminal_at, id, queue
				limit ${batchSize}
			),
			deleted as (
				delete from pgconductor._private_executions e
				using batch
				where e.id = batch.id and e.queue = batch.queue
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

		const dedupe = spec.throttle || spec.debounce;

		return this.sql<[{ id: string | null }]>`
			select id from pgconductor.invoke(
				p_task_key := ${spec.task_key}::text,
				p_queue := ${spec.queue}::text,
				p_payload := ${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
				p_run_at := ${spec.run_at ? spec.run_at.toISOString() : null}::timestamptz,
				p_dedupe_key := ${spec.dedupe_key || null}::text,
				p_dedupe_seconds := ${dedupe ? dedupe.seconds : null}::integer,
				p_dedupe_next_slot := ${Boolean(spec.debounce)}::boolean,
				p_cron_expression := ${spec.cron_expression || null}::text,
				p_priority := ${spec.priority || null}::integer,
				p_trace_context := ${spec.trace_context ? this.sql.json(spec.trace_context) : null}::jsonb,
				p_metadata := ${spec.metadata ? this.sql.json(spec.metadata) : null}::jsonb,
				p_group := ${spec.group || null}::text
			)
		`;
	}

	buildScheduleCronExecution({
		spec,
		scheduleName,
	}: ScheduleCronExecutionArgs): PendingQuery<[{ resolved: number }]> {
		assert.ok(spec.run_at, "scheduleCronExecution requires run_at");
		assert.ok(spec.cron_expression, "scheduleCronExecution requires cron_expression");

		const runAt = spec.run_at as Date;
		const cronExpression = spec.cron_expression as string;
		const timestampSeconds = Math.floor(runAt.getTime() / 1000);
		const dedupeKey = `dynamic::${scheduleName}::${timestampSeconds}`;

		return this.sql<[{ resolved: number }]>`
			with removed as (
				delete from pgconductor._private_executions
				where task_key = ${spec.task_key}::text
					and queue = ${spec.queue}::text
					and dedupe_key like 'dynamic::%'
					and split_part(dedupe_key, '::', 2) = ${scheduleName}::text
					and dedupe_key <> ${dedupeKey}::text
					and cron_expression is not null
					and is_available
				returning result_waiters
			), ${this.removedScheduleWaits()},
			-- a running or settled occurrence is kept
			scheduled as (
				insert into pgconductor._private_executions as e (
					task_key, queue, payload, metadata, run_at, dedupe_key, cron_expression, priority, "group"
				) values (
					${spec.task_key}::text,
					${spec.queue}::text,
					${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
					${spec.metadata ? this.sql.json(spec.metadata) : null}::jsonb,
					${runAt.toISOString()}::timestamptz,
					${dedupeKey}::text,
					${cronExpression}::text,
					${spec.priority || 0}::integer,
					${spec.group || null}::text
				)
				on conflict (task_key, dedupe_key, queue) where singleton_on is null do update set
					payload = excluded.payload,
					metadata = excluded.metadata,
					cron_expression = excluded.cron_expression,
					priority = excluded.priority,
					"group" = excluded."group"
				where e.is_available
			)
			select count(*)::int as resolved from resolved_result_waits
		`;
	}

	buildUnscheduleCronExecution({
		taskKey,
		queue,
		scheduleName,
	}: UnscheduleCronExecutionArgs): PendingQuery<Row[]> {
		return this.sql`
			select pgconductor._private_unschedule(
				p_task_key := ${taskKey}::text,
				p_queue := ${queue}::text,
				p_schedule_name := ${scheduleName}::text
			)
		`;
	}

	buildScheduleNextCronExecution({
		executionId,
		queue,
		orchestratorId,
		claimToken,
		runAt,
		dedupeKey,
	}: ScheduleNextCronExecutionArgs): PendingQuery<Row[]> {
		return this.sql`
			select pgconductor._private_schedule_next_occurrence(
				p_execution_id := ${executionId}::uuid,
				p_queue := ${queue}::text,
				p_orchestrator_id := ${orchestratorId}::uuid,
				p_claim_token := ${claimToken}::uuid,
				p_run_at := ${runAt.toISOString()}::timestamptz,
				p_dedupe_key := ${dedupeKey}::text
			)
		`;
	}

	// Waits for pending schedule executions removed by the statement's `removed` CTE
	private removedScheduleWaits(): PendingQuery<Row[]> {
		return this.sql`resolved_result_waits as (
			select pgconductor._private_resolve_result_waits(jsonb_agg(w.value || jsonb_build_object(
				'outcome', jsonb_build_object('status', 'cancelled', 'reason', 'Schedule removed')
			)))
			from removed r
			cross join lateral jsonb_array_elements(r.result_waiters) w
			having count(*) > 0
		)`;
	}

	buildInvokeBatch(specs: ExecutionSpec[]): PendingQuery<{ id: string }[]> {
		const specsArray = specs.map((spec) => {
			if (spec.throttle && spec.debounce) {
				throw new Error("Cannot use both throttle and debounce - choose one");
			}

			if (spec.debounce) {
				throw new Error("Batch invoke only supports throttle, not debounce");
			}

			return {
				task_key: spec.task_key,
				queue: spec.queue,
				payload: spec.payload || null,
				run_at: spec.run_at,
				dedupe_key: spec.dedupe_key || null,
				dedupe_seconds: spec.throttle ? spec.throttle.seconds : null,
				dedupe_next_slot: false,
				cron_expression: spec.cron_expression || null,
				trace_context: spec.trace_context,
				metadata: spec.metadata,
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

	buildGetExecution({ executionId, task }: GetExecutionArgs): PendingQuery<ExecutionInfo[]> {
		return this.sql<ExecutionInfo[]>`
			select id, task_key as "taskKey", queue,
				case
					when completed_at is not null then 'completed'
					when failed_at is not null and cancelled then 'cancelled'
					when failed_at is not null then 'failed'
					when locked_by is not null then 'running'
					else 'pending'
				end as status,
				result, last_error as error, attempts, created_at as "createdAt",
				completed_at as "completedAt", failed_at as "failedAt"
			from pgconductor._private_executions
			where id = ${executionId}::uuid
				${task ? this.sql`and task_key = ${task.key}::text and queue = ${task.queue}::text` : this.sql``}
		`;
	}

	buildLoadStep({
		executionId,
		queue,
		key,
	}: LoadStepArgs): PendingQuery<[{ result: Payload | null }]> {
		return this.sql<[{ result: Payload | null }]>`
			select result from pgconductor._private_steps
			where execution_id = ${executionId}::uuid and queue = ${queue}::text and key = ${key}::text
		`;
	}

	buildCountSteps({ executionIds, key }: CountStepsArgs): PendingQuery<[{ count: number }]> {
		return this.sql<[{ count: number }]>`
			select count(*)::int as count from pgconductor._private_steps
			where execution_id = any(${this.sql.array(executionIds, 2951)}::uuid[]) and key = ${key}::text
		`;
	}

	buildSaveStep({
		executionId,
		queue,
		key,
		result,
		orchestratorId,
		claimToken,
	}: SaveStepArgs): PendingQuery<[{ claimed: boolean }]> {
		return this.sql<[{ claimed: boolean }]>`
			with claimed_execution as materialized (
				select e.id, e.queue
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
					and e.claim_token = ${claimToken}::uuid
				for update
			),
			inserted as (
				insert into pgconductor._private_steps (execution_id, queue, key, result)
				select e.id, e.queue, ${key}::text, ${this.sql.json(result)}::jsonb
				from claimed_execution e
				on conflict (execution_id, key) do nothing
			)
			select exists (select 1 from claimed_execution) as claimed
		`;
	}

	buildStartExecution({
		executionId,
		queue,
		orchestratorId,
		claimToken,
		key,
		spec,
		cancelWithParent,
	}: StartExecutionArgs): PendingQuery<{ id: string }[]> {
		return this.sql<{ id: string }[]>`
			with claimed_execution as materialized (
				select e.id, e.queue
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
					and e.claim_token = ${claimToken}::uuid
				for update
			), stored_step as (
				select (s.result->>'id')::uuid as id
				from pgconductor._private_steps s, claimed_execution c
				where s.execution_id = c.id and s.key = ${key}::text
			), started_execution as (
				insert into pgconductor._private_executions as e (
					id, task_key, queue, payload, trace_context, metadata, run_at, dedupe_key, priority, "group",
					parent_execution_id, cancel_with_parent
				)
				select
					pgconductor._private_portable_uuidv7(),
					${spec.task_key}::text,
					${spec.queue}::text,
					${spec.payload ? this.sql.json(spec.payload) : null}::jsonb,
					${spec.trace_context ? this.sql.json(spec.trace_context) : null}::jsonb,
					${spec.metadata ? this.sql.json(spec.metadata) : null}::jsonb,
					coalesce(${spec.run_at ? spec.run_at.toISOString() : null}::timestamptz, pgconductor._private_current_time()),
					${spec.dedupe_key}::text,
					${spec.priority || 0}::integer,
					${spec.group || null}::text,
					case when ${cancelWithParent}::boolean then claimed_execution.id end,
					${cancelWithParent}::boolean
				from claimed_execution
				where not exists (select from stored_step)
				on conflict (task_key, dedupe_key, queue) where singleton_on is null
				do update set dedupe_key = excluded.dedupe_key
				returning e.id
			), saved_step as (
				insert into pgconductor._private_steps (execution_id, queue, key, result)
				select c.id, c.queue, ${key}::text, jsonb_build_object('id', s.id)
				from claimed_execution c, started_execution s
				on conflict (execution_id, key) do nothing
			)
			select id from stored_step
			union all
			select id from started_execution
		`;
	}

	buildRegisterEventWait({
		executionId,
		queue,
		taskKey,
		eventKey,
		stepKey,
		requiredFieldCount,
		terms,
		timeoutMs,
		orchestratorId,
		suspend,
		claimToken,
	}: RegisterEventWaitArgs): PendingQuery<{ result: EventWaitResult | null }[]> {
		return this.sql<{ result: EventWaitResult | null }[]>`
			select pgconductor._private_register_event_wait(
				${executionId}::uuid,
				${queue}::text,
				${taskKey}::text,
				${orchestratorId}::uuid,
				${claimToken}::uuid,
				${eventKey}::text,
				${stepKey}::text,
				${requiredFieldCount}::smallint,
				${this.sql.json(terms)}::jsonb,
				${timeoutMs}::bigint,
				${suspend}::boolean
			) as result
		`;
	}

	buildRegisterEventRace({
		executionId,
		queue,
		taskKey,
		stepKey,
		branches,
		timeoutMs,
		orchestratorId,
		claimToken,
		deliveringSignalId,
	}: RegisterEventRaceArgs): PendingQuery<{ result: EventRaceResult | null }[]> {
		return this.sql<{ result: EventRaceResult | null }[]>`
			select pgconductor._private_register_event_race(
				${executionId}::uuid,
				${queue}::text,
				${taskKey}::text,
				${orchestratorId}::uuid,
				${claimToken}::uuid,
				${stepKey}::text,
				${this.sql.json(branches)}::jsonb,
				${timeoutMs}::bigint,
				${deliveringSignalId}::text
			) as result
		`;
	}

	buildClearWaitingState({
		executionId,
		queue,
		orchestratorId,
		claimToken,
	}: ClearWaitingStateArgs): PendingQuery<RowList<Row[]>> {
		return this.sql<RowList<Row[]>>`
			-- The child is cancelled like any other: cancel_execution locks it before this parent
			with timed_out as materialized (
				select e.waiting_on_execution_id as child_id
				from pgconductor._private_executions e
				where e.id = ${executionId}::uuid
					and e.queue = ${queue}::text
					and e.locked_by = ${orchestratorId}::uuid
					and e.claim_token = ${claimToken}::uuid
					and e.waiting_on_execution_id is not null
			), cancelled_child as materialized (
				select pgconductor.cancel_execution(child_id, 'Cancelled: parent timed out')
				from timed_out
			)
			update pgconductor._private_executions e
			set waiting_on_execution_id = null, waiting_step_key = null
			from (select count(*) from cancelled_child) c
			where e.id = ${executionId}::uuid
				and e.queue = ${queue}::text
				and e.locked_by = ${orchestratorId}::uuid
				and e.claim_token = ${claimToken}::uuid
			returning e.id
		`;
	}

	buildCountActiveOrchestratorsBelow({
		version,
	}: CountActiveOrchestratorsBelowArgs): PendingQuery<{ count: number }[]> {
		return this.sql<{ count: number }[]>`
			select count(*) as count
			from pgconductor._private_orchestrators
			where migration_number < ${version}::integer
		`;
	}

	buildDispatchCustomEvents({
		eventIds,
		orchestratorId,
	}: DispatchCustomEventsArgs): PendingQuery<{ event_id: string }[]> {
		return this.sql<{ event_id: string }[]>`
			with sources as materialized (
				select
					source.id as event_id,
					source.payload ->> 'eventKey' as event_key,
					source.payload -> 'payload' as event_payload,
					source.created_at,
					source.trace_context,
					source.metadata
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
			), event_values as materialized (
				select source.event_id, source.event_key, field.key as field_name,
					field.value, jsonb_typeof(field.value) as scalar_type
				from sources source
				cross join lateral jsonb_each(source.event_payload) field
			), event_prefixes as materialized (
				select
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
				from sources source
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
				from sources source
				join pgconductor._private_custom_event_subscriptions subscription
					on subscription.event_key = source.event_key
					and subscription.required_field_count = 0
			), candidates as materialized (
				select matched.event_id, subscription.id as subscription_id,
					subscription.task_key, subscription.queue, subscription.payload_fields
				from matched_subscriptions matched
				join pgconductor._private_custom_event_subscriptions subscription
					on subscription.id = matched.subscription_id
					and subscription.kind = 'task_trigger'
				join pgconductor._private_tasks task
					on task.key = subscription.task_key and task.queue = subscription.queue
			), inserted_destinations as (
				insert into pgconductor._private_executions (
					id, task_key, queue, payload, parent_execution_id, subscription_id, trace_context,
					metadata
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
					candidate.subscription_id,
					source.trace_context,
					source.metadata
				from candidates candidate
				join sources source on source.event_id = candidate.event_id
				order by candidate.event_id, candidate.subscription_id
				on conflict (parent_execution_id, subscription_id, queue)
				where subscription_id is not null
				do nothing
			), selected_waits as materialized (
				select distinct on (subscription.id)
					matched.event_id,
					subscription.id as subscription_id,
					subscription.execution_id,
					subscription.queue,
					subscription.step_key,
					source.event_key,
					source.event_payload
				from matched_subscriptions matched
				join pgconductor._private_custom_event_subscriptions subscription
					on subscription.id = matched.subscription_id
					and subscription.kind = 'execution_wait'
				join sources source on source.event_id = matched.event_id
				where source.created_at > subscription.created_at
					and (
						subscription.expires_at is null
						or source.created_at <= subscription.expires_at
					)
				order by subscription.id, source.created_at, source.event_id
			), locked_wait_executions as materialized (
				-- A claimed or retrying subscriber keeps the result for its next wait.
				-- Only an execution suspended at an event wait is woken; it may be waiting
				-- on another of its subscriptions and suspend again. The flag is read
				-- from the locked row because the statement snapshot may be older.
				select execution.id, execution.queue,
					execution.locked_by is null
						and execution.waiting_on_execution_id is null
						and execution.waiting_step_key is not null as suspended
				from pgconductor._private_executions execution
				join selected_waits wait
					on wait.execution_id = execution.id and wait.queue = execution.queue
				where execution.completed_at is null
					and execution.failed_at is null
					and not execution.cancelled
				order by execution.id
				for update of execution
			), inserted_wait_steps as (
				insert into pgconductor._private_steps (execution_id, queue, key, result)
				select wait.execution_id, wait.queue, wait.step_key,
					jsonb_build_object(
						'status', 'resolved',
						'event', jsonb_build_object(
							'name', wait.event_key,
							'payload', wait.event_payload
						)
					)
				from selected_waits wait
				join locked_wait_executions execution
					on execution.id = wait.execution_id and execution.queue = wait.queue
				on conflict (execution_id, key) do nothing
				returning execution_id, queue, key
			), deleted_waits as (
				delete from pgconductor._private_custom_event_subscriptions subscription
				using inserted_wait_steps step
				where subscription.kind = 'execution_wait'
					and subscription.execution_id = step.execution_id
					and subscription.queue = step.queue
					and subscription.step_key = step.key
				returning subscription.execution_id, subscription.queue
			), woken_waits as (
				update pgconductor._private_executions execution
				set run_at = pgconductor._private_current_time(),
					waiting_on_execution_id = null,
					waiting_step_key = null
				from deleted_waits wait
				join locked_wait_executions locked
					on locked.id = wait.execution_id and locked.queue = wait.queue
				where execution.id = wait.execution_id and execution.queue = wait.queue
					and locked.suspended
				returning execution.id
			)
			select source.event_id
			from sources source
			order by source.event_id
		`;
	}

	buildEmitEvent({
		id,
		eventKey,
		payload,
		traceContext,
		metadata,
		dedupeKey,
	}: EmitEventArgs & { id: string }): PendingQuery<{ id: string }[]> {
		return this.sql<{ id: string }[]>`
			select pgconductor.emit_event(
				${eventKey}::text,
				${this.sql.json(payload || {})}::jsonb,
				${traceContext ? this.sql.json(traceContext) : null}::jsonb,
				${metadata ? this.sql.json(metadata) : null}::jsonb,
				${id}::uuid,
				${dedupeKey || null}::text
			) as id
		`;
	}

	buildCheckMetadata(metadata: Payload): PendingQuery<[]> {
		return this.sql<[]>`
			select pgconductor._private_check_metadata(${this.sql.json(metadata)}::jsonb)
		`;
	}
}

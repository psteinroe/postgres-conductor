-- Add trace_context column for OpenTelemetry context propagation
-- This stores W3C traceparent/tracestate for distributed tracing

-- Add column to executions table
alter table pgconductor._private_executions
add column if not exists trace_context jsonb;

-- Add trace_context to execution_spec type
alter type pgconductor.execution_spec
add attribute trace_context jsonb;

-- Update invoke_batch function to pass trace_context through
-- Preserves all existing logic, just adds trace_context column
create or replace function pgconductor.invoke_batch(
    specs pgconductor.execution_spec[]
)
 returns table(id uuid)
 language plpgsql
 volatile
 set search_path to ''
as $function$
declare
    v_now timestamptz;
begin
    v_now := pgconductor._private_current_time();

    -- clear locked dedupe keys before batch insert
    update pgconductor._private_executions as e
    set
        dedupe_key = null,
        locked_by = null,
        locked_at = null,
        failed_at = v_now,
        last_error = 'superseded by reinvoke'
    from unnest(specs) as spec
    where e.dedupe_key = spec.dedupe_key
        and e.task_key = spec.task_key
        and e.queue = coalesce(spec.queue, 'default')
        and e.locked_at is not null
        and spec.dedupe_key is not null;

    -- batch insert all executions
    -- note: duplicate dedupe_keys within same batch will cause error
    -- users should deduplicate client-side if needed
    return query
    insert into pgconductor._private_executions (
        id,
        task_key,
        queue,
        payload,
        run_at,
        dedupe_key,
        singleton_on,
        cron_expression,
        priority,
        trace_context
    )
    select
        pgconductor._private_portable_uuidv7(),
        spec.task_key,
        coalesce(spec.queue, 'default'),
        spec.payload,
        coalesce(spec.run_at, v_now),
        spec.dedupe_key,
        case
            when spec.dedupe_seconds is not null then
                'epoch'::timestamptz + '1 second'::interval * (
                    spec.dedupe_seconds * floor(
                        extract(epoch from v_now) / spec.dedupe_seconds
                    )
                )
            else null
        end,
        spec.cron_expression,
        coalesce(spec.priority, 0),
        spec.trace_context
    from unnest(specs) as spec
    on conflict (task_key, dedupe_key, queue) do update set
        payload = excluded.payload,
        run_at = excluded.run_at,
        priority = excluded.priority,
        cron_expression = excluded.cron_expression,
        singleton_on = excluded.singleton_on,
        trace_context = excluded.trace_context
    returning pgconductor._private_executions.id;
end;
$function$
;

-- Update invoke function to pass trace_context through
-- Preserves all existing logic, just adds trace_context parameter and column
create or replace function pgconductor.invoke(
    p_task_key text,
    p_queue text default 'default',
    p_payload jsonb default null,
    p_run_at timestamptz default null,
    p_dedupe_key text default null,
    p_dedupe_seconds integer default null,
    p_dedupe_next_slot boolean default false,
    p_cron_expression text default null,
    p_priority integer default null,
    p_trace_context jsonb default null
)
 returns table(id uuid)
 language plpgsql
 volatile
 set search_path to ''
as $function$
declare
    v_now timestamptz;
    v_singleton_on timestamptz;
    v_next_singleton_on timestamptz;
    v_run_at timestamptz;
    v_new_id uuid;
begin
  v_now := pgconductor._private_current_time();
  v_run_at := coalesce(p_run_at, v_now);

  -- clear locked dedupe key before insert (supersede pattern)
  if p_dedupe_key is not null then
      update pgconductor._private_executions
      set
          dedupe_key = null,
          locked_by = null,
          locked_at = null,
          failed_at = v_now,
          last_error = 'superseded by reinvoke'
      where dedupe_key = p_dedupe_key
          and task_key = p_task_key
          and queue = p_queue
          and locked_at is not null;
  end if;

  -- singleton throttle/debounce logic
  if p_dedupe_seconds is not null then
      -- calculate current time slot (pg-boss formula)
      v_singleton_on := 'epoch'::timestamptz + '1 second'::interval * (
          p_dedupe_seconds * floor(
              extract(epoch from v_now) / p_dedupe_seconds
          )
      );

      if p_dedupe_next_slot = false then
          -- throttle: try current slot, return empty if blocked
          return query
          insert into pgconductor._private_executions (
              id,
              task_key,
              queue,
              payload,
              run_at,
              dedupe_key,
              singleton_on,
              cron_expression,
              priority,
              trace_context
          ) values (
              pgconductor._private_portable_uuidv7(),
              p_task_key,
              p_queue,
              p_payload,
              v_run_at,
              p_dedupe_key,
              v_singleton_on,
              p_cron_expression,
              coalesce(p_priority, 0),
              p_trace_context
          )
          on conflict (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
          where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false
          do nothing
          returning _private_executions.id;
          return;
      else
          -- debounce: upsert into next slot
          v_next_singleton_on := v_singleton_on + (p_dedupe_seconds || ' seconds')::interval;

          return query
          insert into pgconductor._private_executions (
              id,
              task_key,
              queue,
              payload,
              run_at,
              dedupe_key,
              singleton_on,
              cron_expression,
              priority,
              trace_context
          ) values (
              pgconductor._private_portable_uuidv7(),
              p_task_key,
              p_queue,
              p_payload,
              v_next_singleton_on,
              p_dedupe_key,
              v_next_singleton_on,
              p_cron_expression,
              coalesce(p_priority, 0),
              p_trace_context
          )
          on conflict (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
          where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false
          do update set
              payload = excluded.payload,
              run_at = excluded.run_at,
              priority = excluded.priority,
              trace_context = excluded.trace_context
          returning _private_executions.id;
          return;
      end if;
  end if;

  -- regular invoke (no singleton)
  if p_dedupe_key is not null then
      -- clear keys that are currently locked so a subsequent insert can succeed.
      update pgconductor._private_executions as e
      set
        dedupe_key = null,
        locked_by = null,
        locked_at = null,
        failed_at = pgconductor._private_current_time(),
        last_error = 'superseded by reinvoke'
      where e.dedupe_key = p_dedupe_key
        and e.task_key = p_task_key
        and e.queue = p_queue
        and e.locked_at is not null;
  end if;

  return query insert into pgconductor._private_executions as e (
    id,
    task_key,
    queue,
    payload,
    run_at,
    dedupe_key,
    cron_expression,
    priority,
    trace_context
  ) values (
    pgconductor._private_portable_uuidv7(),
    p_task_key,
    p_queue,
    p_payload,
    v_run_at,
    p_dedupe_key,
    p_cron_expression,
    coalesce(p_priority, 0),
    p_trace_context
  )
  on conflict (task_key, dedupe_key, queue) do update set
    payload = excluded.payload,
    run_at = excluded.run_at,
    priority = excluded.priority,
    cron_expression = excluded.cron_expression,
    trace_context = excluded.trace_context
  returning e.id;
end;
$function$
;

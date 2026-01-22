-- Add trace_context column for OpenTelemetry context propagation
-- This stores W3C traceparent/tracestate for distributed tracing

-- Add column to executions table
alter table pgconductor._private_executions
add column if not exists trace_context jsonb;

-- Add trace_context to execution_spec type
-- Note: ALTER TYPE ... ADD ATTRIBUTE requires Postgres 9.1+
alter type pgconductor.execution_spec
add attribute trace_context jsonb;

-- Update invoke_batch function to handle trace_context
create or replace function pgconductor.invoke_batch(specs pgconductor.execution_spec[])
 returns table(id uuid)
 language plpgsql
as $function$
begin
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
        coalesce(
            spec.run_at,
            pgconductor._private_current_time() + make_interval(secs => coalesce(spec.delay_seconds, 0))
        ),
        spec.dedupe_key,
        case
            when spec.dedupe_seconds is not null then
                pgconductor._private_dedupe_on(spec.dedupe_seconds, spec.dedupe_next_slot)
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

-- Update invoke function to handle trace_context
create or replace function pgconductor.invoke(
    p_task_key text,
    p_queue text default 'default',
    p_payload jsonb default '{}',
    p_run_at timestamptz default null,
    p_delay_seconds integer default null,
    p_dedupe_key text default null,
    p_dedupe_seconds integer default null,
    p_dedupe_next_slot boolean default false,
    p_cron_expression text default null,
    p_priority integer default null,
    p_trace_context jsonb default null
)
 returns table(id uuid)
 language plpgsql
as $function$
declare
  v_run_at timestamptz;
  v_singleton_on timestamptz;
  v_next_singleton_on timestamptz;
begin
  -- Validate: can't use both dedupe_seconds and dedupe_key
  if p_dedupe_seconds is not null and p_dedupe_key is not null then
    raise exception 'Cannot use both dedupe_seconds and dedupe_key - choose one';
  end if;

  -- Calculate run_at
  v_run_at := coalesce(
    p_run_at,
    pgconductor._private_current_time() + make_interval(secs => coalesce(p_delay_seconds, 0))
  );

  -- Handle cron-based singleton deduplication
  if p_cron_expression is not null then
      -- Calculate singleton_on from cron expression
      v_singleton_on := pgconductor._private_cron_singleton_on(p_cron_expression, v_run_at);

      -- Try to insert with current singleton_on
      return query
          insert into pgconductor._private_executions as e (
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
          returning e.id;

      -- If insert succeeded (row returned), we're done
      if found then
          return;
      end if;

      -- Conflict occurred - calculate next singleton slot
      v_next_singleton_on := pgconductor._private_cron_next_singleton_on(p_cron_expression, v_singleton_on);

      -- Try to insert with next singleton_on slot
      return query
          insert into pgconductor._private_executions as _private_executions (
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

  -- Standard insert (no cron)
  return query
  insert into pgconductor._private_executions as e (
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

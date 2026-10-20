
create extension if not exists btree_gist;

-- Returns the current timestamp, shifted by the pgconductor.fake_offset session setting
-- in tests. Fake time keeps running, so rows written one after another get later times.
create function pgconductor._private_current_time ()
  returns timestamptz
  language plpgsql
  volatile
as $$
declare
  v_offset text;
begin
  v_offset := current_setting('pgconductor.fake_offset', true);
  if v_offset is not null and length(trim(v_offset)) > 0 then
    return clock_timestamp() + v_offset::interval;
  end if;

  return clock_timestamp();
end;
$$;

-- utility function to generate a uuidv7 even for older postgres versions.
create function pgconductor._private_portable_uuidv7 ()
  returns uuid
  language plpgsql
  volatile
as $$
declare
  v_server_num integer := current_setting('server_version_num')::int;
  ts_ms bigint;
  b bytea;
  rnd bytea;
  i int;
begin
  if v_server_num >= 180000 then
    return uuidv7 ();
  end if;
  ts_ms := floor(extract(epoch from pgconductor._private_current_time()) * 1000)::bigint;
  rnd := uuid_send(gen_random_uuid());
  b := repeat(E'\\000', 16)::bytea;
  for i in 0..5 loop
    b := set_byte(b, i, ((ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;
  for i in 6..15 loop
    b := set_byte(b, i, get_byte(rnd, i));
  end loop;
  b := set_byte(b, 6, ((get_byte(b, 6) & 15) | (7 << 4)));
  b := set_byte(b, 8, ((get_byte(b, 8) & 63) | 128));
  return encode(b, 'hex')::uuid;
end;
$$;

create table pgconductor._private_orchestrators (
    id uuid default pgconductor._private_portable_uuidv7() primary key,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version text,
    migration_number integer,
    -- set when the orchestrator must stop; every heartbeat reports it until the row is deleted
    shutdown_reason text
);

create index idx_orchestrators_heartbeat on pgconductor._private_orchestrators (last_heartbeat_at);
create index idx_orchestrators_sweep on pgconductor._private_orchestrators (migration_number);

create table pgconductor._private_queues (
    name text primary key
);

create table pgconductor._private_executions (
    id uuid default pgconductor._private_portable_uuidv7(),
    task_key text not null,
    queue text not null default 'default',
    dedupe_key text,
    -- addresses the one non-terminal execution of a task for signal_with_start()
    key text,
    cron_expression text,
    created_at timestamptz default pgconductor._private_current_time() not null,
    failed_at timestamptz,
    completed_at timestamptz,
    payload jsonb,
    result jsonb,
    trace_context jsonb,
    metadata jsonb,
    run_at timestamptz default pgconductor._private_current_time() not null,
    locked_at timestamptz,
    locked_by uuid,
    -- new on every claim, so a late or retried write from an earlier claim cannot touch a newer one
    claim_token uuid,
    "group" text,
    is_available boolean generated always as (locked_at is null and failed_at is null and completed_at is null) stored not null,
    attempts integer default 0 not null,
    last_error text,
    cancelled boolean default false not null,
    priority integer default 0 not null,
    waiting_on_execution_id uuid,
    waiting_step_key text,
    parent_execution_id uuid,
    -- started by ctx.invoke with cancelWithParent: cancelled when its parent is cancelled or fails permanently
    cancel_with_parent boolean default false not null,
    subscription_id uuid,
    singleton_on timestamptz,
    -- signals ever stored for this execution; settlement reads it from the locked row to see
    -- signals committed after its snapshot was taken
    signal_count integer default 0 not null,
    -- waits for this execution's outcome, as {execution_id, queue, step_key}. registration appends
    -- under the row lock, so settlement sees waits registered after its snapshot was taken
    result_waiters jsonb,

    -- source execution metadata of a dead-letter delivery, copied so the source row may be removed
    dead_letter jsonb,
    primary key (id, queue),
    constraint chk_executions_event_delivery_parent
        check (subscription_id is null or parent_execution_id is not null)
) partition by list (queue);

-- ordinary dedupe: one stored execution per key. throttled and debounced executions are keyed by
-- their slot below instead, so a stored one does not block the key in a later slot
create unique index on pgconductor._private_executions (task_key, dedupe_key, queue)
where singleton_on is null;

-- unique index for singleton throttle/debounce enforcement on parent table
-- this automatically creates matching indexes on all partitions
-- must include partition key (queue) per PostgreSQL requirement
-- use coalesce to treat NULL dedupe_key as empty string for singleton matching
create unique index on pgconductor._private_executions (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false;

-- at most one non-terminal execution per (task, key). a cancelled execution no longer holds its key,
-- so a signal sent after cancellation starts a new execution instead of being dropped
create unique index on pgconductor._private_executions (task_key, key, queue)
where key is not null and completed_at is null and failed_at is null and cancelled = false;

create table pgconductor._private_tasks (
    key text not null,

    -- queue that this task belongs to (used for queue-based worker assignment)
    queue text default 'default' not null,

    -- retry settings - uses fixed Inngest-style backoff schedule
    max_attempts integer default 3 not null,

    -- retention settings: NULL=keep forever, 0=delete immediately, N=delete after N days
    remove_on_complete_days integer,
    remove_on_fail_days integer,

    -- task can be executed only within certain time windows
    -- e.g. business hours, weekends, nights, ...
    -- we will stop the execution of executions outside of these time windows at step boundaries
    window_start timetz,
    window_end timetz,
    constraint "windows" check (
        (window_start is null and window_end is null) or
        (
            window_start is not null and
            window_end is not null and
            window_start != window_end
        )
    ),

    -- concurrency controls are intentionally soft and coordinated at claim time
    -- NULL means no limit (unlimited concurrency)
    concurrency_limit integer,
    group_concurrency_limit integer,

    -- Destination copied onto each source task registration.
    dead_letter_queue text,
    dead_letter_task_key text,

    constraint positive_concurrency_limits check (
        (concurrency_limit is null or concurrency_limit > 0) and
        (group_concurrency_limit is null or group_concurrency_limit > 0)
    ),
    constraint dead_letter_not_self check (
        dead_letter_queue is null or dead_letter_queue <> queue or
        (dead_letter_task_key is not null and dead_letter_task_key <> key)
    ),

    primary key (queue, key)
);

create table pgconductor._private_steps (
    execution_id uuid not null,
    key text not null,
    queue text not null,
    result jsonb,
    created_at timestamptz default pgconductor._private_current_time() not null,
    primary key (execution_id, key),
    constraint fk_execution foreign key (execution_id, queue) references pgconductor._private_executions(id, queue) on delete cascade
);

-- application signals of an execution. seq orders delivery; delivered_at is set once a handler
-- returned for the signal, and a redelivered id is stored once
create table pgconductor._private_execution_signals (
    execution_id uuid not null,
    queue text not null,
    id text not null,
    name text not null,
    payload jsonb,
    seq bigint generated always as identity,
    created_at timestamptz default pgconductor._private_current_time() not null,
    delivered_at timestamptz,
    primary key (execution_id, id),
    constraint chk_execution_signal_names check (
        btrim(id) <> '' and octet_length(id) <= 255
        and btrim(name) <> '' and octet_length(name) <= 255
    ),
    constraint fk_execution foreign key (execution_id, queue) references pgconductor._private_executions(id, queue) on delete cascade
);

create index idx_execution_signals_undelivered
    on pgconductor._private_execution_signals (execution_id, seq)
    where delivered_at is null;

create unique index idx_executions_dead_letter_delivery
    on pgconductor._private_executions ((dead_letter->>'sourceExecutionId'), queue, task_key)
    where dead_letter is not null;

-- Trigger function to manage executions partitions per queue
-- Automatically creates partition when queue is inserted
create or replace function pgconductor._private_manage_queue_partition()
 returns trigger
 language plpgsql
 volatile
 set search_path to ''
as $function$
declare
  v_partition_name text;
begin
  -- readable prefix plus a hash of the exact queue name, e.g. executions_default_c21f969b.
  -- sized so that the longest index name below stays within 63 bytes.
  v_partition_name := 'executions_'
    || left(regexp_replace(lower(coalesce(new.name, old.name)), '[^a-z0-9]+', '_', 'g'), 15)
    || '_' || left(md5(coalesce(new.name, old.name)), 8);

  if tg_op = 'INSERT' then
    execute format(
      'create table pgconductor.%I partition of pgconductor._private_executions for values in (%L) with (fillfactor=70)',
      v_partition_name,
      new.name
    );

    -- create indices

    -- main index for fetching available executions
    execute format(
      'create index %I on pgconductor.%I (priority, run_at, created_at, id) include (task_key) where is_available = true',
      'idx_' || v_partition_name || '_get_executions',
      v_partition_name
    );

    -- index for claiming available executions of one task under concurrency limits
    execute format(
      'create index %I on pgconductor.%I (task_key, priority, run_at, created_at, id) where is_available = true',
      'idx_' || v_partition_name || '_get_task_executions',
      v_partition_name
    );

    -- index for waiting executions lookup
    execute format(
      'create index %I on pgconductor.%I (waiting_on_execution_id) where waiting_on_execution_id is not null',
      'idx_' || v_partition_name || '_waiting_on_execution_id',
      v_partition_name
    );

    -- index for parent execution lookup (child -> parent)
    execute format(
      'create index %I on pgconductor.%I (parent_execution_id) where parent_execution_id is not null',
      'idx_' || v_partition_name || '_parent_execution_id',
      v_partition_name
    );

    -- index for unlocking locked executions
    execute format(
      'create index %I on pgconductor.%I (locked_by) where locked_by is not null',
      'idx_' || v_partition_name || '_locked_by',
      v_partition_name
    );

    -- index for cleanup of completed
    execute format(
      'create index %I on pgconductor.%I (completed_at) where completed_at is not null',
      'idx_' || v_partition_name || '_completed_cleanup',
      v_partition_name
    );

    -- index for cleanup of failed
    execute format(
      'create index %I on pgconductor.%I (failed_at) where failed_at is not null',
      'idx_' || v_partition_name || '_failed_cleanup',
      v_partition_name
    );

    -- index for schedule lookups
    execute format(
      'create index %I on pgconductor.%I ((split_part(dedupe_key, ''::'', 2))) where cron_expression is not null',
      'idx_' || v_partition_name || '_schedule',
      v_partition_name
    );

    -- index for task_key joins (used in return_executions)
    execute format(
      'create index %I on pgconductor.%I (task_key)',
      'idx_' || v_partition_name || '_task_key',
      v_partition_name
    );

    -- covering index used to count active executions for soft task and group limits
    execute format(
      'create index %I on pgconductor.%I (task_key, "group") where locked_at is not null and failed_at is null and completed_at is null',
      'idx_' || v_partition_name || '_active_concurrency',
      v_partition_name
    );

    RETURN NEW;

  elsif tg_op = 'UPDATE' then
    -- protect queues required by the runtime
    if old.name = 'pgconductor.internal' or new.name = 'pgconductor.internal' then
      raise exception 'Modifying the internal queue is not allowed';
    end if;

    if old.name = 'default' or new.name = 'default' then
      raise exception 'Modifying the default queue is not allowed';
    end if;

    -- disallow renaming queues
    if new.name != old.name then
      raise exception 'Renaming queues is not allowed. Queue name cannot be changed from % to %', old.name, new.name;
    end if;

    return new;

  elsif tg_op = 'DELETE' then
    -- protect queues required by the runtime
    if old.name = 'pgconductor.internal' then
      raise exception 'Deleting the internal queue is not allowed';
    end if;

    if old.name = 'default' then
      raise exception 'Deleting the default queue is not allowed';
    end if;

    -- foreign keys into the partition block dropping it, so clear its rows (cascading to
    -- steps and subscriptions) and detach it first
    delete from pgconductor._private_executions where queue = old.name;

    execute format(
      'alter table pgconductor._private_executions detach partition pgconductor.%I',
      v_partition_name
    );

    execute format(
      'drop table pgconductor.%I',
      v_partition_name
    );

    return old;
  end if;
end;
$function$;

-- attach trigger to queues table
create trigger manage_queue_partition_trigger
  after insert or update or delete on pgconductor._private_queues
  for each row
  execute function pgconductor._private_manage_queue_partition();

-- create default queue (trigger will create its partition)
insert into pgconductor._private_queues (name) values ('default');

-- deletes an orchestrator by id, or those whose last heartbeat is older than p_max_age, and
-- releases their executions. a claim locks its owner row, so the delete waits for claims in
-- flight, and the release runs as a second statement whose snapshot includes them.
create or replace function pgconductor._private_remove_orchestrators(
    p_id uuid default null,
    p_max_age interval default null
)
 returns void
 language plpgsql
 volatile
 set search_path to ''
as $function$
declare
  v_ids uuid[];
begin
  with deleted as (
    delete from pgconductor._private_orchestrators o
    where o.id = p_id
      or o.last_heartbeat_at < pgconductor._private_current_time() - p_max_age
    returning o.id
  )
  select array_agg(id) into v_ids from deleted;

  -- cancelled executions are reclaimed and settled like any other result
  update pgconductor._private_executions e
  set
    locked_by = null,
    locked_at = null
  where e.locked_by = any(v_ids);
end;
$function$;

-- metadata is copied to derived executions, so it is only checked where it enters
create or replace function pgconductor._private_check_metadata(p_metadata jsonb)
 returns void
 language plpgsql
 immutable
 set search_path to ''
as $function$
begin
  if octet_length(p_metadata::text) > 8192 then
    raise exception 'Execution metadata must not exceed 8192 bytes, got % bytes', octet_length(p_metadata::text);
  end if;
end;
$function$;

create type pgconductor.execution_spec as (
    task_key text,
    queue text,
    payload jsonb,
    run_at timestamptz,
    dedupe_key text,
    dedupe_seconds integer,
    dedupe_next_slot boolean,
    cron_expression text,
    priority integer,
    "group" text,
    trace_context jsonb,
    metadata jsonb
);

create type pgconductor.task_spec as (
    key text,
    queue text,
    max_attempts integer,
    remove_on_complete_days integer,
    remove_on_fail_days integer,
    window_start timetz,
    window_end timetz,
    concurrency_limit integer,
    group_concurrency_limit integer,
    dead_letter_queue text,
    dead_letter_task_key text
);

create type pgconductor.event_subscription_spec as (
    task_key text,
    event_key text,
    payload_fields text[],
    required_field_count smallint,
    terms jsonb
);

create or replace function pgconductor._private_register_worker(
    p_queue_name text,
    p_task_specs pgconductor.task_spec[],
    p_cron_schedules pgconductor.execution_spec[],
    p_event_subscriptions pgconductor.event_subscription_spec[] default array[]::pgconductor.event_subscription_spec[]
)
returns void
language plpgsql
volatile
set search_path to ''
as $function$
declare
  v_resolved integer;
begin
  -- Upsert every required queue in a stable order (triggers partition
  -- creation).  Registrations may reference one another as dead-letter queues,
  -- so queue creation and locking share a global order.
  insert into pgconductor._private_queues (name)
  select required.name
  from (
    select p_queue_name as name
    union
    select spec.dead_letter_queue
    from unnest(p_task_specs) as spec
    where spec.dead_letter_queue is not null
  ) required
  order by required.name
  on conflict (name) do nothing;

  -- Queue row locks are the only registration locks; acquire them in order.
  perform 1
  from pgconductor._private_queues queue
  where queue.name in (
    select p_queue_name
    union
    select spec.dead_letter_queue
    from unnest(p_task_specs) as spec
    where spec.dead_letter_queue is not null
  )
  order by queue.name
  for update;

  -- Register/update tasks.
  insert into pgconductor._private_tasks (key, queue, max_attempts, remove_on_complete_days, remove_on_fail_days, window_start, window_end, concurrency_limit, group_concurrency_limit, dead_letter_queue, dead_letter_task_key)
  select
    spec.key,
    coalesce(spec.queue, 'default'),
    coalesce(spec.max_attempts, 3),
    spec.remove_on_complete_days,
    spec.remove_on_fail_days,
    spec.window_start,
    spec.window_end,
    spec.concurrency_limit,
    spec.group_concurrency_limit,
    spec.dead_letter_queue,
    spec.dead_letter_task_key
  from unnest(p_task_specs) as spec
  on conflict (queue, key)
  do update set
    max_attempts = coalesce(excluded.max_attempts, pgconductor._private_tasks.max_attempts),
    remove_on_complete_days = excluded.remove_on_complete_days,
    remove_on_fail_days = excluded.remove_on_fail_days,
    window_start = excluded.window_start,
    window_end = excluded.window_end,
    concurrency_limit = excluded.concurrency_limit,
    group_concurrency_limit = excluded.group_concurrency_limit,
    dead_letter_queue = excluded.dead_letter_queue,
    dead_letter_task_key = excluded.dead_letter_task_key;

  -- Insert scheduled cron executions.
  insert into pgconductor._private_executions (task_key, queue, payload, run_at, dedupe_key, cron_expression, priority, "group")
  select
    spec.task_key,
    coalesce(spec.queue, 'default'),
    coalesce(spec.payload, '{}'::jsonb),
    coalesce(spec.run_at, pgconductor._private_current_time()),
    spec.dedupe_key,
    spec.cron_expression,
    coalesce(spec.priority, 0),
    spec."group"
  from unnest(p_cron_schedules) as spec
  where spec.dedupe_key is not null
  on conflict (task_key, dedupe_key, queue) where singleton_on is null do update set
    payload = excluded.payload,
    run_at = excluded.run_at,
    cron_expression = excluded.cron_expression,
    priority = excluded.priority,
    "group" = excluded."group";

  -- Clean up stale schedules for this queue.
  with removed as (
    delete from pgconductor._private_executions
    where queue = p_queue_name
      and cron_expression is not null
      and is_available
      and dedupe_key like 'scheduled::%'
      and (task_key, split_part(dedupe_key, '::', 2), cron_expression) not in (
        select spec.task_key, split_part(spec.dedupe_key, '::', 2), spec.cron_expression
        from unnest(p_cron_schedules) as spec
        where spec.dedupe_key is not null and spec.dedupe_key like 'scheduled::%'
      )
    returning result_waiters
  )
  select pgconductor._private_resolve_result_waits(jsonb_agg(w.value || jsonb_build_object(
    'outcome', jsonb_build_object('status', 'cancelled', 'reason', 'Schedule removed')
  )))
  into v_resolved
  from removed r
  cross join lateral jsonb_array_elements(r.result_waiters) w
  having count(*) > 0;

  update pgconductor._private_executions
  set cancelled = true
  where queue = p_queue_name
    and cron_expression is not null
    and dedupe_key like 'scheduled::%'
    and locked_by is not null
    and completed_at is null
    and failed_at is null
    and cancelled = false
    and (task_key, split_part(dedupe_key, '::', 2), cron_expression) not in (
      select spec.task_key, split_part(spec.dedupe_key, '::', 2), spec.cron_expression
      from unnest(p_cron_schedules) as spec
      where spec.dedupe_key is not null and spec.dedupe_key like 'scheduled::%'
    );

  perform pgconductor._private_replace_custom_event_subscriptions(
    p_queue_name,
    p_event_subscriptions
  );
end;
$function$;

-- Removes the dynamic cron schedule p_schedule_name: deletes its pending
-- occurrences and cancels running ones. Holding the queue row lock, like
-- registration, keeps running occurrences from inserting a successor until
-- this commits; each statement then sees the successors inserted before.
create or replace function pgconductor._private_unschedule(
    p_task_key text,
    p_queue text,
    p_schedule_name text
)
returns void
language plpgsql
volatile
set search_path to ''
as $function$
declare
  v_resolved integer;
begin
  perform 1 from pgconductor._private_queues queue where queue.name = p_queue for update;

  with removed as (
    delete from pgconductor._private_executions
    where task_key = p_task_key
      and queue = p_queue
      and dedupe_key like 'dynamic::%'
      and split_part(dedupe_key, '::', 2) = p_schedule_name
      and cron_expression is not null
      and is_available
    returning result_waiters
  )
  select pgconductor._private_resolve_result_waits(jsonb_agg(w.value || jsonb_build_object(
    'outcome', jsonb_build_object('status', 'cancelled', 'reason', 'Schedule removed')
  )))
  into v_resolved
  from removed r
  cross join lateral jsonb_array_elements(r.result_waiters) w
  having count(*) > 0;

  -- A separate statement, so it also sees occurrences claimed while the delete ran
  update pgconductor._private_executions
  set cancelled = true
  where task_key = p_task_key
    and queue = p_queue
    and dedupe_key like 'dynamic::%'
    and split_part(dedupe_key, '::', 2) = p_schedule_name
    and cron_expression is not null
    and locked_by is not null
    and completed_at is null
    and failed_at is null
    and cancelled = false;
end;
$function$;

-- Inserts the next occurrence of the cron schedule of the claimed occurrence
-- p_execution_id unless it was cancelled. Removing a schedule holds the queue
-- row lock and cancels running occurrences, so a removal that commits first
-- leaves no successor and one that commits later sees and removes it. The
-- successor is stored as a step, so retries and later attempts of the
-- occurrence insert it once, and an existing one is never superseded.
create or replace function pgconductor._private_schedule_next_occurrence(
    p_execution_id uuid,
    p_queue text,
    p_orchestrator_id uuid,
    p_claim_token uuid,
    p_run_at timestamptz,
    p_dedupe_key text
)
returns void
language plpgsql
volatile
set search_path to ''
as $function$
declare
  v_next_id uuid;
begin
  perform 1 from pgconductor._private_queues queue where queue.name = p_queue for share;

  perform 1
  from pgconductor._private_executions execution
  where execution.id = p_execution_id
    and execution.queue = p_queue
    and execution.locked_by = p_orchestrator_id
    and execution.claim_token = p_claim_token
    and not execution.cancelled
    and not exists (
      select 1 from pgconductor._private_steps step
      where step.execution_id = p_execution_id and step.key = 'pgconductor.next-occurrence'
    )
  for update of execution;

  if not found then
    return;
  end if;

  insert into pgconductor._private_executions as e (
    task_key, queue, run_at, dedupe_key, cron_expression, "group", metadata
  )
  select execution.task_key, execution.queue, p_run_at, p_dedupe_key, execution.cron_expression,
    execution."group", execution.metadata
  from pgconductor._private_executions execution
  where execution.id = p_execution_id and execution.queue = p_queue
  on conflict (task_key, dedupe_key, queue) where singleton_on is null
  do update set dedupe_key = excluded.dedupe_key
  returning e.id into v_next_id;

  insert into pgconductor._private_steps (execution_id, queue, key, result)
  values (p_execution_id, p_queue, 'pgconductor.next-occurrence', jsonb_build_object('id', v_next_id));
end;
$function$;

create or replace function pgconductor.invoke_batch(
    specs pgconductor.execution_spec[]
)
 returns table(id uuid)
 language plpgsql
 volatile
 set search_path to ''
as $function$
declare
    spec pgconductor.execution_spec;
begin
    foreach spec in array specs loop
        return query select * from pgconductor.invoke(
            p_task_key := spec.task_key,
            p_queue := coalesce(spec.queue, 'default'),
            p_payload := spec.payload,
            p_run_at := spec.run_at,
            p_dedupe_key := spec.dedupe_key,
            p_dedupe_seconds := spec.dedupe_seconds,
            p_dedupe_next_slot := coalesce(spec.dedupe_next_slot, false),
            p_cron_expression := spec.cron_expression,
            p_priority := spec.priority,
            p_group := spec."group",
            p_trace_context := spec.trace_context,
            p_metadata := spec.metadata
        );
    end loop;
end;
$function$
;

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
    p_group text default null,
    p_trace_context jsonb default null,
    p_metadata jsonb default null
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
    v_superseded_id uuid;
begin
  perform pgconductor._private_check_metadata(p_metadata);
  v_now := pgconductor._private_current_time();
  v_run_at := coalesce(p_run_at, v_now);

  -- clear locked dedupe key before insert (supersede pattern). throttle and debounce never
  -- supersede: a running execution holds its slot
  if p_dedupe_key is not null and p_dedupe_seconds is null then
      with superseded as (
          select e.id, e.queue, e.result_waiters, t.remove_on_fail_days = 0 as should_remove
          from pgconductor._private_executions e
          left join pgconductor._private_tasks t on t.key = e.task_key and t.queue = e.queue
          where e.dedupe_key = p_dedupe_key
              and e.task_key = p_task_key
              and e.queue = p_queue
              and e.singleton_on is null
              and e.locked_at is not null
          for update of e
      ),
      removed as (
          delete from pgconductor._private_executions e
          using superseded s
          where e.id = s.id and e.queue = s.queue and s.should_remove
      ),
      failed as (
          update pgconductor._private_executions e
          set
              dedupe_key = null,
              locked_by = null,
              locked_at = null,
              failed_at = v_now,
              last_error = 'superseded by reinvoke'
            from superseded s
          where e.id = s.id and e.queue = s.queue and s.should_remove is not true
      ),
      resolved as (
          select pgconductor._private_resolve_result_waits(jsonb_agg(w.value || jsonb_build_object(
              'outcome', jsonb_build_object('status', 'failed', 'error', 'superseded by reinvoke')
          )))
          from superseded s
          cross join lateral jsonb_array_elements(s.result_waiters) w
          having count(*) > 0
      )
      select s.id
      into v_superseded_id
      from superseded s
      left join resolved on true;

      if v_superseded_id is not null then
          perform pgconductor._private_cancel_linked_children(jsonb_build_array(jsonb_build_object(
              'execution_id', v_superseded_id, 'reason', 'superseded by reinvoke'
          )));
      end if;
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
              trace_context,
              metadata,
              run_at,
              dedupe_key,
              singleton_on,
              cron_expression,
              priority,
              "group"
          ) values (
              pgconductor._private_portable_uuidv7(),
              p_task_key,
              p_queue,
              p_payload,
              p_trace_context,
              p_metadata,
              v_run_at,
              p_dedupe_key,
              v_singleton_on,
              p_cron_expression,
              coalesce(p_priority, 0),
              p_group
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
              trace_context,
              metadata,
              run_at,
              dedupe_key,
              singleton_on,
              cron_expression,
              priority,
              "group"
          ) values (
              pgconductor._private_portable_uuidv7(),
              p_task_key,
              p_queue,
              p_payload,
              p_trace_context,
              p_metadata,
              v_next_singleton_on,
              p_dedupe_key,
              v_next_singleton_on,
              p_cron_expression,
              coalesce(p_priority, 0),
              p_group
          )
          on conflict (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
          where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false
          do update set
              payload = excluded.payload,
              trace_context = excluded.trace_context,
              metadata = excluded.metadata,
              run_at = excluded.run_at,
              priority = excluded.priority,
              cron_expression = excluded.cron_expression,
              "group" = excluded."group"
          returning _private_executions.id;
          return;
      end if;
  end if;

  -- regular invoke (no singleton)
  return query insert into pgconductor._private_executions as e (
    id,
    task_key,
    queue,
    payload,
    trace_context,
    metadata,
    run_at,
    dedupe_key,
    cron_expression,
    priority,
    "group"
  ) values (
    pgconductor._private_portable_uuidv7(),
    p_task_key,
    p_queue,
    p_payload,
    p_trace_context,
    p_metadata,
    v_run_at,
    p_dedupe_key,
    p_cron_expression,
    coalesce(p_priority, 0),
    p_group
  )
  on conflict (task_key, dedupe_key, queue) where singleton_on is null do update set
    payload = excluded.payload,
    trace_context = excluded.trace_context,
    metadata = excluded.metadata,
    run_at = excluded.run_at,
    priority = excluded.priority,
    cron_expression = excluded.cron_expression,
    "group" = excluded."group"
  where e.locked_at is null
  returning e.id;

  -- a claim landed after the supersede check; the upsert locked that execution, so a retry supersedes it
  if not found then
    return query select * from pgconductor.invoke(
      p_task_key,
      p_queue,
      p_payload,
      p_run_at,
      p_dedupe_key,
      p_dedupe_seconds,
      p_dedupe_next_slot,
      p_cron_expression,
      p_priority,
      p_group,
      p_trace_context,
      p_metadata
    );
  end if;
end;
$function$
;

-- stores a signal for an execution whose row the caller has locked; an id stored before is
-- ignored. the worker running the execution reads it with its next claim. a new signal resolves
-- the ctx.waitForAny() signal branches listening for its name and wakes an execution suspended
-- at a wait, like dispatch does. registration holds the same row lock, so it either sees the
-- signal or its branch is resolved here.
create or replace function pgconductor._private_store_signal(
  p_execution_id uuid,
  p_queue text,
  p_name text,
  p_id text,
  p_payload jsonb
)
returns void
language plpgsql
volatile
set search_path to ''
as $function$
begin
  insert into pgconductor._private_execution_signals (execution_id, queue, id, name, payload)
  values (p_execution_id, p_queue, p_id, p_name, p_payload)
  on conflict (execution_id, id) do nothing;

  if not found then
    return;
  end if;

  update pgconductor._private_executions
  set signal_count = signal_count + 1
  where id = p_execution_id and queue = p_queue;

  with stored_steps as (
    insert into pgconductor._private_steps (execution_id, queue, key, result)
    select p_execution_id, p_queue, subscription.step_key, jsonb_build_object(
      'status', 'resolved', 'signal', jsonb_build_object('id', p_id, 'name', p_name)
    )
    from pgconductor._private_custom_event_subscriptions subscription
    where subscription.kind = 'execution_wait'
      and subscription.execution_id = p_execution_id
      and subscription.signal_name = p_name
      and (subscription.expires_at is null
        or subscription.expires_at >= pgconductor._private_current_time())
    on conflict (execution_id, key) do nothing
    returning key
  ), deleted_waits as (
    delete from pgconductor._private_custom_event_subscriptions subscription
    using stored_steps step
    where subscription.kind = 'execution_wait'
      and subscription.execution_id = p_execution_id
      and subscription.step_key = step.key
  )
  update pgconductor._private_executions e
  set run_at = pgconductor._private_current_time(), waiting_step_key = null
  where e.id = p_execution_id and e.queue = p_queue
    and e.locked_by is null
    and e.waiting_on_execution_id is null
    and e.waiting_step_key is not null
    and exists (select 1 from stored_steps);
end;
$function$;

-- signal an execution. returns false when it is completed, failed, cancelled or unknown.
create or replace function pgconductor.signal_execution(
  p_execution_id uuid,
  p_name text,
  p_id text,
  p_payload jsonb default null
)
returns boolean
language plpgsql
volatile
set search_path to ''
as $function$
declare
  v_queue text;
begin
  -- the row lock orders this with settlement, which re-queues an execution that has signals it
  -- did not deliver
  select e.queue
  into v_queue
  from pgconductor._private_executions e
  where e.id = p_execution_id
    and e.completed_at is null
    and e.failed_at is null
    and not e.cancelled
  for update;

  if not found then
    return false;
  end if;

  perform pgconductor._private_store_signal(p_execution_id, v_queue, p_name, p_id, p_payload);
  return true;
end;
$function$;

-- signal the non-terminal execution of (task, key), or start one with the signal stored
create or replace function pgconductor.signal_with_start(
  p_task_key text,
  p_queue text,
  p_key text,
  p_name text,
  p_id text,
  p_payload jsonb default null,
  p_input jsonb default null,
  p_trace_context jsonb default null,
  p_metadata jsonb default null,
  p_priority integer default null
)
returns uuid
language plpgsql
volatile
set search_path to ''
as $function$
declare
  v_execution_id uuid;
begin
  if p_key is null or btrim(p_key) = '' then
    raise exception 'signal_with_start requires a non-empty key';
  end if;

  perform pgconductor._private_check_metadata(p_metadata);

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'pgconductor.key:' || p_queue || ':' || p_task_key || ':' || p_key, 0
  ));

  -- a concurrent settlement holding the row is waited for; if it completed the execution, the
  -- row no longer matches and a new execution starts
  select e.id
  into v_execution_id
  from pgconductor._private_executions e
  where e.task_key = p_task_key
    and e.queue = p_queue
    and e.key = p_key
    and e.completed_at is null
    and e.failed_at is null
    and not e.cancelled
  for update;

  if not found then
    insert into pgconductor._private_executions (
      id, task_key, queue, payload, trace_context, metadata, key, priority
    ) values (
      pgconductor._private_portable_uuidv7(),
      p_task_key,
      p_queue,
      p_input,
      p_trace_context,
      p_metadata,
      p_key,
      coalesce(p_priority, 0)
    )
    returning id into v_execution_id;
  end if;

  perform pgconductor._private_store_signal(v_execution_id, p_queue, p_name, p_id, p_payload);
  return v_execution_id;
end;
$function$;

-- locks the executions in p_ids all at once, or waits without holding any of them: waiting for
-- one while holding another can deadlock with a statement that locks them the other way round
create or replace function pgconductor._private_lock_executions(p_ids uuid[])
returns void
language plpgsql
volatile
set search_path to ''
as $function$
declare
  v_id uuid;
begin
  loop
    begin
      perform 1
      from pgconductor._private_executions e
      where e.id = any(p_ids)
      for update of e nowait;
      return;
    exception when lock_not_available then
      -- wait for each one alone, rolling back to release it, then try them all again
      foreach v_id in array p_ids loop
        begin
          perform 1 from pgconductor._private_executions e where e.id = v_id for update of e;
          raise exception using errcode = 'PGC01';
        exception when sqlstate 'PGC01' then
          null;
        end;
      end loop;
    end;
  end loop;
end;
$function$;

-- cancels the linked children of ended executions ([{execution_id, reason}]) with the reason
-- their parent ended with. callers run it after their own writes, and it reads the children
-- with a fresh snapshot, so it also finds the ones started while the caller waited for a lock.
create or replace function pgconductor._private_cancel_linked_children(p_parents jsonb)
returns void
language plpgsql
volatile
set search_path to ''
as $function$
begin
  perform pgconductor.cancel_execution(child.id, parent.reason)
  from jsonb_to_recordset(p_parents) as parent(execution_id uuid, reason text)
  join pgconductor._private_executions child on child.parent_execution_id = parent.execution_id
  where child.cancel_with_parent
    and child.completed_at is null
    and child.failed_at is null;
end;
$function$;

-- cancel an execution
create or replace function pgconductor.cancel_execution(
  p_execution_id uuid,
  p_reason text default 'Cancelled by user'
)
returns boolean
language plpgsql
volatile
set search_path to ''
as $function$
declare
  v_orchestrator_id uuid;
  v_queue text;
  v_descendant_ids uuid[];
  v_ancestor_ids uuid[];
  v_linked_ids uuid[];
  v_locked_ids uuid[];
  v_rows_affected integer;
  v_resolved integer;
begin
  -- a waiting execution is cancelled with the executions it waits on and its
  -- linked children, and fails the executions waiting on it. the graph may
  -- change while we wait for locks, so it is read again after locking. a
  -- changed graph releases the stale locks by rolling back, then locks the new
  -- graph. the whole graph is locked at once, so a settlement that locks a
  -- child before the parent waiting on it cannot deadlock with this, and the
  -- calls for linked children find their locks held.
  loop
    begin
      perform pgconductor._private_lock_executions(v_locked_ids);

      with recursive descendants as (
        select e.waiting_on_execution_id as id, 1 as depth
        from pgconductor._private_executions e
        where e.id = p_execution_id and e.waiting_on_execution_id is not null
        union all
        select e.waiting_on_execution_id, d.depth + 1
        from descendants d
        join pgconductor._private_executions e on e.id = d.id
        where e.waiting_on_execution_id is not null
      )
      select array_agg(id order by depth desc) into v_descendant_ids from descendants;

      with recursive ancestors as (
        select e.id, 1 as depth
        from pgconductor._private_executions e
        where e.waiting_on_execution_id = p_execution_id and e.locked_by is null
        union all
        select e.id, a.depth + 1
        from ancestors a
        join pgconductor._private_executions e on e.waiting_on_execution_id = a.id
        where e.locked_by is null
      )
      select array_agg(id order by depth) into v_ancestor_ids from ancestors;

      -- a running execution is only flagged; its settlement cancels its linked children
      with recursive linked as (
        select child.id, child.waiting_on_execution_id
        from pgconductor._private_executions e
        join pgconductor._private_executions child
          on child.parent_execution_id = e.id and child.cancel_with_parent
        where e.id = any(v_descendant_ids || p_execution_id || v_ancestor_ids)
          and (e.id <> p_execution_id or e.locked_by is null)
          and child.completed_at is null
          and child.failed_at is null
        union all
        select child.id, child.waiting_on_execution_id
        from linked l
        join pgconductor._private_executions child
          on child.parent_execution_id = l.id
          and (child.cancel_with_parent or child.id = l.waiting_on_execution_id)
        where child.completed_at is null and child.failed_at is null
      )
      select array_agg(id order by id) into v_linked_ids from linked;

      exit when v_locked_ids = v_linked_ids || v_descendant_ids || p_execution_id || v_ancestor_ids;
      v_locked_ids := v_linked_ids || v_descendant_ids || p_execution_id || v_ancestor_ids;
      raise exception using errcode = 'PGC01';
    exception when sqlstate 'PGC01' then
      null;
    end;
  end loop;

  select locked_by, queue
  into v_orchestrator_id, v_queue
  from pgconductor._private_executions
  where id = p_execution_id
    and completed_at is null
    and failed_at is null;

  if not found then
    return false;
  end if;

  if v_orchestrator_id is not null then
    -- running: set cancelled flag, the worker running it aborts it on its next poll
    update pgconductor._private_executions
    set
      cancelled = true,
      last_error = p_reason
    where id = p_execution_id
      and queue = v_queue
      and cancelled = false;

    get diagnostics v_rows_affected = row_count;

    if v_rows_affected > 0 then
      delete from pgconductor._private_custom_event_subscriptions
      where kind = 'execution_wait'
        and execution_id = p_execution_id;
    end if;

    return v_rows_affected > 0;
  end if;

  -- running descendants are aborted by the workers running them on their next poll
  update pgconductor._private_executions e
  set cancelled = true, last_error = p_reason
  where e.id = any(v_descendant_ids)
    and e.locked_by is not null
    and e.completed_at is null
    and e.failed_at is null
    and e.cancelled = false;

  -- pending: fail immediately
  with pending as (
    select e.id, e.queue,
      case when e.id = any(v_ancestor_ids) then 'Child execution failed: ' || p_reason
        else p_reason end as error,
      e.id = any(v_ancestor_ids) is not true as cancelled,
      t.remove_on_fail_days = 0 as should_remove,
      e.result_waiters
    from pgconductor._private_executions e
    left join pgconductor._private_tasks t on t.key = e.task_key and t.queue = e.queue
    where e.id = any(v_descendant_ids || p_execution_id || v_ancestor_ids)
      and e.locked_by is null
      and e.completed_at is null
      and e.failed_at is null
  ),
  removed as (
    delete from pgconductor._private_executions e
    using pending p
    where e.id = p.id and e.queue = p.queue and p.should_remove
  ),
  failed as (
    update pgconductor._private_executions e
    set
      failed_at = pgconductor._private_current_time(),
      last_error = p.error,
      cancelled = p.cancelled,
      waiting_on_execution_id = null,
      waiting_step_key = null
    from pending p
    where e.id = p.id and e.queue = p.queue and p.should_remove is not true
  ),
  deleted_waits as (
    delete from pgconductor._private_custom_event_subscriptions s
    using pending p
    where s.kind = 'execution_wait'
      and s.execution_id = p.id
  )
  select pgconductor._private_resolve_result_waits(jsonb_agg(
    w.value || jsonb_build_object('outcome', case when p.cancelled
      then jsonb_build_object('status', 'cancelled', 'reason', p.error)
      else jsonb_build_object('status', 'failed', 'error', p.error) end)
  ))
  into v_resolved
  from pending p
  cross join lateral jsonb_array_elements(p.result_waiters) w
  having count(*) > 0;

  perform pgconductor._private_cancel_linked_children(jsonb_agg(jsonb_build_object(
    'execution_id', id,
    'reason', case when id = any(v_ancestor_ids) then 'Child execution failed: ' || p_reason
      else p_reason end
  )))
  from unnest(v_descendant_ids || p_execution_id || v_ancestor_ids) id;

  return true;
end;
$function$;

-- Event deliveries use the source dispatch execution as durable lineage without
-- participating in workflow parent/child settlement.
create unique index idx_executions_event_destination
    on pgconductor._private_executions (parent_execution_id, subscription_id, queue)
    where subscription_id is not null;

create table pgconductor._private_custom_event_subscriptions (
    id uuid primary key default pgconductor._private_portable_uuidv7(),
    -- null for a wait on the outcome of another execution or on a signal
    event_key text,
    task_key text not null,
    queue text not null,
    payload_fields text[],
    required_field_count smallint not null check (required_field_count between 0 and 8),
    created_at timestamptz not null default pgconductor._private_current_time(),
    kind text not null default 'task_trigger',
    execution_id uuid,
    step_key text,
    expires_at timestamptz,
    awaited_execution_id uuid,
    -- a ctx.waitForAny() branch that a signal with this name wakes
    signal_name text,
    constraint chk_custom_event_subscription_event_key check (
        btrim(event_key) <> '' and octet_length(event_key) between 1 and 255
    ),
    constraint chk_custom_event_subscription_kind check (
        kind in ('task_trigger', 'execution_wait')
    ),
    constraint chk_custom_event_subscription_shape check (
        (kind = 'task_trigger'
            and event_key is not null
            and execution_id is null
            and step_key is null
            and expires_at is null
            and awaited_execution_id is null
            and signal_name is null)
        or
        (kind = 'execution_wait'
            and execution_id is not null
            and step_key is not null
            and payload_fields is null
            and num_nonnulls(event_key, awaited_execution_id, signal_name) = 1)
    ),
    constraint fk_custom_event_subscription_execution
        foreign key (execution_id, queue)
        references pgconductor._private_executions(id, queue)
        on delete cascade
);

create index idx_custom_event_subscriptions_event
    on pgconductor._private_custom_event_subscriptions
       (event_key, required_field_count, id);

create unique index idx_custom_event_subscription_execution_wait
    on pgconductor._private_custom_event_subscriptions (execution_id, step_key);

create index idx_custom_event_subscription_wait_match
    on pgconductor._private_custom_event_subscriptions
       (event_key, created_at, expires_at, id)
    where kind = 'execution_wait';

-- drop a queue and everything that would write into its partition (the trigger drops the partition)
create or replace function pgconductor.drop_queue(queue_name text)
 returns void
 language sql
 volatile
 set search_path to ''
as $function$
  -- registration locks a queue before its tasks, so this locks the queue and the queues whose
  -- tasks dead-letter into it first, in the same order
  select 1
  from pgconductor._private_queues queue
  where queue.name = drop_queue.queue_name
    or queue.name in (
      select task.queue from pgconductor._private_tasks task
      where task.dead_letter_queue = drop_queue.queue_name
    )
  order by queue.name
  for update;

  update pgconductor._private_tasks
  set dead_letter_queue = null, dead_letter_task_key = null
  where dead_letter_queue = drop_queue.queue_name;

  delete from pgconductor._private_custom_event_subscriptions where queue = drop_queue.queue_name;

  delete from pgconductor._private_tasks where queue = drop_queue.queue_name;

  delete from pgconductor._private_queues where name = drop_queue.queue_name;
$function$;

-- TypeScript validates and compiles filters once. Each row is one typed OR
-- alternative; rows sharing a subscription and field form one clause, while
-- distinct fields are ANDed by the dispatch query.
create table pgconductor._private_event_filter_terms (
    subscription_id uuid not null,
    term_number smallint not null check (term_number > 0),
    event_key text not null,
    field_name text not null,
    operator text not null check (
        operator in ('exact', 'prefix', 'numeric_range', 'exists', 'anything_but')
    ),
    scalar_type text check (scalar_type in ('string', 'number', 'boolean', 'null')),
    text_value text,
    number_value numeric,
    boolean_value boolean,
    prefix_length smallint generated always as (
        case when operator = 'prefix' then length(text_value)::smallint end
    ) stored,
    number_range numrange,
    primary key (subscription_id, term_number),
    constraint chk_event_filter_term_names check (
        btrim(event_key) <> '' and octet_length(event_key) between 1 and 255
        and btrim(field_name) <> '' and octet_length(field_name) between 1 and 128
        and (text_value is null or octet_length(text_value) <= 1024)
    ),
    constraint chk_event_filter_term_value check ((
        case operator
            when 'prefix' then scalar_type = 'string'
                and text_value is not null and length(text_value) between 1 and 64
                and number_value is null and boolean_value is null and number_range is null
            when 'numeric_range' then scalar_type = 'number'
                and text_value is null and number_value is null and boolean_value is null
                and number_range is not null and not isempty(number_range)
            when 'exists' then scalar_type is null
                and text_value is null and number_value is null
                and boolean_value is not null and number_range is null
            when 'exact' then number_range is null and (
                (scalar_type = 'string' and text_value is not null
                    and number_value is null and boolean_value is null)
                or (scalar_type = 'number' and text_value is null
                    and number_value is not null and boolean_value is null)
                or (scalar_type = 'boolean' and text_value is null
                    and number_value is null and boolean_value is not null)
                or (scalar_type = 'null' and text_value is null
                    and number_value is null and boolean_value is null)
            )
            when 'anything_but' then number_range is null and (
                (scalar_type = 'string' and text_value is not null
                    and number_value is null and boolean_value is null)
                or (scalar_type = 'number' and text_value is null
                    and number_value is not null and boolean_value is null)
                or (scalar_type = 'boolean' and text_value is null
                    and number_value is null and boolean_value is not null)
                or (scalar_type = 'null' and text_value is null
                    and number_value is null and boolean_value is null)
            )
            else false
        end
    ) is true),
    constraint fk_event_filter_term_subscription foreign key (subscription_id)
        references pgconductor._private_custom_event_subscriptions(id) on delete cascade
);

create index idx_event_filter_term_exact_text
    on pgconductor._private_event_filter_terms
       (event_key collate "C", field_name collate "C", text_value collate "C", subscription_id)
    where operator = 'exact' and scalar_type = 'string';
create index idx_event_filter_term_exact_number
    on pgconductor._private_event_filter_terms
       (event_key, field_name, number_value, subscription_id)
    where operator = 'exact' and scalar_type = 'number';
create index idx_event_filter_term_exact_boolean
    on pgconductor._private_event_filter_terms
       (event_key, field_name, boolean_value, subscription_id)
    where operator = 'exact' and scalar_type = 'boolean';
create index idx_event_filter_term_exact_null
    on pgconductor._private_event_filter_terms
       (event_key, field_name, subscription_id)
    where operator = 'exact' and scalar_type = 'null';
create index idx_event_filter_term_prefix
    on pgconductor._private_event_filter_terms
       (event_key collate "C", field_name collate "C", prefix_length,
        text_value collate "C", subscription_id)
    where operator = 'prefix';
create index idx_event_filter_term_numeric_range
    on pgconductor._private_event_filter_terms using gist
       (event_key, field_name, number_range, subscription_id)
    where operator = 'numeric_range';
create index idx_event_filter_term_exists_true
    on pgconductor._private_event_filter_terms
       (event_key, field_name, subscription_id)
    where operator = 'exists' and boolean_value;
create index idx_event_filter_term_exists_false
    on pgconductor._private_event_filter_terms
       (event_key, subscription_id, field_name)
    where operator = 'exists' and not boolean_value;
create index idx_event_filter_term_anything_but
    on pgconductor._private_event_filter_terms
       (event_key, field_name, subscription_id)
    where operator = 'anything_but';

create or replace function pgconductor._private_expand_event_filter_terms(
    p_subscription_id uuid,
    p_event_key text,
    p_terms jsonb
)
returns table (
    subscription_id uuid,
    term_number smallint,
    event_key text,
    field_name text,
    operator text,
    scalar_type text,
    text_value text,
    number_value numeric,
    boolean_value boolean,
    number_range numrange
)
language sql
immutable
as $function$
    select
        p_subscription_id,
        term.ordinality::smallint,
        p_event_key,
        fields.field_name,
        fields.operator,
        fields.scalar_type,
        fields.text_value,
        fields.number_value,
        fields.boolean_value,
        case when fields.operator = 'numeric_range' then pg_catalog.numrange(
            fields.lower_value,
            fields.upper_value,
            (case when fields.lower_inclusive then '[' else '(' end)
                ||
            (case when fields.upper_inclusive then ']' else ')' end)
        ) end
    from pg_catalog.jsonb_array_elements(coalesce(p_terms, '[]'::jsonb))
        with ordinality as term(value, ordinality)
    cross join lateral pg_catalog.jsonb_to_record(term.value) as fields(
        field_name text,
        operator text,
        scalar_type text,
        text_value text,
        number_value numeric,
        boolean_value boolean,
        lower_value numeric,
        upper_value numeric,
        lower_inclusive boolean,
        upper_inclusive boolean
    );
$function$;

-- Registers the subscription for a wait step and, when p_suspend is set,
-- releases the execution until an event or the timeout wakes it. Without
-- p_suspend the execution keeps running and dispatch stores a matching event
-- as the step result; an existing subscription is left as it is. Returns the
-- step result once the wait has settled.
create or replace function pgconductor._private_register_event_wait(
    p_execution_id uuid,
    p_queue text,
    p_task_key text,
    p_orchestrator_id uuid,
    p_claim_token uuid,
    p_event_key text,
    p_step_key text,
    p_required_field_count smallint,
    p_terms jsonb,
    p_timeout_ms bigint,
    p_suspend boolean
)
returns jsonb
language plpgsql
volatile
set search_path to ''
as $function$
declare
    v_subscription_id uuid;
    v_expires_at timestamptz;
    v_result jsonb;
    v_now timestamptz := pgconductor._private_current_time();
begin
    perform 1
    from pgconductor._private_executions execution
    where execution.id = p_execution_id
      and execution.queue = p_queue
      and execution.task_key = p_task_key
      and execution.locked_by = p_orchestrator_id
      and execution.claim_token = p_claim_token
      and execution.completed_at is null
      and execution.failed_at is null
      and not execution.cancelled
    for update;

    if not found then
        raise exception 'execution % is not claimed by orchestrator %', p_execution_id, p_orchestrator_id;
    end if;

    -- Dispatch locks the execution before storing a result, so a result stored
    -- while the execution was claimed is visible here.
    select step.result
    into v_result
    from pgconductor._private_steps step
    where step.execution_id = p_execution_id
      and step.key = p_step_key;

    if found then
        return v_result;
    end if;

    select subscription.id, subscription.expires_at
    into v_subscription_id, v_expires_at
    from pgconductor._private_custom_event_subscriptions subscription
    where subscription.kind = 'execution_wait'
      and subscription.execution_id = p_execution_id
      and subscription.queue = p_queue
      and subscription.step_key = p_step_key
    for update;

    if not found then
        v_expires_at := v_now + p_timeout_ms * interval '1 millisecond';

        insert into pgconductor._private_custom_event_subscriptions (
            event_key,
            task_key,
            queue,
            payload_fields,
            required_field_count,
            kind,
            execution_id,
            step_key,
            expires_at
        ) values (
            p_event_key,
            p_task_key,
            p_queue,
            null,
            p_required_field_count,
            'execution_wait',
            p_execution_id,
            p_step_key,
            v_expires_at
        )
        returning id into v_subscription_id;

        insert into pgconductor._private_event_filter_terms (
            subscription_id,
            term_number,
            event_key,
            field_name,
            operator,
            scalar_type,
            text_value,
            number_value,
            boolean_value,
            number_range
        )
        select *
        from pgconductor._private_expand_event_filter_terms(
            v_subscription_id,
            p_event_key,
            p_terms
        );
    elsif not p_suspend then
        return null;
    elsif v_expires_at <= v_now then
        delete from pgconductor._private_custom_event_subscriptions
        where id = v_subscription_id;

        insert into pgconductor._private_steps (execution_id, queue, key, result)
        values (
            p_execution_id,
            p_queue,
            p_step_key,
            jsonb_build_object('status', 'timed_out')
        )
        on conflict (execution_id, key) do nothing;

        return jsonb_build_object('status', 'timed_out');
    elsif v_expires_at is null then
        -- A subscription registered without suspending starts its timeout here.
        v_expires_at := v_now + p_timeout_ms * interval '1 millisecond';

        update pgconductor._private_custom_event_subscriptions
        set expires_at = v_expires_at
        where id = v_subscription_id;
    end if;

    if p_suspend then
        update pgconductor._private_executions
        set attempts = greatest(attempts - 1, 0),
            run_at = coalesce(v_expires_at, 'infinity'),
            waiting_step_key = p_step_key,
            locked_by = null,
            locked_at = null
        where id = p_execution_id and queue = p_queue;
    end if;

    return null;
end;
$function$;

-- Stores the outcomes of settled executions for the waits in p_waits, read from their
-- result_waiters ([{execution_id, queue, step_key, outcome}]), and wakes waiters suspended
-- at a wait, like dispatch does. An outcome after the deadline of its wait is not stored,
-- so the wait times out. Every path that ends an execution calls it with the
-- result_waiters of its locked or deleted row, and only when there are waiters, so
-- settlement stays cheap to plan.
create or replace function pgconductor._private_resolve_result_waits(p_waits jsonb)
returns integer
language plpgsql
volatile
set search_path to ''
as $function$
declare
    v_resolved integer;
begin
    with waits as materialized (
        select *
        from jsonb_to_recordset(p_waits)
            as w(execution_id uuid, queue text, step_key text, outcome jsonb)
    ), locked_waiters as materialized (
        select e.id, e.queue,
            e.locked_by is null
                and e.waiting_on_execution_id is null
                and e.waiting_step_key is not null as suspended
        from pgconductor._private_executions e
        where (e.id, e.queue) in (select w.execution_id, w.queue from waits w)
            and e.completed_at is null
            and e.failed_at is null
            and not e.cancelled
        order by e.id
        for update of e
    ), stored_steps as (
        insert into pgconductor._private_steps (execution_id, queue, key, result)
        select w.execution_id, w.queue, w.step_key,
            jsonb_build_object('status', 'resolved', 'outcome', w.outcome)
        from waits w
        join locked_waiters l on l.id = w.execution_id and l.queue = w.queue
        join pgconductor._private_custom_event_subscriptions subscription
            on subscription.kind = 'execution_wait'
            and subscription.execution_id = w.execution_id
            and subscription.step_key = w.step_key
        where subscription.expires_at is null
            or subscription.expires_at >= pgconductor._private_current_time()
        on conflict (execution_id, key) do nothing
        returning execution_id, queue, key
    ), deleted_waits as (
        delete from pgconductor._private_custom_event_subscriptions subscription
        using stored_steps step
        where subscription.kind = 'execution_wait'
            and subscription.execution_id = step.execution_id
            and subscription.step_key = step.key
    ), woken as (
        update pgconductor._private_executions e
        set run_at = pgconductor._private_current_time(), waiting_step_key = null
        from locked_waiters l
        where e.id = l.id and e.queue = l.queue and l.suspended
            and exists (select 1 from stored_steps step where step.execution_id = l.id)
    )
    select count(*) into v_resolved from stored_steps;

    return v_resolved;
end;
$function$;

-- Registers a waitForAny step over subscriptions created by
-- _private_register_event_wait, referenced by their step keys. The branch whose
-- event was stored first wins (then branch order). An { execution } branch is
-- registered by the first call: the outcome of a settled execution is stored at once,
-- otherwise the wait gets a subscription and is added to the execution's result_waiters,
-- from which settlement and cancel_execution store the outcome. A { signal } branch is
-- registered here too: an undelivered signal with its name, or the one whose handler is
-- running (p_delivering_signal_id), is stored at once, otherwise the wait gets a
-- subscription from which _private_store_signal stores the next one. Otherwise the timeout is set
-- on all branch subscriptions and the execution is released until dispatch wakes
-- it or the timeout passes. A race without a subscription has nothing to wait on and
-- raises. Settling removes the branch subscriptions and closes the branches, so
-- subscribing again on a resume is a no-op.
create or replace function pgconductor._private_register_event_race(
    p_execution_id uuid,
    p_queue text,
    p_task_key text,
    p_orchestrator_id uuid,
    p_claim_token uuid,
    p_step_key text,
    p_branches jsonb,
    p_timeout_ms bigint,
    p_delivering_signal_id text default null
)
returns jsonb
language plpgsql
volatile
set search_path to ''
as $function$
declare
    v_step_keys text[] := array(select jsonb_array_elements(p_branches)->>'stepKey');
    v_awaited_ids uuid[] := array(
        select (branch->>'executionId')::uuid
        from jsonb_array_elements(p_branches) branch
        where branch ? 'executionId'
    );
    v_new_branches jsonb;
    v_unknown_id uuid;
    v_result jsonb;
    v_expires_at timestamptz;
    v_subscriptions bigint;
    v_now timestamptz := pgconductor._private_current_time();
begin
    -- Settlement locks an execution before its waiters, so awaited executions are locked first,
    -- all at once: one of them may be waiting on another
    if cardinality(v_awaited_ids) > 0 then
        perform pgconductor._private_lock_executions(v_awaited_ids);
    end if;

    perform 1
    from pgconductor._private_executions execution
    where execution.id = p_execution_id
      and execution.queue = p_queue
      and execution.task_key = p_task_key
      and execution.locked_by = p_orchestrator_id
      and execution.claim_token = p_claim_token
      and execution.completed_at is null
      and execution.failed_at is null
      and not execution.cancelled
    for update;

    if not found then
        raise exception 'execution % is not claimed by orchestrator %', p_execution_id, p_orchestrator_id;
    end if;

    select step.result
    into v_result
    from pgconductor._private_steps step
    where step.execution_id = p_execution_id
      and step.key = p_step_key;

    if found then
        return v_result;
    end if;

    if cardinality(v_awaited_ids) > 0 then
        -- A branch with a stored step already received its outcome, and one with a subscription
        -- receives it from the path that ends the execution, which checks the deadline
        select coalesce(jsonb_agg(branch), '[]'::jsonb)
        into v_new_branches
        from jsonb_array_elements(p_branches) branch
        where branch ? 'executionId'
          and not exists (
              select 1 from pgconductor._private_steps step
              where step.execution_id = p_execution_id and step.key = branch->>'stepKey'
          )
          and not exists (
              select 1 from pgconductor._private_custom_event_subscriptions subscription
              where subscription.kind = 'execution_wait'
                and subscription.execution_id = p_execution_id
                and subscription.step_key = branch->>'stepKey'
          );

        select (branch->>'executionId')::uuid
        into v_unknown_id
        from jsonb_array_elements(v_new_branches) branch
        where not exists (
            select 1 from pgconductor._private_executions awaited
            where awaited.id = (branch->>'executionId')::uuid
              and (branch->>'taskKey' is null
                  or (awaited.task_key = branch->>'taskKey' and awaited.queue = branch->>'queue'))
        )
        limit 1;

        if found then
            raise exception 'Execution % not found', v_unknown_id;
        end if;

        with branches as materialized (
            select branch->>'stepKey' as step_key, awaited.id,
                case
                    when awaited.completed_at is not null
                        then jsonb_build_object('status', 'completed', 'result', awaited.result)
                    when awaited.failed_at is null then null
                    when awaited.cancelled
                        then jsonb_build_object('status', 'cancelled', 'reason', awaited.last_error)
                    else jsonb_build_object('status', 'failed', 'error', awaited.last_error)
                end as outcome
            from jsonb_array_elements(v_new_branches) branch
            join pgconductor._private_executions awaited
                on awaited.id = (branch->>'executionId')::uuid
                and (branch->>'taskKey' is null
                    or (awaited.task_key = branch->>'taskKey' and awaited.queue = branch->>'queue'))
        ), settled as (
            insert into pgconductor._private_steps (execution_id, queue, key, result)
            select p_execution_id, p_queue, b.step_key,
                jsonb_build_object('status', 'resolved', 'outcome', b.outcome)
            from branches b
            where b.outcome is not null
        ), registered as (
            insert into pgconductor._private_custom_event_subscriptions (
                task_key, queue, required_field_count, kind, execution_id, step_key,
                awaited_execution_id
            )
            select p_task_key, p_queue, 0, 'execution_wait', p_execution_id, b.step_key, b.id
            from branches b
            where b.outcome is null
            on conflict (execution_id, step_key) do nothing
            returning step_key, awaited_execution_id
        )
        update pgconductor._private_executions awaited
        set result_waiters = coalesce(awaited.result_waiters, '[]'::jsonb) || r.waiters
        from (
            select awaited_execution_id, jsonb_agg(jsonb_build_object(
                'execution_id', p_execution_id, 'queue', p_queue, 'step_key', step_key
            )) as waiters
            from registered
            group by awaited_execution_id
        ) r
        where awaited.id = r.awaited_execution_id;
    end if;

    -- A { signal } branch wins with a signal whose handler has not returned. Senders lock this
    -- row, so a signal stored after this check resolves the branch through its subscription
    with branches as materialized (
        select branch->>'stepKey' as step_key, branch->>'signal' as name
        from jsonb_array_elements(p_branches) branch
        where branch ? 'signal'
          and not exists (
              select 1 from pgconductor._private_steps step
              where step.execution_id = p_execution_id and step.key = branch->>'stepKey'
          )
    ), received as (
        select distinct on (b.step_key) b.step_key, s.id, s.name
        from branches b
        join pgconductor._private_execution_signals s
            on s.execution_id = p_execution_id
            and s.name = b.name
            and (s.delivered_at is null or s.id = p_delivering_signal_id)
        where not exists (
            select 1 from pgconductor._private_custom_event_subscriptions subscription
            where subscription.kind = 'execution_wait'
              and subscription.execution_id = p_execution_id
              and subscription.step_key = b.step_key
        )
        order by b.step_key, s.seq
    ), settled as (
        insert into pgconductor._private_steps (execution_id, queue, key, result)
        select p_execution_id, p_queue, r.step_key, jsonb_build_object(
            'status', 'resolved', 'signal', jsonb_build_object('id', r.id, 'name', r.name)
        )
        from received r
    )
    insert into pgconductor._private_custom_event_subscriptions (
        task_key, queue, required_field_count, kind, execution_id, step_key, signal_name
    )
    select p_task_key, p_queue, 0, 'execution_wait', p_execution_id, b.step_key, b.name
    from branches b
    where not exists (select 1 from received r where r.step_key = b.step_key)
    on conflict (execution_id, step_key) do nothing;

    select jsonb_build_object('key', branch.value->>'key') || (step.result - 'status')
    into v_result
    from jsonb_array_elements(p_branches) with ordinality branch
    join pgconductor._private_steps step
        on step.execution_id = p_execution_id
        and step.key = branch.value->>'stepKey'
    where step.result->>'status' = 'resolved'
    order by step.created_at, branch.ordinality
    limit 1;

    if v_result is null then
        -- The first call sets the timeout, so later calls find it set.
        select max(subscription.expires_at), count(*)
        into v_expires_at, v_subscriptions
        from pgconductor._private_custom_event_subscriptions subscription
        where subscription.kind = 'execution_wait'
          and subscription.execution_id = p_execution_id
          and subscription.step_key = any(v_step_keys);

        if v_subscriptions = 0 then
            raise exception 'waitForAny "%" has no open branch', p_step_key;
        elsif v_expires_at is null then
            v_expires_at := v_now + p_timeout_ms * interval '1 millisecond';

            update pgconductor._private_custom_event_subscriptions subscription
            set expires_at = v_expires_at
            where subscription.kind = 'execution_wait'
              and subscription.execution_id = p_execution_id
              and subscription.step_key = any(v_step_keys);
        elsif v_expires_at <= v_now then
            v_result := jsonb_build_object('key', 'timeout');
        end if;
    end if;

    if v_result is null then
        update pgconductor._private_executions
        set attempts = greatest(attempts - 1, 0),
            run_at = coalesce(v_expires_at, 'infinity'),
            waiting_step_key = p_step_key,
            locked_by = null,
            locked_at = null
        where id = p_execution_id and queue = p_queue;

        return null;
    end if;

    insert into pgconductor._private_steps (execution_id, queue, key, result)
    select p_execution_id, p_queue, p_step_key, v_result
    union all
    select p_execution_id, p_queue, step_key, jsonb_build_object('status', 'closed')
    from unnest(v_step_keys) step_key
    on conflict (execution_id, key) do nothing;

    delete from pgconductor._private_custom_event_subscriptions subscription
    where subscription.kind = 'execution_wait'
      and subscription.execution_id = p_execution_id
      and subscription.step_key = any(v_step_keys);

    -- Executions still running no longer need to resolve this race
    if cardinality(v_awaited_ids) > 0 then
        update pgconductor._private_executions awaited
        set result_waiters = (
            select jsonb_agg(w)
            from jsonb_array_elements(awaited.result_waiters) w
            where not (w->>'execution_id' = p_execution_id::text and w->>'step_key' = any(v_step_keys))
        )
        where awaited.id = any(v_awaited_ids)
          and awaited.completed_at is null
          and awaited.failed_at is null
          and awaited.result_waiters is not null;
    end if;

    return v_result;
end;
$function$;

-- Worker registration replaces only durable task-trigger subscriptions. Active
-- execution waits belong to running workflows and survive worker restarts.
-- Ids derive from the definition so unchanged subscriptions keep their delivery
-- identity and retried dispatches do not deliver to them twice.
create or replace function pgconductor._private_replace_custom_event_subscriptions(
    p_queue_name text,
    p_subscriptions pgconductor.event_subscription_spec[]
)
returns void
language sql
volatile
set search_path to ''
as $function$
    with prepared as materialized (
        select distinct
            md5(jsonb_build_array(p_queue_name, subscription)::text)::uuid as id,
            subscription.*
        from unnest(p_subscriptions) subscription
    ), removed as (
        delete from pgconductor._private_custom_event_subscriptions
        where queue = p_queue_name
          and kind = 'task_trigger'
          and id not in (select id from prepared)
    ), inserted_subscriptions as (
        insert into pgconductor._private_custom_event_subscriptions (
            id, event_key, task_key, queue, payload_fields,
            required_field_count, kind
        )
        select id, event_key, task_key, p_queue_name,
            payload_fields, required_field_count, 'task_trigger'
        from prepared
        on conflict (id) do nothing
        returning id
    )
    insert into pgconductor._private_event_filter_terms (
        subscription_id, term_number, event_key, field_name, operator,
        scalar_type, text_value, number_value, boolean_value, number_range
    )
    select terms.*
    from prepared
    join inserted_subscriptions on inserted_subscriptions.id = prepared.id
    cross join lateral pgconductor._private_expand_event_filter_terms(
        prepared.id,
        prepared.event_key,
        prepared.terms
    ) terms;
$function$;

insert into pgconductor._private_queues (name)
values ('pgconductor.internal')
on conflict do nothing;

insert into pgconductor._private_tasks (
    key, queue, max_attempts, remove_on_complete_days, remove_on_fail_days
)
values (
    'pgconductor.event-dispatch', 'pgconductor.internal', 3, 1, null
)
on conflict (queue, key) do update set
    max_attempts = excluded.max_attempts,
    remove_on_complete_days = excluded.remove_on_complete_days,
    remove_on_fail_days = excluded.remove_on_fail_days;

create or replace function pgconductor.emit_event(
    p_event_key text,
    p_payload jsonb default '{}'::jsonb,
    p_trace_context jsonb default null,
    p_metadata jsonb default null,
    p_id uuid default pgconductor._private_portable_uuidv7(),
    p_dedupe_key text default null
)
returns uuid
language plpgsql
volatile
set search_path to ''
as $function$
declare
    v_payload jsonb := p_payload;
    -- json array text, so no two (name, key) pairs encode alike
    v_dedupe_key text := case
        when p_dedupe_key is not null then jsonb_build_array(p_event_key, p_dedupe_key)::text
    end;
begin
    if p_event_key is null
        or btrim(p_event_key) = ''
        or octet_length(p_event_key) > 255
    then
        raise exception 'Event name must contain between 1 and 255 UTF-8 bytes';
    end if;

    if v_payload is null or jsonb_typeof(v_payload) <> 'object' then
        raise exception 'Event payload must be a JSON object';
    end if;

    perform pgconductor._private_check_metadata(p_metadata);

    insert into pgconductor._private_executions (
        id, task_key, queue, payload, trace_context, metadata, dedupe_key
    ) values (
        p_id,
        'pgconductor.event-dispatch',
        'pgconductor.internal',
        jsonb_build_object('eventKey', p_event_key, 'payload', v_payload),
        p_trace_context,
        p_metadata,
        v_dedupe_key
    )
    on conflict do nothing;

    if found or p_dedupe_key is null then
        return p_id;
    end if;

    -- a duplicate inserts nothing, and do update would write a row version per redelivery; a new
    -- statement sees a concurrently committed winner that a cte sharing the insert's snapshot misses
    return (
        select id
        from pgconductor._private_executions
        where task_key = 'pgconductor.event-dispatch'
          and queue = 'pgconductor.internal'
          and dedupe_key = v_dedupe_key
          and singleton_on is null
    );
end;
$function$;

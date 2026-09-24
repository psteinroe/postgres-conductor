
create extension if not exists btree_gist;

-- Returns either the actual current timestamp or a fake one for tests.
-- Uses session variable (current_setting) for test time control.
create function pgconductor._private_current_time ()
  returns timestamptz
  language plpgsql
  volatile
as $$
declare
  v_fake text;
begin
  v_fake := current_setting('pgconductor.fake_now', true);
  if v_fake is not null and length(trim(v_fake)) > 0 then
    return v_fake::timestamptz;
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
    migration_number integer
);

create index idx_orchestrators_heartbeat on pgconductor._private_orchestrators (last_heartbeat_at);
create index idx_orchestrators_sweep on pgconductor._private_orchestrators (migration_number);

create table pgconductor._private_orchestrator_signals (
    id uuid primary key default pgconductor._private_portable_uuidv7(),
    orchestrator_id uuid not null references pgconductor._private_orchestrators(id) on delete cascade,
    type text not null,
    execution_id uuid,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default pgconductor._private_current_time()
);

create index idx_orchestrator_signals_orchestrator on pgconductor._private_orchestrator_signals(orchestrator_id, created_at);

create unique index idx_orchestrator_signals_cancel_unique
    on pgconductor._private_orchestrator_signals(orchestrator_id, execution_id)
    where type = 'cancel_execution' and execution_id is not null;

create unique index idx_orchestrator_signals_shutdown_unique
    on pgconductor._private_orchestrator_signals(orchestrator_id)
    where type = 'shutdown';

create table pgconductor._private_queues (
    name text primary key
);

create table pgconductor._private_executions (
    id uuid default pgconductor._private_portable_uuidv7(),
    task_key text not null,
    queue text not null default 'default',
    dedupe_key text,
    cron_expression text,
    created_at timestamptz default pgconductor._private_current_time() not null,
    failed_at timestamptz,
    completed_at timestamptz,
    payload jsonb,
    run_at timestamptz default pgconductor._private_current_time() not null,
    locked_at timestamptz,
    locked_by uuid,
    "group" text,
    is_available boolean generated always as (locked_at is null and failed_at is null and completed_at is null) stored not null,
    attempts integer default 0 not null,
    last_error text,
    cancelled boolean default false not null,
    priority integer default 0 not null,
    waiting_on_execution_id uuid,
    waiting_step_key text,
    parent_execution_id uuid,
    subscription_id uuid,
    singleton_on timestamptz,

    -- Dead-letter metadata is denormalized so retained source rows are optional.
    dead_letter_source_execution_id uuid,
    dead_letter_source_queue text,
    dead_letter_source_task_key text,
    dead_letter_error text,
    dead_letter_attempts integer,
    dead_letter_failed_at timestamptz,
    primary key (id, queue),
    unique (task_key, dedupe_key, queue),
    constraint chk_executions_event_delivery_parent
        check (subscription_id is null or parent_execution_id is not null)
) partition by list (queue);

-- unique index for singleton throttle/debounce enforcement on parent table
-- this automatically creates matching indexes on all partitions
-- must include partition key (queue) per PostgreSQL requirement
-- use coalesce to treat NULL dedupe_key as empty string for singleton matching
create unique index on pgconductor._private_executions (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false;

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
    id uuid default pgconductor._private_portable_uuidv7() primary key,
    key text not null,
    execution_id uuid not null,
    queue text not null,
    result jsonb,
    created_at timestamptz default pgconductor._private_current_time() not null,
    unique (key, execution_id),
    constraint fk_execution foreign key (execution_id, queue) references pgconductor._private_executions(id, queue) on delete cascade
);

create index idx_steps_execution_id on pgconductor._private_steps (execution_id);

create unique index idx_executions_dead_letter_delivery
    on pgconductor._private_executions (dead_letter_source_execution_id, queue, task_key)
    where dead_letter_source_execution_id is not null;

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
  if tg_op = 'INSERT' then
    v_partition_name := 'executions_' || replace(new.name, '-', '_');

    -- Create partition for this queue: executions_default, executions_reports, etc.
    execute format(
      'create table if not exists pgconductor.%I partition of pgconductor._private_executions for values in (%L) with (fillfactor=70)',
      v_partition_name,
      new.name
    );

    -- create indices

    -- main index for fetching available executions
    execute format(
      'create index if not exists %I on pgconductor.%I (priority, run_at, created_at, id) include (task_key) where is_available = true',
      'idx_' || v_partition_name || '_get_executions',
      v_partition_name
    );

    -- index for waiting executions lookup
    execute format(
      'create index if not exists %I on pgconductor.%I (waiting_on_execution_id) where waiting_on_execution_id is not null',
      'idx_' || v_partition_name || '_waiting_on_execution_id',
      v_partition_name
    );

    -- index for parent execution lookup (child -> parent)
    execute format(
      'create index if not exists %I on pgconductor.%I (parent_execution_id) where parent_execution_id is not null',
      'idx_' || v_partition_name || '_parent_execution_id',
      v_partition_name
    );

    -- index for unlocking locked executions
    execute format(
      'create index if not exists %I on pgconductor.%I (locked_by) where locked_by is not null',
      'idx_' || v_partition_name || '_locked_by',
      v_partition_name
    );

    -- index for cleanup of completed
    execute format(
      'create index if not exists %I on pgconductor.%I (completed_at) where completed_at is not null',
      'idx_' || v_partition_name || '_completed_cleanup',
      v_partition_name
    );

    -- index for cleanup of failed
    execute format(
      'create index if not exists %I on pgconductor.%I (failed_at) where failed_at is not null',
      'idx_' || v_partition_name || '_failed_cleanup',
      v_partition_name
    );

    -- index for dynamic schedule lookups
    execute format(
      'create index if not exists %I on pgconductor.%I ((split_part(dedupe_key, ''::'', 2))) where dedupe_key like ''dynamic::%%'' and cron_expression is not null',
      'idx_' || v_partition_name || '_dynamic_schedule',
      v_partition_name
    );

    -- index for task_key joins (used in return_executions)
    execute format(
      'create index if not exists %I on pgconductor.%I (task_key)',
      'idx_' || v_partition_name || '_task_key',
      v_partition_name
    );

    -- covering index used to count active executions for soft task and group limits
    execute format(
      'create index if not exists %I on pgconductor.%I (task_key, "group") where locked_at is not null and failed_at is null and completed_at is null',
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

    v_partition_name := 'executions_' || replace(old.name, '-', '_');

    -- drop the partition for this queue
    execute format(
      'drop table if exists pgconductor.%I',
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

-- create default queue (trigger will create executions_default partition)
insert into pgconductor._private_queues (name) values ('default');

-- drop a queue (will trigger partition deletion via trigger)
create or replace function pgconductor.drop_queue(queue_name text)
 returns void
 language sql
 volatile
 set search_path to ''
as $function$
  delete from pgconductor._private_queues where name = drop_queue.queue_name;
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
    "group" text
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
    queue = coalesce(excluded.queue, pgconductor._private_tasks.queue),
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
  insert into pgconductor._private_executions (task_key, queue, payload, run_at, dedupe_key, cron_expression, "group")
  select
    spec.task_key,
    coalesce(spec.queue, 'default'),
    coalesce(spec.payload, '{}'::jsonb),
    coalesce(spec.run_at, pgconductor._private_current_time()),
    spec.dedupe_key,
    spec.cron_expression,
    spec."group"
  from unnest(p_cron_schedules) as spec
  where spec.dedupe_key is not null
  on conflict (task_key, dedupe_key, queue) do update set
    payload = excluded.payload,
    run_at = excluded.run_at,
    cron_expression = excluded.cron_expression,
    "group" = excluded."group";

  -- Clean up stale schedules for this queue.
  delete from pgconductor._private_executions
  where queue = p_queue_name
    and cron_expression is not null
    and run_at > pgconductor._private_current_time()
    and dedupe_key like 'scheduled::%'
    and split_part(dedupe_key, '::', 2) not in (
      select split_part(spec.dedupe_key, '::', 2)
      from unnest(p_cron_schedules) as spec
      where spec.dedupe_key is not null and spec.dedupe_key like 'scheduled::%'
    );

  update pgconductor._private_executions
  set cancelled = true
  where queue = p_queue_name
    and cron_expression is not null
    and dedupe_key like 'scheduled::%'
    and locked_by is not null
    and completed_at is null
    and failed_at is null
    and cancelled = false
    and split_part(dedupe_key, '::', 2) not in (
      select split_part(spec.dedupe_key, '::', 2)
      from unnest(p_cron_schedules) as spec
      where spec.dedupe_key is not null and spec.dedupe_key like 'scheduled::%'
    );

  perform pgconductor._private_replace_custom_event_subscriptions(
    p_queue_name,
    p_event_subscriptions
  );
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
    v_now timestamptz;
begin
    v_now := pgconductor._private_current_time();

    -- clear locked dedupe keys before batch insert
    with superseded as (
        select e.id, e.queue, e.task_key
        from pgconductor._private_executions as e
        cross join unnest(specs) as spec
        where e.dedupe_key = spec.dedupe_key
            and e.task_key = spec.task_key
            and e.queue = coalesce(spec.queue, 'default')
            and e.locked_at is not null
            and spec.dedupe_key is not null
        for update of e
    )
    update pgconductor._private_executions e
    set
        dedupe_key = null,
        locked_by = null,
        locked_at = null,
        failed_at = v_now,
        last_error = 'superseded by reinvoke'
    from superseded s
    where e.id = s.id;

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
        "group"
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
        spec."group"
    from unnest(specs) as spec
    on conflict (task_key, dedupe_key, queue) do update set
        payload = excluded.payload,
        run_at = excluded.run_at,
        priority = excluded.priority,
        cron_expression = excluded.cron_expression,
        singleton_on = excluded.singleton_on,
        "group" = excluded."group"
    returning pgconductor._private_executions.id;
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
    p_group text default null
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
      with superseded as (
          select e.id, e.queue, e.task_key
          from pgconductor._private_executions e
          where e.dedupe_key = p_dedupe_key
              and e.task_key = p_task_key
              and e.queue = p_queue
              and e.locked_at is not null
          for update of e
      )
      update pgconductor._private_executions e
      set
          dedupe_key = null,
          locked_by = null,
          locked_at = null,
          failed_at = v_now,
          last_error = 'superseded by reinvoke'
        from superseded s
      where e.id = s.id;
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
              "group"
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
    v_run_at,
    p_dedupe_key,
    p_cron_expression,
    coalesce(p_priority, 0),
    p_group
  )
  on conflict (task_key, dedupe_key, queue) do update set
    payload = excluded.payload,
    run_at = excluded.run_at,
    priority = excluded.priority,
    cron_expression = excluded.cron_expression,
    "group" = excluded."group"
  returning e.id;
end;
$function$
;

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
  v_child_id uuid;
  v_child_orchestrator_id uuid;
  v_child_queue text;
  v_completed boolean;
  v_failed boolean;
  v_rows_affected integer;
begin
  select
    locked_by,
    queue,
    waiting_on_execution_id,
    completed_at is not null,
    failed_at is not null
  into v_orchestrator_id, v_queue, v_child_id, v_completed, v_failed
  from pgconductor._private_executions
  where id = p_execution_id
  for update;

  if not found or v_completed or v_failed then
    return false;
  end if;

  if v_orchestrator_id is null then
    -- pending: fail immediately. If this is a waiting parent, resolve its
    -- child relationship in the same transaction so the child cannot become
    -- orphaned or leave the workflow stranded.
    if v_child_id is not null then
      select locked_by, queue
      into v_child_orchestrator_id, v_child_queue
      from pgconductor._private_executions
      where id = v_child_id
      for update;

      if found and v_child_orchestrator_id is null then
        update pgconductor._private_executions
        set
          failed_at = pgconductor._private_current_time(),
          last_error = 'Cancelled: parent execution was cancelled',
          locked_by = null,
          locked_at = null,
          waiting_on_execution_id = null,
          waiting_step_key = null
        where id = v_child_id
          and completed_at is null
          and failed_at is null;
      elsif found then
        update pgconductor._private_executions
        set cancelled = true, last_error = p_reason
        where id = v_child_id
          and completed_at is null
          and failed_at is null
          and cancelled = false;

        get diagnostics v_rows_affected = row_count;
        if v_rows_affected > 0 then
          insert into pgconductor._private_orchestrator_signals
            (orchestrator_id, type, execution_id, payload)
          values (
            v_child_orchestrator_id,
            'cancel_execution',
            v_child_id,
            jsonb_build_object('queue', v_child_queue, 'reason', p_reason)
          )
          on conflict (orchestrator_id, execution_id)
            where type = 'cancel_execution' and execution_id is not null
          do nothing;
        end if;
      end if;

      delete from pgconductor._private_custom_event_subscriptions
      where kind = 'execution_wait'
        and execution_id = v_child_id;
    end if;

    update pgconductor._private_executions
    set
      failed_at = pgconductor._private_current_time(),
      last_error = p_reason,
      locked_by = null,
      locked_at = null,
      waiting_on_execution_id = null,
      waiting_step_key = null
    where id = p_execution_id
      and completed_at is null
      and failed_at is null
      and locked_by is null
      and locked_at is null;

    get diagnostics v_rows_affected = row_count;

    if v_rows_affected > 0 then
      delete from pgconductor._private_custom_event_subscriptions
      where kind = 'execution_wait'
        and execution_id = p_execution_id;
    end if;

    return v_rows_affected > 0;
  else
    -- running: signal orchestrator + set cancelled flag
    update pgconductor._private_executions
    set
      cancelled = true,
      last_error = p_reason
    where id = p_execution_id
      and queue = v_queue
      and locked_by = v_orchestrator_id
      and completed_at is null
      and cancelled = false;

    get diagnostics v_rows_affected = row_count;

    if v_rows_affected > 0 then
      delete from pgconductor._private_custom_event_subscriptions
      where kind = 'execution_wait'
        and execution_id = p_execution_id;

      insert into pgconductor._private_orchestrator_signals
        (orchestrator_id, type, execution_id, payload)
      values (
        v_orchestrator_id,
        'cancel_execution',
        p_execution_id,
        jsonb_build_object('queue', v_queue, 'reason', p_reason)
      )
      on conflict (orchestrator_id, execution_id)
        where type = 'cancel_execution' and execution_id is not null
      do nothing;

      return true;
    else
      return false;
    end if;
  end if;
end;
$function$;

-- Event deliveries use the source dispatch execution as durable lineage without
-- participating in workflow parent/child settlement.
create unique index idx_executions_event_destination
    on pgconductor._private_executions (parent_execution_id, subscription_id, queue)
    where subscription_id is not null;

create table pgconductor._private_custom_event_subscriptions (
    id uuid primary key default pgconductor._private_portable_uuidv7(),
    event_key text not null,
    task_key text not null,
    queue text not null,
    payload_fields text[],
    required_field_count smallint not null check (required_field_count between 0 and 8),
    created_at timestamptz not null default pgconductor._private_current_time(),
    kind text not null default 'task_trigger',
    execution_id uuid,
    step_key text,
    expires_at timestamptz,
    constraint chk_custom_event_subscription_event_key check (
        btrim(event_key) <> '' and octet_length(event_key) between 1 and 255
    ),
    constraint chk_custom_event_subscription_kind check (
        kind in ('task_trigger', 'execution_wait')
    ),
    constraint chk_custom_event_subscription_shape check (
        (kind = 'task_trigger'
            and execution_id is null
            and step_key is null
            and expires_at is null)
        or
        (kind = 'execution_wait'
            and execution_id is not null
            and step_key is not null
            and payload_fields is null)
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
    on pgconductor._private_custom_event_subscriptions (execution_id, step_key)
    where kind = 'execution_wait';

create index idx_custom_event_subscription_wait_match
    on pgconductor._private_custom_event_subscriptions
       (event_key, created_at, expires_at, id)
    where kind = 'execution_wait';

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

create or replace function pgconductor._private_register_event_wait(
    p_execution_id uuid,
    p_queue text,
    p_task_key text,
    p_orchestrator_id uuid,
    p_event_key text,
    p_step_key text,
    p_required_field_count smallint,
    p_terms jsonb,
    p_timeout_ms bigint
)
returns table (timed_out boolean, timeout_ms bigint)
language plpgsql
volatile
set search_path to ''
as $function$
declare
    v_subscription_id uuid;
    v_expires_at timestamptz;
    v_now timestamptz;
begin
    perform 1
    from pgconductor._private_executions execution
    where execution.id = p_execution_id
      and execution.queue = p_queue
      and execution.task_key = p_task_key
      and execution.locked_by = p_orchestrator_id
      and execution.completed_at is null
      and execution.failed_at is null
      and not execution.cancelled
    for update;

    if not found then
        return query select false, null::bigint;
        return;
    end if;

    v_now := pgconductor._private_current_time();

    select subscription.id, subscription.expires_at
    into v_subscription_id, v_expires_at
    from pgconductor._private_custom_event_subscriptions subscription
    where subscription.kind = 'execution_wait'
      and subscription.execution_id = p_execution_id
      and subscription.queue = p_queue
      and subscription.step_key = p_step_key
    for update;

    if found then
        if v_expires_at is not null and v_expires_at <= v_now then
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

            return query select true, null::bigint;
            return;
        end if;

        return query select false, case
            when v_expires_at is null then null::bigint
            else greatest(0, ceil(extract(epoch from (
                v_expires_at - pgconductor._private_current_time()
            )) * 1000)::bigint)
        end;
        return;
    end if;

    v_expires_at := case
        when p_timeout_ms is null then null
        else v_now + (p_timeout_ms || ' milliseconds')::interval
    end;

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

    return query select false, case
        when v_expires_at is null then null::bigint
        else greatest(0, ceil(extract(epoch from (
            v_expires_at - pgconductor._private_current_time()
        )) * 1000)::bigint)
    end;
end;
$function$;

-- Worker registration replaces only durable task-trigger subscriptions. Active
-- execution waits belong to running workflows and survive worker restarts.
create or replace function pgconductor._private_replace_custom_event_subscriptions(
    p_queue_name text,
    p_subscriptions pgconductor.event_subscription_spec[]
)
returns void
language sql
volatile
set search_path to ''
as $function$
    with removed as materialized (
        delete from pgconductor._private_custom_event_subscriptions
        where queue = p_queue_name and kind = 'task_trigger'
        returning 1
    ), prepared as materialized (
        select pgconductor._private_portable_uuidv7() as id,
            subscription.task_key,
            subscription.event_key,
            subscription.payload_fields,
            coalesce(subscription.required_field_count, 0) as required_field_count,
            coalesce(subscription.terms, '[]'::jsonb) as terms,
            subscription.input_ordinal
        from unnest(p_subscriptions) with ordinality
          as subscription(
              task_key, event_key, payload_fields,
              required_field_count, terms, input_ordinal
          )
        cross join (select count(*) from removed) removal_barrier
    ), inserted_subscriptions as (
        insert into pgconductor._private_custom_event_subscriptions (
            id, event_key, task_key, queue, payload_fields,
            required_field_count, kind
        )
        select id, event_key, task_key, p_queue_name,
            payload_fields, required_field_count, 'task_trigger'
        from prepared
        order by input_ordinal
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
    'pgconductor.event-dispatch', 'pgconductor.internal', 3, 0, null
)
on conflict (queue, key) do update set
    max_attempts = excluded.max_attempts,
    remove_on_complete_days = excluded.remove_on_complete_days,
    remove_on_fail_days = excluded.remove_on_fail_days;

create or replace function pgconductor.emit_event(
    p_event_key text,
    p_payload jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
volatile
set search_path to ''
as $function$
declare
    v_payload jsonb := p_payload;
    v_event_id uuid;
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

    insert into pgconductor._private_executions (
        id, task_key, queue, payload
    ) values (
        pgconductor._private_portable_uuidv7(),
        'pgconductor.event-dispatch',
        'pgconductor.internal',
        jsonb_build_object('eventKey', p_event_key, 'payload', v_payload)
    )
    returning id into v_event_id;

    return v_event_id;
end;
$function$;

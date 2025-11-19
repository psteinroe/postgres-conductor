-- todo:
-- - throttling (limit, period, key),
-- - concurrency (limit, key),
-- - rateLimit (limit, period, key),
-- - debounce (period, key) -> via invoke
-- throttling, concurrency and rateLimit: key and seconds - fetch and group by - USE SLOTS similar to https://planetscale.com/blog/the-slotted-counter-pattern
-- batch processing via array payloads?

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Returns either the actual current timestamp or a fake one for tests.
-- Uses session variable (current_setting) for test time control.
create function pgconductor.current_time ()
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
create function pgconductor.portable_uuidv7 ()
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
  ts_ms := floor(extract(epoch from pgconductor.current_time()) * 1000)::bigint;
  rnd := uuid_send(public.uuid_generate_v4 ());
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

CREATE TABLE pgconductor.orchestrators (
    id uuid default pgconductor.portable_uuidv7() primary key,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version text,
    migration_number integer,
    shutdown_signal boolean default false not null
);

CREATE TABLE pgconductor.queues (
    name text primary key
);

CREATE TABLE pgconductor.executions (
    id uuid default pgconductor.portable_uuidv7(),
    task_key text not null,
    queue text not null default 'default',
    dedupe_key text,
    cron_expression text,
    created_at timestamptz default pgconductor.current_time() not null,
    failed_at timestamptz,
    completed_at timestamptz,
    payload jsonb,
    run_at timestamptz default pgconductor.current_time() not null,
    locked_at timestamptz,
    locked_by uuid,
    attempts integer default 0 not null,
    last_error text,
    priority integer default 0 not null,
    waiting_on_execution_id uuid,
    waiting_step_key text,
    primary key (id, queue),
    unique (task_key, dedupe_key, queue)
) PARTITION BY LIST (queue);

CREATE TABLE pgconductor.tasks (
    key text primary key,

    -- queue that this task belongs to (used for queue-based worker assignment)
    queue text default 'default' not null,

    -- retry settings - uses fixed Inngest-style backoff schedule
    max_attempts integer default 3 not null,

    -- task can be executed only within certain time windows
    -- e.g. business hours, weekends, nights, ...
    -- we will stop the execution of executions outside of these time windows at step boundaries
    window_start timetz,
    window_end timetz,
    CONSTRAINT "windows" CHECK (
        (window_start IS NULL AND window_end IS NULL) OR
        (
            window_start IS NOT NULL AND
            window_end IS NOT NULL AND
            window_start != window_end
        )
    )
);

CREATE TABLE pgconductor.steps (
    id uuid default pgconductor.portable_uuidv7() primary key,
    key text not null,
    execution_id uuid not null,
    queue text not null,
    result jsonb,
    created_at timestamptz default pgconductor.current_time() not null,
    unique (key, execution_id),
    CONSTRAINT fk_execution FOREIGN KEY (execution_id, queue) REFERENCES pgconductor.executions(id, queue) ON DELETE CASCADE
);

-- Trigger function to manage executions partitions per queue
-- Automatically creates partition when queue is inserted
CREATE OR REPLACE FUNCTION pgconductor.manage_queue_partition()
 RETURNS trigger
 LANGUAGE plpgsql
 VOLATILE
 SET search_path TO ''
AS $function$
DECLARE
  v_partition_name text;
BEGIN
  IF TG_OP = 'INSERT' THEN
    v_partition_name := 'executions_' || replace(NEW.name, '-', '_');

    -- Create partition for this queue: executions_default, executions_reports, etc.
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS pgconductor.%I PARTITION OF pgconductor.executions FOR VALUES IN (%L) WITH (fillfactor=70)',
      v_partition_name,
      NEW.name
    );

    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    -- Protect default queue from modification
    IF OLD.name = 'default' OR NEW.name = 'default' THEN
      RAISE EXCEPTION 'Modifying the default queue is not allowed';
    END IF;

    -- Disallow renaming queues
    IF NEW.name != OLD.name THEN
      RAISE EXCEPTION 'Renaming queues is not allowed. Queue name cannot be changed from % to %', OLD.name, NEW.name;
    END IF;

    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    -- Protect default queue from deletion
    IF OLD.name = 'default' THEN
      RAISE EXCEPTION 'Deleting the default queue is not allowed';
    END IF;

    v_partition_name := 'executions_' || replace(OLD.name, '-', '_');

    -- Drop the partition for this queue
    EXECUTE format(
      'DROP TABLE IF EXISTS pgconductor.%I',
      v_partition_name
    );

    RETURN OLD;
  END IF;
END;
$function$;

-- Attach trigger to queues table
CREATE TRIGGER manage_queue_partition_trigger
  AFTER INSERT OR UPDATE OR DELETE ON pgconductor.queues
  FOR EACH ROW
  EXECUTE FUNCTION pgconductor.manage_queue_partition();

-- Create default queue (trigger will create executions_default partition)
INSERT INTO pgconductor.queues (name) VALUES ('default');

-- Drop a queue (will trigger partition deletion via trigger)
CREATE OR REPLACE FUNCTION pgconductor.drop_queue(queue_name text)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
  DELETE FROM pgconductor.queues WHERE name = drop_queue.queue_name;
$function$;

create type pgconductor.execution_spec as (
    task_key text,
    queue text,
    payload jsonb,
    run_at timestamptz,
    dedupe_key text,
    cron_expression text,
    priority integer
);

create type pgconductor.task_spec as (
    key text,
    queue text,
    max_attempts integer,
    window_start timetz,
    window_end timetz
);

CREATE OR REPLACE FUNCTION pgconductor.register_worker(
    p_queue_name text,
    p_task_specs pgconductor.task_spec[],
    p_cron_schedules pgconductor.execution_spec[]
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SET search_path TO ''
AS $function$
BEGIN
  -- Step 1: Upsert queue (triggers partition creation)
  INSERT INTO pgconductor.queues (name)
  VALUES (p_queue_name)
  ON CONFLICT (name) DO NOTHING;

  -- Step 2: Register/update tasks
  INSERT INTO pgconductor.tasks (key, queue, max_attempts, window_start, window_end)
  SELECT
    spec.key,
    COALESCE(spec.queue, 'default'),
    COALESCE(spec.max_attempts, 3),
    spec.window_start,
    spec.window_end
  FROM unnest(p_task_specs) AS spec
  ON CONFLICT (key)
  DO UPDATE SET
    queue = COALESCE(EXCLUDED.queue, pgconductor.tasks.queue),
    max_attempts = COALESCE(EXCLUDED.max_attempts, pgconductor.tasks.max_attempts),
    window_start = EXCLUDED.window_start,
    window_end = EXCLUDED.window_end;

  -- Step 3: Insert scheduled cron executions (ON CONFLICT DO NOTHING)
  INSERT INTO pgconductor.executions (task_key, queue, payload, run_at, dedupe_key, cron_expression)
  SELECT
    spec.task_key,
    COALESCE(spec.queue, 'default'),
    COALESCE(spec.payload, '{}'::jsonb),
    COALESCE(spec.run_at, pgconductor.current_time()),
    spec.dedupe_key,
    spec.cron_expression
  FROM unnest(p_cron_schedules) AS spec
  WHERE spec.dedupe_key IS NOT NULL
  ON CONFLICT (task_key, dedupe_key, queue) DO NOTHING;

  -- Step 4: Clean up stale schedules for this queue
  DELETE FROM pgconductor.executions
  WHERE queue = p_queue_name
    AND cron_expression IS NOT NULL
    AND run_at > pgconductor.current_time()
    AND (task_key, cron_expression) NOT IN (
      SELECT spec.task_key, spec.cron_expression
      FROM unnest(p_cron_schedules) AS spec
      WHERE spec.cron_expression IS NOT NULL
    );
END;
$function$;

CREATE OR REPLACE FUNCTION pgconductor.invoke_batch(
    specs pgconductor.execution_spec[]
)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 VOLATILE
 SET search_path TO ''
AS $function$
begin
    -- Clear locked dedupe keys before batch insert
    update pgconductor.executions as e
    set
        dedupe_key = null,
        locked_by = null,
        locked_at = null,
        failed_at = pgconductor.current_time(),
        last_error = 'superseded by reinvoke'
    from unnest(specs) as spec
    where e.dedupe_key = spec.dedupe_key
        and e.task_key = spec.task_key
        and e.queue = coalesce(spec.queue, 'default')
        and e.locked_at is not null
        and spec.dedupe_key is not null;

    -- Batch insert all executions
    return query
    insert into pgconductor.executions (
        id,
        task_key,
        queue,
        payload,
        run_at,
        dedupe_key,
        cron_expression,
        priority
    )
    select
        pgconductor.portable_uuidv7(),
        spec.task_key,
        coalesce(spec.queue, 'default'),
        spec.payload,
        coalesce(spec.run_at, pgconductor.current_time()),
        spec.dedupe_key,
        spec.cron_expression,
        coalesce(spec.priority, 0)
    from unnest(specs) as spec
    returning id;
end;
$function$
;

CREATE OR REPLACE FUNCTION pgconductor.invoke_child(
    task_key text,
    queue text default 'default',
    payload jsonb default null,
    run_at timestamptz default null,
    dedupe_key text default null,
    cron_expression text default null,
    priority integer default null,
    parent_execution_id uuid default null,
    parent_step_key text default null,
    parent_timeout_ms integer default null
)
 RETURNS uuid
 LANGUAGE plpgsql
 VOLATILE
 SET search_path TO ''
AS $function$
declare
    v_execution_id uuid;
begin
    select id from pgconductor.invoke(
        invoke_child.task_key,
        invoke_child.queue,
        invoke_child.payload,
        invoke_child.run_at,
        invoke_child.dedupe_key,
        invoke_child.cron_expression,
        invoke_child.priority
    ) into v_execution_id;

    update pgconductor.executions
    set
        waiting_on_execution_id = v_execution_id,
        waiting_step_key = invoke_child.parent_step_key,
        run_at = case
            when invoke_child.parent_timeout_ms is not null then
                pgconductor.current_time() + (invoke_child.parent_timeout_ms || ' milliseconds')::interval
            else
                'infinity'::timestamptz
        end
    where id = invoke_child.parent_execution_id
      and invoke_child.parent_execution_id is not null;

    return v_execution_id;
end;
$function$
;


CREATE OR REPLACE FUNCTION pgconductor.invoke(
    task_key text,
    queue text default 'default',
    payload jsonb default null,
    run_at timestamptz default null,
    dedupe_key text default null,
    cron_expression text default null,
    priority integer default null
)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 VOLATILE
 SET search_path TO ''
AS $function$
begin
  if invoke.dedupe_key is not null then
      -- Clear keys that are currently locked so a subsequent insert can succeed.
      update pgconductor.executions as e
      set
        dedupe_key = null,
        locked_by = null,
        locked_at = null,
        failed_at = pgconductor.current_time(),
        last_error = 'superseded by reinvoke'
      where e.dedupe_key = invoke.dedupe_key
        and e.task_key = invoke.task_key
        and e.queue = invoke.queue
        and e.locked_at is not null;
  end if;

  return query insert into pgconductor.executions as e (
    id,
    task_key,
    queue,
    payload,
    run_at,
    dedupe_key,
    cron_expression,
    priority
  ) values (
    pgconductor.portable_uuidv7(),
    invoke.task_key,
    invoke.queue,
    invoke.payload,
    coalesce(invoke.run_at, pgconductor.current_time()),
    invoke.dedupe_key,
    invoke.cron_expression,
    coalesce(invoke.priority, 0)
  ) returning e.id;
end;
$function$
;

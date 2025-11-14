-- todo:
-- - throttling (limit, period, key),
-- - concurrency (limit, key),
-- - rateLimit (limit, period, key),
-- - debounce (period, key) -> via invoke
-- throttling, concurrency and rateLimit: key and seconds - fetch and group by - USE SLOTS similar to https://planetscale.com/blog/the-slotted-counter-pattern
-- maybe: cel as a postgres extension -> no, rust binary with cdc
-- batch processing via array payloads?
-- check what extensions are available on install and store it on a settings table, e.g. pg_jsonschema, pg_partman, ...
-- we need to fetch workflow config on fetch executions and add executions anyways...


-- assert required extensions are installed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Test configuration table for controlling behavior in tests
CREATE TABLE IF NOT EXISTS pgconductor.test_config (
  key text PRIMARY KEY,
  value text
);

-- Returns either the actual current timestamp or a fake one for tests.
-- Checks test_config table first, then session variable, then real time.
create function pgconductor.current_time ()
  returns timestamptz
  language plpgsql
  volatile
as $$
declare
  v_fake text;
begin
  -- Check test_config table first (works across all connections)
  BEGIN
    SELECT value INTO v_fake FROM pgconductor.test_config WHERE key = 'fake_now';
    IF v_fake IS NOT NULL AND length(trim(v_fake)) > 0 THEN
      RETURN v_fake::timestamptz;
    END IF;
  EXCEPTION
    WHEN undefined_table THEN
      NULL; -- Table doesn't exist yet during initial migration
  END;

  -- Fall back to session variable (for backwards compatibility)
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

CREATE TABLE pgconductor.settings (
    key text primary key,
    value jsonb not null
);

-- NOTES FOR LIVE MIGRATIONS:
-- todo: how to make sure we handle sudden worker crashes? we could leverage steps to also update locked_at periodically
-- or have a separate heartbeat mechanism per execution
-- and if a execution has been locked for too long, we can assume the worker crashed and make it available again
-- DANGER: this could lead to multiple orchestrators working on the same execution if the heartbeat interval is too long
-- separate heartbeat mechanism is safer than leveraging steps because steps might not be executed frequently enough and we do
-- not want this burden on the user
-- we could even just put the heartbeat on the orchestrators table and have a background process that checks for stale locks
-- this should be fine too and it means we do not have to join the orchestrators table when fetching executions
-- just refresh locked_at?
-- todo: stale worker cleanup function that removes orchestrators and unlocks their executions if last_heartbeat_at is too old

-- add breaking marker in migrations
CREATE TABLE pgconductor.orchestrators (
    id uuid default pgconductor.portable_uuidv7() primary key,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version text,
    migration_number integer,
    shutdown_signal boolean default false not null
);

CREATE TABLE pgconductor.executions (
    id uuid default pgconductor.portable_uuidv7(),
    task_key text not null,
    key text,
    created_at timestamptz default pgconductor.current_time() not null,
    failed_at timestamptz,
    completed_at timestamptz,
    payload jsonb default '{}'::jsonb not null,
    run_at timestamptz default pgconductor.current_time() not null,
    locked_at timestamptz,
    locked_by uuid,
    attempts integer default 0 not null,
    last_error text,
    priority integer default 0 not null,
    waiting_on_execution_id uuid,
    waiting_step_key text,
    primary key (id, task_key)
) PARTITION BY LIST (task_key);

CREATE TABLE pgconductor.executions_default PARTITION OF pgconductor.executions DEFAULT with (fillfactor=70);

CREATE TABLE pgconductor.failed_executions (
    LIKE pgconductor.executions INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS EXCLUDING INDEXES,
    PRIMARY KEY (failed_at, id)
) PARTITION BY RANGE (failed_at);

CREATE TABLE pgconductor.completed_executions (
    LIKE pgconductor.executions INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS EXCLUDING INDEXES,
    PRIMARY KEY (completed_at, id)
) PARTITION BY RANGE (completed_at);

-- todo: this will be managed by pg_partman or a cron job in the future
CREATE TABLE pgconductor.failed_executions_default PARTITION OF pgconductor.failed_executions
    FOR VALUES FROM (MINVALUE) TO (MAXVALUE);

-- todo: this will be managed by pg_partman or a cron job in the future
CREATE TABLE pgconductor.completed_executions_default PARTITION OF pgconductor.completed_executions
    FOR VALUES FROM (MINVALUE) TO (MAXVALUE);

CREATE TABLE pgconductor.tasks (
    key text primary key,
    -- we could validate execution payloads on insert if pg jsonchema extension is available
    input_schema jsonb default '{}'::jsonb not null,

    -- retry settings - uses fixed Inngest-style backoff schedule
    max_attempts integer default 3 not null,

    partition boolean default false not null,

    -- task can be executed only within certain time windows
    -- e.g. business hours, weekends, nights, ...
    -- we will stop the execution of executions outside of these time windows at step boundaries
    -- TODO: we should adjust run_at based on these windows when inserting / rescheduling executions
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
    execution_id uuid,
    result jsonb,
    created_at timestamptz default pgconductor.current_time() not null,
    unique (key, execution_id)
);

-- Load a step result by execution_id and key
CREATE OR REPLACE FUNCTION pgconductor.load_step(
    execution_id uuid,
    key text
) RETURNS jsonb
LANGUAGE sql
STABLE
SET search_path TO ''
AS $$
    SELECT jsonb_build_object('result', result) FROM pgconductor.steps
    WHERE execution_id = load_step.execution_id AND key = load_step.key;
$$;

-- Save a step result and optionally update execution run_at
CREATE OR REPLACE FUNCTION pgconductor.save_step(
    execution_id uuid,
    key text,
    result jsonb,
    run_in_ms integer default null
) RETURNS void
LANGUAGE sql
VOLATILE
SET search_path TO ''
AS $$
    WITH inserted AS (
        INSERT INTO pgconductor.steps (execution_id, key, result)
        VALUES (save_step.execution_id, save_step.key, save_step.result)
        ON CONFLICT (execution_id, key) DO NOTHING
        RETURNING id
    )
    UPDATE pgconductor.executions
    SET run_at = pgconductor.current_time() + (run_in_ms || ' milliseconds')::interval
    WHERE id = save_step.execution_id
      AND run_in_ms IS NOT NULL
      AND EXISTS (SELECT 1 FROM inserted);
$$;

-- TODO: use this table to manage concurrency, throttling and rate limiting
-- manage them when task is being updated/created
-- CREATE TABLE pgconductor.slots (
--     key text primary key,
--     task_key text not null,
--     locked_at timestamptz,
--     locked_by uuid
-- );
--
-- CREATE TABLE pgconductor.schedule (
--     name text REFERENCES pgconductor.tasks ON DELETE CASCADE,
--     key text not null,
--     cron text not null,
--     timezone text,
--     data jsonb,
--     PRIMARY KEY (name, key)
-- );

CREATE OR REPLACE FUNCTION pgconductor.orchestrators_heartbeat(
  orchestrator_id uuid,
  version text,
  migration_number integer
)
RETURNS boolean
LANGUAGE sql
VOLATILE
SET search_path TO ''
AS $function$
WITH latest AS (
  SELECT COALESCE(MAX(version), -1) AS db_version
  FROM pgconductor.schema_migrations
), upsert AS (
  INSERT INTO pgconductor.orchestrators AS o (
    id,
    version,
    migration_number,
    last_heartbeat_at
  )
  VALUES (
    orchestrators_heartbeat.orchestrator_id,
    orchestrators_heartbeat.version,
    orchestrators_heartbeat.migration_number,
    pgconductor.current_time()
  )
  ON CONFLICT (id)
  DO UPDATE
  SET
    last_heartbeat_at = pgconductor.current_time(),
    version = EXCLUDED.version,
    migration_number = EXCLUDED.migration_number
  RETURNING shutdown_signal
), marked AS (
  UPDATE pgconductor.orchestrators
  SET shutdown_signal = true
  WHERE id = orchestrators_heartbeat.orchestrator_id
    AND (SELECT db_version FROM latest) > orchestrators_heartbeat.migration_number
  RETURNING shutdown_signal
)
SELECT COALESCE(
  (SELECT shutdown_signal FROM marked),
  CASE
    WHEN (SELECT db_version FROM latest) > orchestrators_heartbeat.migration_number THEN true
    ELSE (SELECT shutdown_signal FROM upsert)
  END
);
$function$;

CREATE OR REPLACE FUNCTION pgconductor.sweep_orchestrators(migration_number int)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
UPDATE pgconductor.orchestrators
SET shutdown_signal = true
WHERE migration_number < sweep_orchestrators.migration_number;
$function$;

CREATE OR REPLACE FUNCTION pgconductor.recover_stale_orchestrators(max_age interval)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
WITH expired AS (
    DELETE FROM pgconductor.orchestrators o
    WHERE o.last_heartbeat_at < pgconductor.current_time() - recover_stale_orchestrators.max_age
    RETURNING o.id
)
UPDATE pgconductor.executions e
SET
  locked_by = NULL,
  locked_at = NULL
from expired
WHERE e.locked_by = expired.id
$function$
;

CREATE OR REPLACE FUNCTION pgconductor.orchestrator_shutdown(orchestrator_id uuid)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
WITH deleted AS (
    DELETE FROM pgconductor.orchestrators
    WHERE id = orchestrator_shutdown.orchestrator_id
    RETURNING id
)
UPDATE pgconductor.executions e
SET
  locked_by = NULL,
  locked_at = NULL
FROM deleted
WHERE e.locked_by = deleted.id
$function$
;

-- Trigger function to manage executions partitions based on tasks.partition setting
CREATE OR REPLACE FUNCTION pgconductor.manage_execution_partition()
 RETURNS trigger
 LANGUAGE plpgsql
 VOLATILE
 SET search_path TO ''
AS $function$
declare
  v_partition_name text;
begin
  v_partition_name := 'pgconductor.executions_' || replace(NEW.key, '-', '_');

  -- INSERT: Create partition if partition = true
  IF TG_OP = 'INSERT' THEN
    IF NEW.partition = true THEN
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF pgconductor.executions FOR VALUES IN (%L) with (fillfactor=70)',
        v_partition_name,
        NEW.key
      );
    END IF;

    RETURN NEW;
  END IF;

  -- UPDATE: Handle partition setting changes
  IF TG_OP = 'UPDATE' AND OLD.partition != NEW.partition THEN

    -- Changed from false to true: create partition and move rows
    IF NEW.partition = true THEN
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF pgconductor.executions FOR VALUES IN (%L) with (fillfactor=70)',
        v_partition_name,
        NEW.key
      );

      EXECUTE format(
        'WITH moved AS (
           DELETE FROM pgconductor.executions_default
           WHERE task_key = %L
           RETURNING *
         )
         INSERT INTO %I SELECT * FROM moved',
        NEW.key,
        v_partition_name
      );

    -- Changed from true to false: move rows back to default and drop partition
    ELSE
      -- Detach partition first (makes it a regular table)
      EXECUTE format(
        'ALTER TABLE pgconductor.executions DETACH PARTITION %I',
        v_partition_name
      );

      -- Move rows to default partition
      EXECUTE format(
        'INSERT INTO pgconductor.executions_default SELECT * FROM %I',
        v_partition_name
      );

      -- Drop the detached table
      EXECUTE format('DROP TABLE %I', v_partition_name);
    END IF;
  END IF;

  -- DELETE: Drop dedicated partition if it exists
  IF TG_OP = 'DELETE' THEN
    IF OLD.partition = true THEN
      EXECUTE format('DROP TABLE %I', v_partition_name);
    END IF;
    RETURN OLD;
  END IF;

  RETURN NEW;
end;
$function$;

-- Attach trigger to tasks table
CREATE TRIGGER manage_execution_partition_trigger
  AFTER INSERT OR UPDATE OR DELETE ON pgconductor.tasks
  FOR EACH ROW
  EXECUTE FUNCTION pgconductor.manage_execution_partition();

-- SELECT partman.create_parent(
--     p_parent_table => 'pgconductor.failed_executions',
--     p_control => 'failed_at',
--     p_type => 'native',
--     p_interval => '1 day',
--     p_premake => 7,
--     p_start_partition => current_date::timestamptz,
--     p_inherit_fk => true
-- );

-- SELECT partman.create_parent(
--     p_parent_table => 'pgconductor.completed_executions',
--     p_control => 'completed_at',
--     p_type => 'native',
--     p_interval => '1 day',
--     p_premake => 7,
--     p_start_partition => current_date::timestamptz,
--     p_inherit_fk => true
-- );
--
-- UPDATE partman.part_config
-- SET
--     retention = '7 days',
--     retention_keep_table = false,
--     retention_keep_index = false
-- WHERE parent_table IN (
--     'pgconductor.failed_executions',
--     'pgconductor.completed_executions'
-- );

CREATE OR REPLACE FUNCTION pgconductor.get_executions(task_key text, orchestrator_id uuid, batch_size integer default 100)
 RETURNS TABLE(id uuid, payload jsonb, waiting_on_execution_id uuid)
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
with task as (
  select window_start,
         window_end,
         max_attempts
  from pgconductor.tasks
  where key = get_executions.task_key
),

-- Only proceed if we're inside the active window (or no window defined)
allowed as (
  select 1
  from task t
  where
    (t.window_start is null and t.window_end is null)
    or (
      pgconductor.current_time()::timetz >= t.window_start
      and pgconductor.current_time()::timetz < t.window_end
    )
),

e as (
  select e.id
  from pgconductor.executions e
  join task t on true
  where exists (select 1 from allowed)
    and e.attempts < t.max_attempts
    and e.locked_at is null
    and e.run_at <= pgconductor.current_time()
    and e.task_key = get_executions.task_key
  order by e.priority asc, e.run_at asc
  limit get_executions.batch_size
  for update skip locked
)

update pgconductor.executions
set
  attempts = executions.attempts + 1,
  locked_by = get_executions.orchestrator_id,
  locked_at = pgconductor.current_time()
from e
where executions.id = e.id
returning executions.id, executions.payload, executions.waiting_on_execution_id;
$function$
;

-- Composite type for returning execution results
create type pgconductor.execution_result as (
    execution_id uuid,
    status text, -- 'completed', 'failed', or 'released'
    result jsonb,
    error text
);

-- Fixed Inngest-style backoff schedule (in seconds)
-- Returns backoff delay for given attempt number (1-indexed)
-- 15s, 30s, 1m, 2m, 5m, 10m, 20m, 40m, 1h, 2h
-- Attempts beyond the schedule use the last value (2h)
CREATE OR REPLACE FUNCTION pgconductor.backoff_seconds(attempt integer)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE
AS $function$
  SELECT (ARRAY[15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200])[LEAST(attempt, 10)];
$function$;

CREATE OR REPLACE FUNCTION pgconductor.return_executions(results pgconductor.execution_result[])
RETURNS integer
LANGUAGE sql
VOLATILE
SET search_path TO ''
AS $function$
  WITH results AS (
    SELECT *
    FROM unnest(return_executions.results) AS r
  ),
  -- move all completed executions to completed_executions
  completed AS (
    DELETE FROM pgconductor.executions e
    USING results r
    WHERE e.id = r.execution_id AND r.status = 'completed'
    RETURNING e.*
  ),
  inserted_completed AS (
    INSERT INTO pgconductor.completed_executions (
      id, task_key, key, created_at, failed_at, completed_at,
      payload, run_at, locked_at, locked_by, attempts, last_error, priority
    )
    SELECT
      id,
      task_key,
      key,
      created_at,
      failed_at,
      pgconductor.current_time(),
      payload,
      run_at,
      locked_at,
      locked_by,
      attempts,
      last_error,
      priority
    FROM completed
    RETURNING id
  ),
  -- complete executions that were waiting on completed children
  parent_steps_completed AS (
    INSERT INTO pgconductor.steps (execution_id, key, result)
    SELECT
      parent_e.id,
      parent_e.waiting_step_key,
      r.result
    FROM results r
    JOIN pgconductor.executions parent_e ON parent_e.waiting_on_execution_id = r.execution_id
    WHERE r.status = 'completed'
    ON CONFLICT (execution_id, key) DO NOTHING
    RETURNING execution_id
  ),
  woken_parents_completed AS (
    UPDATE pgconductor.executions
    SET
      run_at = pgconductor.current_time(),
      waiting_on_execution_id = NULL,
      waiting_step_key = NULL
    FROM parent_steps_completed
    WHERE id = parent_steps_completed.execution_id
    RETURNING id
  ),
  task AS (
    SELECT
      e.id AS execution_id,
      w.max_attempts
    FROM results r
    JOIN pgconductor.executions e ON e.id = r.execution_id
    JOIN pgconductor.tasks w ON w.key = e.task_key
    WHERE r.status = 'failed'
  ),
  permanently_failed_children AS (
    SELECT
      r.execution_id,
      r.error as child_error
    FROM results r
    JOIN pgconductor.executions e ON e.id = r.execution_id
    JOIN pgconductor.tasks w ON w.key = e.task_key
    WHERE r.status = 'failed' AND e.attempts >= w.max_attempts
  ),
  deleted_failed AS (
    DELETE FROM pgconductor.executions e
    WHERE (
      -- Direct failures: execution permanently failed
      e.id IN (SELECT execution_id FROM permanently_failed_children)
      OR
      -- CASCADE: parent waiting on permanently failed child
      e.waiting_on_execution_id IN (SELECT execution_id FROM permanently_failed_children)
    )
    RETURNING
      e.id,
      e.task_key,
      e.key,
      e.created_at,
      e.payload,
      e.run_at,
      e.locked_at,
      e.locked_by,
      e.attempts,
      e.priority,
      COALESCE(
        (SELECT error FROM results WHERE execution_id = e.id AND status = 'failed'),
        (SELECT 'Child execution failed: ' || COALESCE(child_error, 'unknown error')
         FROM permanently_failed_children
         WHERE execution_id = e.waiting_on_execution_id)
      ) as last_error
  ),
  retried AS (
    UPDATE pgconductor.executions e
    SET
      last_error = COALESCE(
        (SELECT error FROM results WHERE execution_id = e.id AND status = 'failed'),
        'unknown error'
      ),
      run_at = GREATEST(pgconductor.current_time(), e.run_at)
        + (pgconductor.backoff_seconds(e.attempts) * INTERVAL '1 second'),
      locked_by = NULL,
      locked_at = NULL
    FROM task w
    WHERE e.id = w.execution_id
      AND e.attempts < w.max_attempts
    RETURNING e.*
  ),
  inserted_failed AS (
    INSERT INTO pgconductor.failed_executions (
      id, task_key, key, created_at, failed_at, completed_at,
      payload, run_at, locked_at, locked_by, attempts, last_error, priority
    )
    SELECT
      id,
      task_key,
      key,
      created_at,
      pgconductor.current_time(),
      NULL,
      payload,
      run_at,
      locked_at,
      locked_by,
      attempts,
      last_error,
      priority
    FROM deleted_failed
    RETURNING id
  ),
  released AS (
    UPDATE pgconductor.executions e
    SET
      attempts = GREATEST(e.attempts - 1, 0),
      locked_by = NULL,
      locked_at = NULL
    FROM results r
    WHERE e.id = r.execution_id
      AND r.status = 'released'
    RETURNING e.id
  )
  SELECT 1;
$function$;

create type pgconductor.execution_spec as (
    task_key text,
    payload jsonb,
    run_at timestamptz,
    key text,
    priority integer
);

CREATE OR REPLACE FUNCTION pgconductor.upsert_task(
    key text,
    max_attempts integer default null,
    partition boolean default null,
    window_start timetz default null,
    window_end timetz default null
)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
  INSERT INTO pgconductor.tasks (
    key,
    max_attempts,
    partition,
    window_start,
    window_end
  )
  VALUES (
    upsert_task.key,
    COALESCE(upsert_task.max_attempts, 3),
    COALESCE(upsert_task.partition, false),
    upsert_task.window_start,
    upsert_task.window_end
  )
  ON CONFLICT (key)
  DO UPDATE SET
    max_attempts = COALESCE(EXCLUDED.max_attempts, pgconductor.tasks.max_attempts),
    partition = COALESCE(EXCLUDED.partition, pgconductor.tasks.partition),
    window_start = EXCLUDED.window_start,
    window_end = EXCLUDED.window_end;
$function$;

CREATE OR REPLACE FUNCTION pgconductor.invoke(
    specs pgconductor.execution_spec[]
)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 VOLATILE
 SET search_path TO ''
AS $function$
begin
  -- Clear keys that are currently locked so a subsequent insert can succeed.
  update pgconductor.executions as e
  set
    key = null,
    attempts = w.max_attempts,
    locked_by = null,
    locked_at = null,
    last_error = 'superseded by reinvoke'
  from unnest(specs) spec
  join pgconductor.tasks w on w.key = spec.task_key
  where spec.key is not null
  and e.key = spec.key
  and e.task_key = spec.task_key
  and e.locked_at is not null;

  return query insert into pgconductor.executions as e (
    id,
    task_key,
    payload,
    run_at,
    key,
    priority
  )
    select
      pgconductor.portable_uuidv7(),
      spec.task_key,
      coalesce(spec.payload, '{}'::jsonb),
      coalesce(spec.run_at, pgconductor.current_time()),
      spec.key,
      coalesce(spec.priority, 0)
    from unnest(specs) spec
  returning e.id;
end;
$function$
;

CREATE OR REPLACE FUNCTION pgconductor.invoke(
    task_key text,
    payload jsonb default null,
    run_at timestamptz default null,
    key text default null,
    priority integer default null,
    parent_execution_id uuid default null,
    parent_step_key text default null,
    parent_timeout_ms integer default null
)
 RETURNS uuid
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
WITH inserted_child AS (
  INSERT INTO pgconductor.executions (
    id,
    task_key,
    payload,
    run_at,
    key,
    priority
  )
  VALUES (
    pgconductor.portable_uuidv7(),
    invoke.task_key,
    COALESCE(invoke.payload, '{}'::jsonb),
    COALESCE(invoke.run_at, pgconductor.current_time()),
    invoke.key,
    COALESCE(invoke.priority, 0)
  )
  RETURNING id
),
updated_parent AS (
  UPDATE pgconductor.executions
  SET
    waiting_on_execution_id = (SELECT id FROM inserted_child),
    waiting_step_key = invoke.parent_step_key,
    run_at = CASE
      WHEN invoke.parent_timeout_ms IS NOT NULL THEN
        pgconductor.current_time() + (invoke.parent_timeout_ms || ' milliseconds')::interval
      ELSE
        'infinity'::timestamptz
    END
  WHERE id = invoke.parent_execution_id
    AND invoke.parent_execution_id IS NOT NULL
  RETURNING 1
)
SELECT id FROM inserted_child;
$function$
;

CREATE OR REPLACE FUNCTION pgconductor.clear_waiting_state(
  execution_id uuid
)
RETURNS void
LANGUAGE sql
VOLATILE
SET search_path TO ''
AS $function$
  UPDATE pgconductor.executions
  SET
    waiting_on_execution_id = NULL,
    waiting_step_key = NULL
  WHERE id = clear_waiting_state.execution_id;
$function$
;

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

-- Returns either the actual current timestamp or a fake one if
-- the session sets `pgconductor.fake_now`. This lets tests control time.
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
    id uuid primary key,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version text,
    migration_number integer,
    shutdown_signal boolean default false not null
);

CREATE TABLE pgconductor.executions (
    id uuid,
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

-- TODO: register task rpc for upsert that is called on worker startup

-- TODO: add settings here
CREATE TABLE pgconductor.tasks (
    key text primary key,
    -- we could validate execution payloads on insert if pg jsonchema extension is available
    input_schema jsonb default '{}'::jsonb not null,

    -- retry settings
    max_attempts integer default 3 not null,
    retry_exponential_backoff boolean default true not null,
    retry_delay_seconds integer default 60 not null,
    max_retry_delay_seconds integer,

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
    id bigint primary key generated always as identity,
    key text not null,
    execution_id bigint not null,
    result jsonb default '{}'::jsonb not null,
    created_at timestamptz default pgconductor.current_time() not null
);

-- TODO: use this table to manage concurrency, throttling and rate limiting
-- manage them when task is being updated/created
CREATE TABLE pgconductor.slots (
    key text primary key,
    task_key text not null,
    locked_at timestamptz,
    locked_by uuid
);

CREATE TABLE pgconductor.schedule (
    name text REFERENCES pgconductor.tasks ON DELETE CASCADE,
    key text not null,
    cron text not null,
    timezone text,
    data jsonb,
    PRIMARY KEY (name, key)
);

CREATE OR REPLACE FUNCTION pgconductor.orchestrators_heartbeat(
  v_orchestrator_id uuid,
  v_version text,
  v_migration_number integer
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
    v_orchestrator_id,
    v_version,
    v_migration_number,
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
  WHERE id = v_orchestrator_id
    AND (SELECT db_version FROM latest) > v_migration_number
  RETURNING shutdown_signal
)
SELECT COALESCE(
  (SELECT shutdown_signal FROM marked),
  CASE
    WHEN (SELECT db_version FROM latest) > v_migration_number THEN true
    ELSE (SELECT shutdown_signal FROM upsert)
  END
);
$function$;

CREATE OR REPLACE FUNCTION pgconductor.sweep_orchestrators(v_migration_number int)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
UPDATE pgconductor.orchestrators
SET shutdown_signal = true
WHERE migration_number < v_migration_number;
$function$;

CREATE OR REPLACE FUNCTION pgconductor.recover_stale_orchestrators(v_max_age interval)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
WITH expired AS (
    DELETE FROM pgconductor.orchestrators o
    WHERE o.last_heartbeat_at < pgconductor.current_time() - v_max_age
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

CREATE OR REPLACE FUNCTION pgconductor.orchestrator_shutdown(v_orchestrator_id uuid)
 RETURNS void
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
WITH deleted AS (
    DELETE FROM pgconductor.orchestrators
    WHERE id = v_orchestrator_id
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
  rnd := uuid_send(uuid_generate_v4 ());
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

CREATE OR REPLACE FUNCTION pgconductor.get_executions(v_task_key text, v_orchestrator_id uuid, v_batch_size integer default 100)
 RETURNS TABLE(id uuid, payload jsonb)
 LANGUAGE sql
 STABLE
 SET search_path TO ''
AS $function$
with task as (
  select window_start,
         window_end,
         max_attempts
  from pgconductor.tasks
  where key = v_task_key
),

-- Only proceed if we’re inside the active window (or no window defined)
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
    and e.task_key = v_task_key
  order by e.priority asc, e.run_at asc
  limit v_batch_size
  for update skip locked
)

update pgconductor.executions
set
  attempts = executions.attempts + 1,
  locked_by = v_orchestrator_id,
  locked_at = pgconductor.current_time()
from e
where executions.id = e.id
returning executions.id, executions.payload;
$function$
;

-- Composite type for returning execution results
create type pgconductor.execution_result as (
    execution_id uuid,
    status text, -- 'completed', 'failed', or 'released'
    result jsonb,
    error text
);

CREATE OR REPLACE FUNCTION pgconductor.return_executions(v_results pgconductor.execution_result[])
RETURNS void
LANGUAGE plpgsql
VOLATILE
SET search_path TO ''
AS $function$
BEGIN
  WITH results AS (
    SELECT *
    FROM unnest(v_results) AS r
  ),
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
  task AS (
    SELECT
      e.id AS execution_id,
      w.retry_exponential_backoff,
      w.retry_delay_seconds,
      w.max_retry_delay_seconds,
      w.max_attempts
    FROM pgconductor.executions e
    JOIN pgconductor.tasks w ON w.key = e.task_key
    JOIN results r ON r.execution_id = e.id
    WHERE r.status = 'failed'
  ),
  deleted_failed AS (
    DELETE FROM pgconductor.executions e
    USING task w
    WHERE e.id = w.execution_id
      AND e.attempts >= w.max_attempts
    RETURNING e.*
  ),
  retried AS (
    UPDATE pgconductor.executions e
    SET
      last_error = COALESCE(
        (SELECT r.error FROM results r WHERE r.execution_id = e.id),
        'unknown error'
      ),
      run_at = CASE
        WHEN w.retry_exponential_backoff THEN
          GREATEST(pgconductor.current_time(), e.run_at)
          + LEAST(
              EXP(LEAST(e.attempts, 10)) * INTERVAL '1 second',
              COALESCE(w.max_retry_delay_seconds * INTERVAL '1 second', INTERVAL '1 hour')
            )
        ELSE
          GREATEST(pgconductor.current_time(), e.run_at)
          + (w.retry_delay_seconds * INTERVAL '1 second')
      END,
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
      completed_at,
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
END;
$function$;

create type pgconductor.execution_spec as (
    task_key text,
    payload jsonb,
    run_at timestamptz,
    key text,
    priority integer
);

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
    priority integer default null
)
 RETURNS uuid
 LANGUAGE sql
 VOLATILE
 SET search_path TO ''
AS $function$
select id from pgconductor.invoke(array[
    (task_key, payload, run_at, key, priority)::pgconductor.execution_spec
])
$function$
;

-- Research only: times are supplied by TypeScript, in epoch milliseconds.
create table if not exists tasks (
    key text not null,
    queue text not null default 'default',
    concurrency_limit integer check (concurrency_limit is null or concurrency_limit > 0),
    group_concurrency_limit integer check (group_concurrency_limit is null or group_concurrency_limit > 0),
    max_attempts integer not null default 3,
    remove_on_complete_days integer,
    remove_on_fail_days integer,
    primary key (key, queue)
);

create table if not exists executions (
    id text not null,
    task_key text not null,
    queue text not null default 'default',
    dedupe_key text,
    singleton_on integer,
    cron_expression text,
    created_at integer not null,
    run_at integer not null,
    failed_at integer,
    completed_at integer,
    locked_at integer,
    locked_by text,
    payload text check (payload is null or json_valid(payload)),
    result text check (result is null or json_valid(result)),
    trace_context text check (trace_context is null or json_valid(trace_context)),
    metadata text check (metadata is null or json_valid(metadata)),
    "group" text,
    attempts integer not null default 0,
    last_error text,
    cancelled integer not null default 0 check (cancelled in (0, 1)),
    priority integer not null default 0,
    waiting_on_execution_id text,
    waiting_step_key text,
    parent_execution_id text,
    subscription_id text,
    is_available integer generated always as
        (locked_at is null and failed_at is null and completed_at is null) stored not null,
    primary key (id, queue),
    unique (task_key, dedupe_key, queue),
    check (subscription_id is null or parent_execution_id is not null)
);

create unique index if not exists executions_singleton
    on executions (task_key, singleton_on, coalesce(dedupe_key, ''), queue)
    where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false;
-- Queue leads each index because SQLite has no queue partitions / INCLUDE.
create index if not exists executions_available
    on executions (queue, priority, run_at, created_at, id, task_key) where is_available = true;
create index if not exists executions_task_available
    on executions (queue, task_key, priority, run_at, created_at, id) where is_available = true;
create index if not exists executions_waiting
    on executions (waiting_on_execution_id) where waiting_on_execution_id is not null;
create index if not exists executions_parent
    on executions (parent_execution_id) where parent_execution_id is not null;
create index if not exists executions_locked
    on executions (locked_by) where locked_by is not null;
create index if not exists executions_completed
    on executions (queue, completed_at) where completed_at is not null;
create index if not exists executions_failed
    on executions (queue, failed_at) where failed_at is not null;
create index if not exists executions_cron
    on executions (queue, (case when instr(dedupe_key, ':') = 0 then '' else
        substr(substr(dedupe_key, instr(dedupe_key, ':') + 1), 1,
            instr(substr(dedupe_key, instr(dedupe_key, ':') + 1) || ':', ':') - 1)
        end)) where cron_expression is not null;
create index if not exists executions_task on executions (queue, task_key);
create index if not exists executions_active
    on executions (queue, task_key, "group")
    where locked_at is not null and failed_at is null and completed_at is null;

create table if not exists steps (
    execution_id text not null,
    queue text not null,
    key text not null,
    result text check (result is null or json_valid(result)),
    created_at integer not null,
    primary key (execution_id, key),
    foreign key (execution_id, queue) references executions (id, queue) on delete cascade
);
create table if not exists orchestrators (
    id text primary key,
    last_heartbeat_at integer not null
);
create index if not exists orchestrators_heartbeat on orchestrators (last_heartbeat_at);

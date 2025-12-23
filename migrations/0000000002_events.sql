alter table pgconductor._private_executions
    add column if not exists subscription_id uuid;

create table if not exists pgconductor._private_custom_events (
    id uuid default pgconductor._private_portable_uuidv7() not null,
    event_key text not null,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz default pgconductor._private_current_time() not null,
    primary key (created_at, id)
) partition by range (created_at);

create index if not exists idx_custom_events_event_key
    on pgconductor._private_custom_events (event_key, created_at desc);

-- Create initial partition for custom events (will cover many years)
create table if not exists pgconductor._private_custom_events_default
    partition of pgconductor._private_custom_events
    for values from (minvalue) to (maxvalue);

create table if not exists pgconductor._private_event_subscriptions (
    id uuid primary key default pgconductor._private_portable_uuidv7(),

    task_key text not null,
    queue text not null,

    event_key text,
    schema_name text,
    table_name text,
    operation pgconductor._private_event_operation,

    when_clause text,
    payload_fields text[],
    column_names text[],

    created_at timestamptz not null default pgconductor._private_current_time(),

    constraint chk_event_type check (
        (event_key is not null and schema_name is null and table_name is null and operation is null)
        or
        (event_key is null and schema_name is not null and table_name is not null and operation is not null)
    )
);

create index if not exists idx_event_subscriptions_custom
    on pgconductor._private_event_subscriptions (event_key)
    where event_key is not null;

create index if not exists idx_event_subscriptions_database
    on pgconductor._private_event_subscriptions (schema_name, table_name, operation)
    where schema_name is not null;

create or replace function pgconductor._private_sync_custom_event_trigger()
    returns trigger
    language plpgsql
    security definer
    set search_path to ''
as $_$
declare
    v_invoke_blocks text;
    v_has_subscriptions boolean;
begin
    -- Only process custom event subscriptions (event_key is not null)
    if coalesce(new.event_key, old.event_key) is null then
        return coalesce(new, old);
    end if;

    drop trigger if exists pgconductor_custom_event on pgconductor._private_custom_events;
    drop function if exists pgconductor._private_trigger_custom_event;

    select exists(
        select 1
        from pgconductor._private_event_subscriptions
        where event_key is not null
    ) into v_has_subscriptions;

    if v_has_subscriptions then
        v_invoke_blocks := (
            select string_agg(format(
                $sql$
                if new.event_key = %L and (%s) then
                    v_task_keys := array_append(v_task_keys, %L);
                    v_queues := array_append(v_queues, %L);
                    v_payloads := array_append(v_payloads, jsonb_build_object('event', new.event_key, 'payload', %s));
                    v_subscription_ids := array_append(v_subscription_ids, %L);
                end if;
                $sql$,
                sub.event_key,
                coalesce(nullif(sub.when_clause, ''), 'true'),
                sub.task_key,
                t.queue,
                pgconductor._private_build_payload_fields(sub.payload_fields, 'new.payload'),
                sub.id
            ), e'\n')
            from pgconductor._private_event_subscriptions as sub
            join pgconductor._private_tasks as t on t.key = sub.task_key
            where sub.event_key is not null
        );

        execute format(
            $sql$
            create or replace function pgconductor._private_trigger_custom_event()
                returns trigger
                language plpgsql
                security definer
                set search_path to ''
            as $inner$
            declare
                v_task_keys text[];
                v_queues text[];
                v_payloads jsonb[];
                v_subscription_ids uuid[];
            begin
                %s

                if array_length(v_task_keys, 1) > 0 then
                    insert into pgconductor._private_executions (task_key, queue, payload, subscription_id)
                    select unnest(v_task_keys), unnest(v_queues), unnest(v_payloads), unnest(v_subscription_ids);
                end if;

                return new;
            end
            $inner$
            $sql$,
            v_invoke_blocks
        );

        execute $sql$
            create trigger pgconductor_custom_event
                after insert on pgconductor._private_custom_events
                for each row
                execute function pgconductor._private_trigger_custom_event()
        $sql$;
    end if;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end;
$_$;

create trigger sync_custom_event_trigger
    after insert or delete or update on pgconductor._private_event_subscriptions
    for each row
    execute function pgconductor._private_sync_custom_event_trigger();

create or replace function pgconductor._private_build_payload_fields(
    p_payload_fields text[],
    p_payload_expr text
)
    returns text
    language sql
    immutable
    set search_path to ''
as $_$
    select case
        when p_payload_fields is null then p_payload_expr
        else 'jsonb_build_object(' || array_to_string(
            array(
                select format('%L, %s->%L', field, p_payload_expr, field)
                from unnest(p_payload_fields) as field
            ),
            ', '
        ) || ')'
    end;
$_$;

create or replace function pgconductor._private_build_column_list(
    p_column_names text[],
    p_record_name text
)
    returns text
    language sql
    immutable
    set search_path to ''
as $_$
    select case
        when p_column_names is null then format('row_to_json(%I.*)', p_record_name)
        else 'jsonb_build_object(' || array_to_string(
            array(
                select format('%L, %I.%I', col, p_record_name, col)
                from unnest(p_column_names) as col
            ),
            ', '
        ) || ')'
    end;
$_$;

create or replace function pgconductor._private_sync_database_trigger()
    returns trigger
    language plpgsql
    security definer
    set search_path to ''
as $_$
declare
    v_table_name text := coalesce(new.table_name, old.table_name);
    v_schema_name text := coalesce(new.schema_name, old.schema_name);
    v_op pgconductor._private_event_operation;
    v_invoke_blocks text;
    v_has_subscriptions boolean;
begin
    -- Only process database event subscriptions (schema_name is not null)
    if v_schema_name is null then
        return coalesce(new, old);
    end if;

    -- Process each operation type (insert, update, delete)
    foreach v_op in array array['insert', 'update', 'delete']::pgconductor._private_event_operation[] loop
        -- Drop existing trigger and function
        execute format(
            'drop trigger if exists pgconductor_event_%s on %I.%I',
            v_op::text, v_schema_name, v_table_name
        );

        execute format(
            'drop function if exists pgconductor._private_trigger_event_%s_on_%I_%I',
            v_op::text, v_schema_name, v_table_name
        );

        -- Check if there are any subscriptions for this operation
        select exists(
            select 1
            from pgconductor._private_event_subscriptions
            where table_name = v_table_name
                and schema_name = v_schema_name
                and operation = v_op
        ) into v_has_subscriptions;

        if v_has_subscriptions then
            -- Build if blocks to check conditions and append to arrays
            -- Each subscription's when_clause is evaluated inside the trigger function
            v_invoke_blocks := (
                select string_agg(format(
                    $sql$
                    if %s then
                        v_task_keys := array_append(v_task_keys, %L);
                        v_queues := array_append(v_queues, %L);
                        v_payloads := array_append(v_payloads, jsonb_build_object(
                            'event', %L,
                            'payload', jsonb_build_object(
                                'old', case when tg_op is distinct from 'INSERT' then %s else null end,
                                'new', case when tg_op is distinct from 'DELETE' then %s else null end,
                                'tg_table', tg_table_name,
                                'tg_op', tg_op
                            )
                        ));
                        v_subscription_ids := array_append(v_subscription_ids, %L);
                    end if;
                    $sql$,
                    coalesce(nullif(sub.when_clause, ''), 'true'),
                    sub.task_key,
                    t.queue,
                    format('%s.%s.%s', v_schema_name, v_table_name, v_op::text),
                    pgconductor._private_build_column_list(sub.column_names, 'old'),
                    pgconductor._private_build_column_list(sub.column_names, 'new'),
                    sub.id
                ), e'\n')
                from pgconductor._private_event_subscriptions as sub
                join pgconductor._private_tasks as t on t.key = sub.task_key
                where sub.table_name = v_table_name
                    and sub.schema_name = v_schema_name
                    and sub.operation = v_op
            );

            -- Create trigger function
            execute format(
                $sql$
                create or replace function pgconductor._private_trigger_event_%s_on_%I_%I()
                    returns trigger
                    language plpgsql
                    security definer
                    set search_path to ''
                as $inner$
                declare
                    v_task_keys text[];
                    v_queues text[];
                    v_payloads jsonb[];
                    v_subscription_ids uuid[];
                begin
                    %s

                    if array_length(v_task_keys, 1) > 0 then
                        insert into pgconductor._private_executions (task_key, queue, payload, subscription_id)
                        select unnest(v_task_keys), unnest(v_queues), unnest(v_payloads), unnest(v_subscription_ids);
                    end if;

                    if tg_op = 'DELETE' then
                        return old;
                    end if;

                    return new;
                end
                $inner$
                $sql$,
                v_op::text,
                v_schema_name,
                v_table_name,
                v_invoke_blocks
            );

            -- Create trigger
            execute format(
                $sql$
                create trigger pgconductor_event_%s
                    after %s on %I.%I
                    for each row
                    execute function pgconductor._private_trigger_event_%s_on_%I_%I()
                $sql$,
                v_op::text,
                upper(v_op::text),
                v_schema_name,
                v_table_name,
                v_op::text,
                v_schema_name,
                v_table_name
            );
        end if;
    end loop;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end;
$_$;

create trigger sync_database_trigger
    after insert or delete or update on pgconductor._private_event_subscriptions
    for each row
    execute function pgconductor._private_sync_database_trigger();

create or replace function pgconductor.emit_event(
    p_event_key text,
    p_payload jsonb default '{}'::jsonb
)
    returns uuid
    language sql
    volatile
    set search_path to ''
as $_$
    insert into pgconductor._private_custom_events (event_key, payload)
    values (p_event_key, p_payload)
    returning id;
$_$;

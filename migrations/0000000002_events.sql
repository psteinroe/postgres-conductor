alter table pgconductor._private_executions
    add column subscription_id uuid,
    add column event_id uuid,
    add column event_created_at timestamptz;

create unique index idx_executions_event_subscription
    on pgconductor._private_executions (event_created_at, event_id, subscription_id, queue)
    where event_id is not null and event_created_at is not null and subscription_id is not null;

-- Events are an append-only inbox. processed_at acknowledges successful fan-out;
-- a failed processor transaction leaves the event unprocessed automatically.
create table if not exists pgconductor._private_custom_events (
    id uuid default pgconductor._private_portable_uuidv7() not null,
    event_key text not null,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz default pgconductor._private_current_time() not null,
    processed_at timestamptz,
    primary key (created_at, id)
) partition by range (created_at);

create table if not exists pgconductor._private_custom_events_default
    partition of pgconductor._private_custom_events
    for values from (minvalue) to (maxvalue);

create index if not exists idx_custom_events_pending
    on pgconductor._private_custom_events (event_key, created_at, id)
    where processed_at is null;

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
    filter jsonb,
    kind text not null default 'task_trigger',
    created_at timestamptz not null default pgconductor._private_current_time(),
    constraint chk_event_type check (
        (event_key is not null and schema_name is null and table_name is null and operation is null)
        or
        (event_key is null and schema_name is not null and table_name is not null and operation is not null)
    ),
    constraint chk_event_subscription_kind check (kind in ('task_trigger', 'execution_wait'))
);

create index if not exists idx_event_subscriptions_custom
    on pgconductor._private_event_subscriptions (event_key)
    where event_key is not null;
create index if not exists idx_event_subscriptions_database
    on pgconductor._private_event_subscriptions (schema_name, table_name, operation)
    where schema_name is not null;

-- Compiled equality constraints are deliberately separate from event payloads.
-- This keeps emission cheap while giving the set-wise matcher an indexed source.
create table if not exists pgconductor._private_event_subscription_filters (
    subscription_id uuid not null,
    event_key text not null,
    field_name text not null,
    value jsonb not null,
    primary key (subscription_id, field_name, value),
    constraint fk_event_filter_subscription foreign key (subscription_id)
        references pgconductor._private_event_subscriptions(id) on delete cascade
);
create index if not exists idx_event_subscription_filters_match
    on pgconductor._private_event_subscription_filters (event_key, field_name, value);

create table if not exists pgconductor._private_event_deliveries (
    event_created_at timestamptz not null,
    event_id uuid not null,
    subscription_id uuid not null,
    delivered_at timestamptz not null default pgconductor._private_current_time(),
    primary key (event_created_at, event_id, subscription_id),
    constraint fk_event_delivery_event foreign key (event_created_at, event_id)
        references pgconductor._private_custom_events(created_at, id) on delete cascade,
    constraint fk_event_delivery_subscription foreign key (subscription_id)
        references pgconductor._private_event_subscriptions(id) on delete cascade
);

create or replace function pgconductor._private_compile_event_filters()
returns trigger language plpgsql security definer set search_path to '' as $function$
begin
    if tg_op = 'DELETE' then
        delete from pgconductor._private_event_subscription_filters where subscription_id = old.id;
        return old;
    end if;
    delete from pgconductor._private_event_subscription_filters where subscription_id = new.id;
    if new.event_key is not null and new.filter is not null then
        insert into pgconductor._private_event_subscription_filters(subscription_id,event_key,field_name,value)
        select new.id, new.event_key, f.key, values.value
        from jsonb_each(new.filter) as f
        cross join lateral jsonb_array_elements(f.value) as values(value)
        on conflict do nothing;
    end if;
    return new;
end;
$function$;

create trigger compile_event_filters
    after insert or update or delete on pgconductor._private_event_subscriptions
    for each row execute function pgconductor._private_compile_event_filters();

create or replace function pgconductor._private_eval_event_when(p_expression text, p_payload jsonb)
returns boolean language plpgsql security definer set search_path to '' as $function$
declare result boolean;
begin
    -- `when` is retained as the legacy custom-event escape hatch. New filters
    -- use compiled constraints; database-trigger `when` remains native below.
    execute format('select (%s)', replace(p_expression, 'new.payload', '$1')) into result using p_payload;
    return coalesce(result, false);
end;
$function$;

create or replace function pgconductor._private_extract_event_payload(p_fields text[], p_payload jsonb)
returns jsonb language sql immutable set search_path to '' as $function$
    select case when p_fields is null then p_payload
        else coalesce((select jsonb_object_agg(key, p_payload -> key) from unnest(p_fields) key), '{}'::jsonb)
    end;
$function$;

create or replace function pgconductor._private_process_custom_events(
    p_batch_size integer default 100
) returns integer language plpgsql volatile security definer set search_path to '' as $function$
declare
    v_count integer := 0;
    v_now timestamptz := pgconductor._private_current_time();
begin
    with candidates as materialized (
        select e.created_at, e.id, e.event_key, e.payload
        from pgconductor._private_custom_events e
        where e.processed_at is null
        order by e.created_at, e.id
        limit greatest(coalesce(p_batch_size, 0), 0)
        for update skip locked
    ), matches as materialized (
        select c.created_at as event_created_at, c.id as event_id,
               s.id as subscription_id, s.task_key, s.queue,
               pgconductor._private_extract_event_payload(s.payload_fields, c.payload) as selected_payload,
               c.event_key
        from candidates c
        join pgconductor._private_event_subscriptions s on s.event_key = c.event_key
        join pgconductor._private_tasks t on t.key = s.task_key and t.queue = s.queue
        where s.when_clause is null
          and not exists (
              select 1
              from pgconductor._private_event_subscription_filters f
              where f.subscription_id = s.id
                and not exists (
                    select 1
                    from pgconductor._private_event_subscription_filters allowed
                    where allowed.subscription_id = f.subscription_id
                      and allowed.field_name = f.field_name
                      and (c.payload -> allowed.field_name) = allowed.value
                )
          )
    ), inserted_deliveries as (
        insert into pgconductor._private_event_deliveries(event_created_at, event_id, subscription_id)
        select event_created_at, event_id, subscription_id
        from matches
        on conflict do nothing
        returning event_created_at, event_id, subscription_id
    ), inserted_executions as (
        insert into pgconductor._private_executions(
            task_key, queue, payload, event_created_at, event_id, subscription_id
        )
        select m.task_key, m.queue,
               jsonb_build_object('event', m.event_key, 'payload', m.selected_payload),
               d.event_created_at, d.event_id, d.subscription_id
        from inserted_deliveries d
        join matches m using (event_created_at, event_id, subscription_id)
        on conflict (event_created_at, event_id, subscription_id, queue)
            where event_id is not null and event_created_at is not null and subscription_id is not null
            do nothing
        returning event_created_at, event_id
    ), processed as (
        update pgconductor._private_custom_events e
           set processed_at = v_now
          from candidates c
         where e.created_at = c.created_at
           and e.id = c.id
        returning e.id
    )
    select count(*) into v_count from processed;
    return v_count;
end;
$function$;

-- Events are retained until acknowledged. This helper is safe to call from
-- maintenance: active/retryable work is never removed.
create or replace function pgconductor._private_remove_processed_events(p_before timestamptz, p_batch_size integer default 1000)
returns integer language sql volatile security definer set search_path to '' as $function$
    with candidates as (
        select created_at, id
        from pgconductor._private_custom_events
        where processed_at is not null and processed_at < p_before
        order by processed_at, created_at, id
        limit greatest(coalesce(p_batch_size, 0), 0)
        for update skip locked
    ), deleted as (
        delete from pgconductor._private_custom_events e
        using candidates c
        where e.created_at = c.created_at and e.id = c.id
        returning 1
    )
    select count(*)::integer from deleted;
$function$;

create or replace function pgconductor.emit_event(
    p_event_key text,
    p_payload jsonb default '{}'::jsonb
) returns uuid language sql volatile set search_path to '' as $function$
    insert into pgconductor._private_custom_events (event_key, payload)
    values (p_event_key, p_payload)
    returning id;
$function$;

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
                join pgconductor._private_tasks as t on t.key = sub.task_key and t.queue = sub.queue
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


create extension if not exists "uuid-ossp";

create schema if not exists pgconductor;

create table if not exists pgconductor.schema_migrations (
  version int primary key,
  name text not null,
  applied_at timestamp default now(),
  breaking boolean default false
);

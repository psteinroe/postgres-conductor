-- Install required extensions
create extension if not exists "uuid-ossp";

create table address_book (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  description text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table contact (
  id uuid primary key default gen_random_uuid(),
  address_book_id uuid not null references address_book(id),
  first_name text not null,
  last_name text,
  email text,
  phone text,
  is_favorite boolean not null default false,
  metadata jsonb,
  created_at timestamptz not null default now()
);

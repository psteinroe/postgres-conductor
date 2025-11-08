CREATE SCHEMA IF NOT EXISTS pgconductor;

CREATE TABLE pgconductor.schema_migrations (
  version INT PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TIMESTAMP DEFAULT NOW(),
  breaking boolean default false
);



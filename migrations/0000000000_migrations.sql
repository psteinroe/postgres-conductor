DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_partman') THEN
    RAISE EXCEPTION 'Extension pg_partman is not installed. Run: CREATE EXTENSION pg_partman;';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp') THEN
    RAISE EXCEPTION 'Extension uuid-ossp is not installed. Run: CREATE EXTENSION "uuid-ossp";';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS pgconductor;

CREATE TABLE pgconductor.schema_migrations (
  version INT PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TIMESTAMP DEFAULT NOW(),
  breaking boolean default false
);

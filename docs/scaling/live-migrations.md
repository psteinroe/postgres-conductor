# Live Migrations

Postgres Conductor deploys schema updates with zero downtime using automatic migration coordination.

## Overview

When you deploy a new version with schema changes, orchestrators coordinate to apply migrations safely without requiring manual shutdown or intervention.

## How It Works

When an orchestrator starts, it:

1. **Checks migration status** - Compares database version with its own migration version
2. **Attempts migration lock** - Uses Postgres advisory locks to coordinate with other orchestrators
3. **Signals old workers** - For breaking changes, tells older orchestrators to shut down gracefully
4. **Waits for completion** - Ensures old workers finish their current tasks
5. **Applies migrations** - Runs migrations in a transaction
6. **Registers and starts** - Begins processing tasks with the new schema

This happens automatically on every orchestrator startup.

## What's Next?

- [Horizontal Scaling](horizontal.md) - Add more workers for capacity
- [Worker Configuration](worker-config.md) - Tune worker performance
- [Maintenance Task](maintenance.md) - Database housekeeping

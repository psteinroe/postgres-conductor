# Maintenance & Cleanup

Postgres Conductor runs automatic maintenance on each queue to keep the database clean.

## Automatic Maintenance

Maintenance runs daily on each queue:

- Removes old completed executions (based on task retention config)
- Removes old failed executions (based on task retention config)
- On the always-present internal queue, removes settled custom-event log rows after seven days

Event cleanup is independent of execution retention. It is bounded and skips events whose internal dispatch execution may still be retried.

## What's Next?

- [Worker Configuration](worker-config.md) - Optimize worker performance
- [Horizontal Scaling](horizontal.md) - Add more workers
- [Live Migrations](live-migrations.md) - Zero-downtime deployments

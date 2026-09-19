# Maintenance & Cleanup

Postgres Conductor runs automatic maintenance on each queue to keep the database clean.

## Automatic Maintenance

Maintenance runs daily on each queue:

- Removes old completed executions (based on task retention config)
- Removes old failed executions (based on task retention config)

Custom events use ordinary internal executions and therefore do not require a separate event-log cleanup pass.

## What's Next?

- [Worker Configuration](worker-config.md) - Optimize worker performance
- [Horizontal Scaling](horizontal.md) - Add more workers
- [Live Migrations](live-migrations.md) - Zero-downtime deployments

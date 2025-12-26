# Overview

Postgres Conductor is a durable execution engine using only Postgres, eliminating the need for additional infrastructure like Redis or message queues.

## Why Postgres Conductor?

**Durable Execution** - Tasks survive crashes and restarts through automatic step memoization

**Multiple Triggers** - Invocable APIs, cron schedules, custom events, and database triggers

**Workflows** - Invoke child tasks and wait for results with full type safety

**Performance** - Process up to 20,000 jobs per second with just Postgres

**Type-Safety** - Share common event and task schemas across your services

**Simple Infrastructure** - No Redis, no message queues - just Postgres

## Get Started

Ready to build with Postgres Conductor?

- [Installation](getting-started/installation.md) - Set up Postgres Conductor
- [Your First Task](getting-started/first-task.md) - Complete walkthrough with examples
- [Task Triggers](crafting-tasks/triggers.md) - Learn about different trigger types
- [Retries and Steps](crafting-tasks/retries-and-steps.md) - Build durable, fault-tolerant tasks


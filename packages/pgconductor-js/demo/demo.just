# pgconductor demo commands
# Run from project root: just demo/<command>

_default:
    @just --list --justfile {{justfile()}}

# Start demo infrastructure (postgres)
up:
    docker compose up -d

# Stop demo infrastructure
down:
    docker compose down

# Rebuild and restart demo infrastructure
restart:
    docker compose down
    docker compose up -d --build

# View logs for demo infrastructure
logs service="":
    #!/usr/bin/env bash
    if [ -z "{{service}}" ]; then
        docker compose logs -f
    else
        docker compose logs -f {{service}}
    fi

# Check status of demo infrastructure
status:
    docker compose ps

# Run worker for demo 01 (basic task execution)
worker-01:
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/01-worker.ts

# Invoke demo 01 task (optionally pass name)
invoke-01 name="World":
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/01-invoke.ts {{name}}

# Run worker for demo 02 (steps & sleep)
worker-02:
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/02-worker.ts

# Invoke demo 02 task (optionally pass items)
invoke-02 *items="apple banana cherry":
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/02-invoke.ts {{items}}

# Run worker for demo 03 (child task invocation)
worker-03:
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/03-worker.ts

# Invoke demo 03 task (optionally pass value)
invoke-03 value="5":
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/03-invoke.ts {{value}}

# Run worker for demo 04 (cron scheduling)
worker-04:
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/04-worker.ts

# Invoke demo 04 task (manual trigger)
invoke-04:
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/04-invoke.ts

# Run worker for demo 05 (dynamic cron scheduling)
worker-05:
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/05-worker.ts

# Invoke demo 05 scheduler (schedule or unschedule)
invoke-05 action="schedule":
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/05-invoke.ts {{action}}

# Run worker for demo 06 (multi-worker scaling)
worker-06:
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/06-worker.ts

# Invoke demo 06 tasks (optionally pass count)
invoke-06 count="10":
    cd {{invocation_directory()}} && bun run packages/pgconductor-js/demo/06-invoke.ts {{count}}

# Run all workers in sequence (useful for quick testing)
all-workers:
    #!/usr/bin/env bash
    cd {{invocation_directory()}}
    echo "Note: Run each worker in a separate terminal for real usage"
    echo "This is just for testing that they start correctly"
    echo ""
    echo "Testing demo 01..."
    timeout 2 bun run packages/pgconductor-js/demo/01-worker.ts || true
    echo "✓ Demo 01 worker starts"
    echo ""
    echo "Testing demo 02..."
    timeout 2 bun run packages/pgconductor-js/demo/02-worker.ts || true
    echo "✓ Demo 02 worker starts"
    echo ""
    echo "Testing demo 03..."
    timeout 2 bun run packages/pgconductor-js/demo/03-worker.ts || true
    echo "✓ Demo 03 worker starts"
    echo ""
    echo "Testing demo 04..."
    timeout 2 bun run packages/pgconductor-js/demo/04-worker.ts || true
    echo "✓ Demo 04 worker starts"
    echo ""
    echo "Testing demo 05..."
    timeout 2 bun run packages/pgconductor-js/demo/05-worker.ts || true
    echo "✓ Demo 05 worker starts"
    echo ""
    echo "Testing demo 06..."
    timeout 2 bun run packages/pgconductor-js/demo/06-worker.ts || true
    echo "✓ Demo 06 worker starts"

# Quick demo flow: start worker in background and invoke tasks
quick:
    #!/usr/bin/env bash
    cd {{invocation_directory()}}
    echo "Starting demo 01 worker in background..."
    bun run packages/pgconductor-js/demo/01-worker.ts & echo $$! > /tmp/pgconductor-demo-01.pid
    sleep 2
    echo "Invoking tasks..."
    bun run packages/pgconductor-js/demo/01-invoke.ts World
    bun run packages/pgconductor-js/demo/01-invoke.ts Alice
    bun run packages/pgconductor-js/demo/01-invoke.ts Bob
    sleep 2
    echo "Stopping worker..."
    kill `cat /tmp/pgconductor-demo-01.pid` 2>/dev/null || true
    rm -f /tmp/pgconductor-demo-01.pid
    echo "✓ Demo 01 complete"

# Setup demo infrastructure and environment
setup:
    @echo "Setting up demo infrastructure..."
    @just demo up
    @echo ""
    @echo "✓ Infrastructure started (postgres on port 5432)"
    @echo ""
    @echo "Export environment variable:"
    @echo "  export AGENT=1"
    @echo ""
    @echo "Note: DATABASE_URL defaults to postgres://postgres:postgres@localhost:5432/postgres"

# Print environment variables for sourcing
env:
    @echo "export AGENT=1"

# Interactive demo guide
guide:
    @echo "╔══════════════════════════════════════════════════════════════╗"
    @echo "║         pgconductor Demo Guide                              ║"
    @echo "╚══════════════════════════════════════════════════════════════╝"
    @echo ""
    @echo "Setup:"
    @echo "  just demo setup                    # Start infrastructure"
    @echo "  export AGENT=1                     # Required environment variable"
    @echo ""
    @echo "Infrastructure commands:"
    @echo "  just demo up                       # Start postgres"
    @echo "  just demo down                     # Stop infrastructure"
    @echo "  just demo restart                  # Rebuild and restart"
    @echo "  just demo logs [service]           # View logs"
    @echo "  just demo status                   # Check status"
    @echo ""
    @echo "Note: DATABASE_URL defaults to postgres://postgres:postgres@localhost:5432/postgres"
    @echo ""
    @echo "Usage: just demo <command>"
    @echo ""
    @echo "Demo 01 - Basic Task Execution"
    @echo "  Terminal 1: just demo worker-01"
    @echo "  Terminal 2: just demo invoke-01 [name]"
    @echo ""
    @echo "Demo 02 - Steps & Sleep"
    @echo "  Terminal 1: just demo worker-02"
    @echo "  Terminal 2: just demo invoke-02 [items...]"
    @echo ""
    @echo "Demo 03 - Child Task Invocation"
    @echo "  Terminal 1: just demo worker-03"
    @echo "  Terminal 2: just demo invoke-03 [value]"
    @echo ""
    @echo "Demo 04 - Cron Scheduling"
    @echo "  Terminal 1: just demo worker-04"
    @echo "  Terminal 2: just demo invoke-04 (optional)"
    @echo ""
    @echo "Demo 05 - Dynamic Cron Scheduling"
    @echo "  Terminal 1: just demo worker-05"
    @echo "  Terminal 2: just demo invoke-05 schedule"
    @echo "  Terminal 2: just demo invoke-05 unschedule"
    @echo ""
    @echo "Demo 06 - Multi-Worker Scaling"
    @echo "  Terminal 1: just demo worker-06"
    @echo "  Terminal 2: just demo invoke-06 [count]"
    @echo ""
    @echo "Quick test: just demo quick"
    @echo ""

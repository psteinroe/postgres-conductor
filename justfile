_default:
  just --list -u

alias f := format
alias l := lint
alias lf := lint-fix
alias r := ready
alias t := test

build-migrations:
    sh ./scripts/build-migrations.sh

lint:
    bun run oxlint --type-aware --deny-warnings
    (cd packages/pgconductor-js && bun run typecheck)

test:
    (cd packages/pgconductor-js && bun run test)

test-unit:
    (cd packages/pgconductor-js && bun run test:unit)

test-integration:
    (cd packages/pgconductor-js && bun run test:integration)

lint-fix:
    bun run oxlint --type-aware --fix --deny-warnings

format:
    bun run oxfmt

ready:
    just lint
    just format

docs:
    uv run zensical serve



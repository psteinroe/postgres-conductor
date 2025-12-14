_default:
  just --list -u

alias f := format
alias l := lint
alias lf := lint-fix
alias r := ready

mod demo "pgconductor-js/demo/demo.just"

build-migrations:
    sh ./scripts/build-migrations.sh

lint:
    bun run oxlint --type-aware --deny-warnings

lint-fix:
    bun run oxlint --type-aware --fix --deny-warnings

format:
    bun run oxfmt

ready:
    just lint
    just format

docs:
    uv run zensical serve



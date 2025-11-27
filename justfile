_default:
  just --list -u

build-migrations:
    sh ./scripts/build-migrations.sh

lint:
    bun run oxlint --type-aware --deny-warnings
    cargo clippy

lint-fix:
    bun run oxlint --type-aware --fix --deny-warnings
    cargo clippy --fix

format:
    bun run oxfmt
    cargo fmt



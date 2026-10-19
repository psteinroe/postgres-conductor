_default:
  just --list -u

alias f := format
alias l := lint
alias lf := lint-fix
alias r := ready
alias t := test

build-migrations:
    bash ./scripts/build-migrations.sh

lint:
    pnpm exec oxlint --type-aware --deny-warnings
    (cd packages/pgconductor-js && pnpm run typecheck)

test:
    (cd packages/pgconductor-js && pnpm run test)

test-unit:
    (cd packages/pgconductor-js && pnpm run test:unit)

test-integration:
    (cd packages/pgconductor-js && pnpm run test:integration)

lint-fix:
    pnpm exec oxlint --type-aware --fix --deny-warnings

format:
    pnpm exec oxfmt

ready:
    just lint
    just format

[working-directory: 'docs']
docs:
    uv run zensical serve

clear-branches:
    git branch --merged | egrep -v "(^\\*|main)" | xargs git branch -d

reset-git:
    git checkout main
    git pull
    just clear-branches



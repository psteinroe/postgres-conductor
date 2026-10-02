#!/usr/bin/env bash

# Packs pgconductor-js, installs the tarball with npm into a temporary project
# and checks that it works for a plain Node consumer: ESM imports, TypeScript
# types and the CLI. Bun is only used to build and pack.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JS_DIR="$SCRIPT_DIR/../packages/pgconductor-js"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

(cd "$JS_DIR" && bun run build && bun pm pack --destination "$WORK_DIR" --quiet)

cd "$WORK_DIR"

cat > package.json <<'EOF'
{ "name": "consumer", "private": true, "type": "module" }
EOF

npm install --no-audit --no-fund ./pgconductor-js-*.tgz @opentelemetry/api typescript@5 @types/node

if grep -q '"catalog:\|"workspace:' node_modules/pgconductor-js/package.json; then
	echo "Packed package.json contains unresolved catalog: or workspace: specifiers"
	exit 1
fi

cat > exports.mjs <<'EOF'
import * as pgconductor from "pgconductor-js";

const expected = [
	"Conductor",
	"Orchestrator",
	"Worker",
	"Task",
	"TaskSchemas",
	"EventSchemas",
	"defineTask",
	"defineEvent",
	"parseDuration",
	"SchemaManager",
	"MigrationStore",
	"WaitForEventTimeoutError",
];

const missing = expected.filter((name) => pgconductor[name] === undefined);
if (missing.length > 0) {
	console.error(`Missing exports: ${missing.join(", ")}`);
	process.exit(1);
}
EOF

node exports.mjs

cat > tsconfig.json <<'EOF'
{
	"compilerOptions": {
		"target": "ES2022",
		"module": "NodeNext",
		"moduleResolution": "NodeNext",
		"strict": true,
		"noEmit": true,
		"skipLibCheck": true,
		"types": ["node"]
	},
	"files": ["consumer.ts"]
}
EOF

cat > consumer.ts <<'EOF'
import {
	Conductor,
	Orchestrator,
	TaskSchemas,
	EventSchemas,
	defineEvent,
	WaitForEventTimeoutError,
	type DefineTask,
	type Logger,
	type TaskContext,
} from "pgconductor-js";

type Greet = DefineTask<{
	name: "greet";
	payload: { name: string };
	returns: { greeting: string };
}>;

class ConsoleLogger implements Logger {
	info(message: string, ...args: unknown[]) {
		console.info(message, ...args);
	}
	error(message: string, ...args: unknown[]) {
		console.error(message, ...args);
	}
	debug(message: string, ...args: unknown[]) {
		console.debug(message, ...args);
	}
	warn(message: string, ...args: unknown[]) {
		console.warn(message, ...args);
	}
}

const conductor = Conductor.create({
	connectionString: "postgres://localhost:5432/postgres",
	tasks: TaskSchemas.fromUnion<Greet>(),
	events: EventSchemas.fromSchema([]),
	context: {},
	logger: new ConsoleLogger(),
});

const greet = conductor.createTask({ name: "greet" }, { invocable: true }, async (event, ctx) => {
	const greeting: string = await ctx.step("build", () => `Hello, ${event.payload.name}`);
	return { greeting };
});

Orchestrator.create({ conductor, tasks: [greet] });

export async function invokeGreet() {
	await conductor.invoke({ name: "greet" }, { name: "Ada" });
	// @ts-expect-error payload must match the task definition
	await conductor.invoke({ name: "greet" }, { name: 1 });
}

export function isTimeout(err: unknown): err is WaitForEventTimeoutError {
	return err instanceof WaitForEventTimeoutError;
}

export type Context = TaskContext;
export const unused = defineEvent;
EOF

npx tsc -p tsconfig.json

npx pgconductor --help

// Baseline for the SQLite prototype (../sqlite-adapter/stress.ts): claim 50 -> settle 50 completed
// against the real Postgres claim and settlement statements, with P concurrent workers.
//
// Mode "single" is today's design: one statement per claim and per settle.
// Mode "interactive" models a storage-agnostic TS core on Postgres: the settle runs inside an
// interactive transaction that first locks the claimed rows and then spends EXTRA_ROUND_TRIPS
// further round trips (standing in for the sequential statements that replace the CTE chain)
// before the real settle statement, so row locks are held across those round trips.
import postgres from "postgres";
import { DatabaseClient } from "../../src/database-client";
import { QueryBuilder } from "../../src/query-builder";
import { SchemaManager } from "../../src/schema-manager";
import { DefaultLogger } from "../../src/lib/logger";
import { TestDatabasePool } from "../../tests/fixtures/test-database";

const EXECUTIONS = 20_000;
const BATCH = 50;
const EXTRA_ROUND_TRIPS = 13;

type Mode = "single" | "interactive";

const pool = await TestDatabasePool.create();

async function measureRoundTrip(url: string): Promise<number> {
	const sql = postgres(url, { max: 1 });
	await sql`select 1`;
	const n = 2000;
	const start = performance.now();
	for (let i = 0; i < n; i++) await sql`select 1`;
	const ms = (performance.now() - start) / n;
	await sql.end();
	return ms;
}

async function run(workers: number, mode: Mode) {
	const db = await pool.child();
	const sql = postgres(db.url, { max: workers + 1 });
	const client = new DatabaseClient({ sql, logger: new DefaultLogger() });

	await new SchemaManager(client).ensureLatest(new AbortController().signal);
	await client.registerWorker({
		queueName: "default",
		taskSpecs: [{ key: "t", queue: "default" }],
		cronSchedules: [],
		eventSubscriptions: [],
	});
	await sql`
		insert into pgconductor._private_executions (task_key, queue)
		select 't', 'default' from generate_series(1, ${EXECUTIONS}::int)
	`;

	const settleLatencies: number[] = [];

	const worker = async () => {
		const orchestratorId = crypto.randomUUID();
		let done = 0;
		while (true) {
			const rows = await client.getExecutions({
				orchestratorId,
				queueName: "default",
				batchSize: BATCH,
				taskKeys: ["t"],
			});
			if (rows.length === 0) return done;

			const grouped = {
				orchestratorId,
				completed: rows.map((row) => ({
					execution_id: row.id,
					queue: row.queue,
					task_key: row.task_key,
					status: "completed" as const,
				})),
				failed: [],
				released: [],
				invokeChild: [],
			};

			const start = performance.now();
			if (mode === "single") {
				await client.returnExecutions(grouped);
			} else {
				await sql.begin(async (tx) => {
					await tx`
						select 1 from pgconductor._private_executions
						where id = any(${rows.map((row) => row.id)}::uuid[]) and queue = 'default'
						for update
					`;
					for (let i = 0; i < EXTRA_ROUND_TRIPS; i++) await tx`select 1`;
					const query = new QueryBuilder(tx).buildReturnExecutions(grouped);
					if (query) await query;
				});
			}
			settleLatencies.push(performance.now() - start);
			done += rows.length;
		}
	};

	const start = performance.now();
	const counts = await Promise.all(Array.from({ length: workers }, worker));
	const seconds = (performance.now() - start) / 1000;

	const [check] = await sql<{ completed: number; attempts_ok: boolean }[]>`
		select count(*) filter (where completed_at is not null)::int as completed,
			bool_and(attempts = 1) as attempts_ok
		from pgconductor._private_executions
	`;
	settleLatencies.sort((a, b) => a - b);
	const p50 = settleLatencies[Math.floor(settleLatencies.length / 2)] || 0;
	await sql.end();

	return {
		workers,
		mode,
		executionsPerSecond: Math.round(EXECUTIONS / seconds),
		seconds: Number(seconds.toFixed(3)),
		settleP50Ms: Number(p50.toFixed(2)),
		claimedTotal: counts.reduce((a, b) => a + b, 0),
		completed: check?.completed,
		attemptsOk: check?.attempts_ok,
	};
}

try {
	const probe = await pool.child();
	const roundTripMs = await measureRoundTrip(probe.url);
	console.log(JSON.stringify({ roundTripMs: Number(roundTripMs.toFixed(3)) }));
	for (const mode of ["single", "interactive"] as const) {
		for (const workers of [1, 2, 4, 8]) {
			console.log(JSON.stringify(await run(workers, mode)));
		}
	}
} finally {
	await pool.destroy();
}

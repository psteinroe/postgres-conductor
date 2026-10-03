import { appendFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ref, SqliteStore } from "./store";

// Run with /tmp bind-mounted to .runs (see README) to respect the write scope.
const count = 20_000;
const deadline = Date.now() + 150_000;
function isBusy(error: unknown): boolean {
	return (
		error instanceof Error &&
		(("code" in error && error.code === "SQLITE_BUSY") ||
			error.message.includes("database is locked"))
	);
}
function expired() {
	if (Date.now() > deadline) throw new Error("stress deadline exceeded");
}
async function worker(path: string, log: string, synchronous: "NORMAL" | "FULL") {
	let busy = 0;
	async function retry<T>(fn: () => T): Promise<T> {
		for (;;) {
			expired();
			try {
				return fn();
			} catch (error) {
				if (isBusy(error) === false) throw error;
				busy += 1;
				await Bun.sleep(2);
			}
		}
	}
	const store = await retry(
		() => new SqliteStore(path, { initialize: false, synchronous, checkLimits: true }),
	);
	const orchestratorId = Bun.randomUUIDv7();
	let claimed = 0;
	let activeMax = 0;
	let claimStatements = 0;
	let settleStatements = 0;
	writeFileSync(log, "");
	for (;;) {
		const rows = await retry(() =>
			store.getExecutions({
				orchestratorId,
				queue: "default",
				batchSize: 50,
				taskKeys: ["ordinary", "limited"],
				now: Date.now(),
			}),
		);
		claimStatements = Math.max(claimStatements, store.lastStatementCount);
		activeMax = Math.max(activeMax, store.lastActiveMax);
		if (rows.length === 0) {
			const pending = store.db
				.query<{ n: number }, []>("select count(*) as n from executions where completed_at is null")
				.get();
			if (pending?.n === 0) break;
			expired();
			await Bun.sleep(2);
			continue;
		}
		appendFileSync(log, rows.map((e) => e.id).join("\n") + "\n");
		claimed += rows.length;
		// Yield between claim and settlement so different processes can overlap.
		await Bun.sleep(0);
		await retry(() =>
			store.returnExecutions({
				orchestratorId,
				completed: rows.map((e) => ({ ...ref(e), result: true })),
				now: Date.now(),
			}),
		);
		settleStatements = Math.max(settleStatements, store.lastStatementCount);
	}
	store.close();
	console.log(JSON.stringify({ claimed, busy, activeMax, claimStatements, settleStatements }));
}

type WorkerResult = {
	claimed: number;
	busy: number;
	activeMax: number;
	claimStatements: number;
	settleStatements: number;
};
async function benchmark(p: number, synchronous: "NORMAL" | "FULL", root: string) {
	expired();
	const directory = join(root, `${p}-${synchronous}`);
	mkdirSync(directory, { recursive: true });
	const path = join(directory, "stress.sqlite");
	const store = new SqliteStore(path, { synchronous });
	store.registerTask({ key: "ordinary" });
	store.registerTask({ key: "limited", concurrency_limit: 5 });
	const ids = new Set<string>();
	const now = Date.now();
	store.db
		.transaction(() => {
			const insert = store.db.query(
				"insert into executions (id, task_key, run_at, created_at) values (?, ?, ?, ?)",
			);
			for (let i = 0; i < count; i += 1) {
				const id = Bun.randomUUIDv7();
				ids.add(id);
				insert.run(id, i % 10 === 0 ? "limited" : "ordinary", now, now);
			}
		})
		.immediate();
	const start = performance.now();
	const children = [];
	for (let i = 0; i < p; i += 1) {
		const log = join(directory, `${i}.log`);
		const process = Bun.spawn(
			[Bun.which("bun") || "bun", import.meta.path, "worker", path, log, synchronous],
			{
				env: { ...Bun.env, AGENT: "1" },
				stdout: "pipe",
				stderr: "pipe",
			},
		);
		children.push({ process, log });
	}
	const reports = await Promise.all(
		children.map(async ({ process }) => {
			const stdout = new Response(process.stdout).text();
			const stderr = new Response(process.stderr).text();
			const code = await process.exited;
			if (code !== 0) throw new Error(`worker exited ${code}: ${await stderr}`);
			return JSON.parse(await stdout) as WorkerResult;
		}),
	);
	const seconds = (performance.now() - start) / 1000;
	const seen = new Set<string>();
	let duplicate = 0;
	for (const { log } of children) {
		const contents = readFileSync(log, "utf8").trim();
		for (const id of contents === "" ? [] : contents.split("\n")) {
			if (seen.has(id)) duplicate += 1;
			if (ids.has(id) === false) throw new Error(`unexpected claimed id ${id}`);
			seen.add(id);
		}
	}
	const verification = store.db
		.query<{ completed: number; once: number; pending: number }, []>(`select
		sum(completed_at is not null) as completed, sum(attempts = 1) as once,
		sum(completed_at is null or failed_at is not null or locked_at is not null) as pending from executions`)
		.get();
	const activeMax = Math.max(...reports.map((r) => r.activeMax));
	if (
		seen.size !== count ||
		duplicate !== 0 ||
		verification?.completed !== count ||
		verification.once !== count ||
		verification.pending !== 0 ||
		activeMax > 5
	) {
		throw new Error(
			`verification failed: ${JSON.stringify({ seen: seen.size, duplicate, verification, activeMax })}`,
		);
	}
	const result = {
		processes: p,
		synchronous,
		executions: count,
		seconds: +seconds.toFixed(3),
		executionsPerSecond: Math.round(count / seconds),
		busyErrors: reports.reduce((sum, r) => sum + r.busy, 0),
		activeMax,
		duplicateClaims: duplicate,
		completedExactlyOnce: verification.once,
		claimStatements: Math.max(...reports.map((r) => r.claimStatements)),
		settleStatements: Math.max(...reports.map((r) => r.settleStatements)),
	};
	store.close();
	// Only this run's database/WAL/log files, under the scoped bind mount, are removed.
	rmSync(directory, { recursive: true });
	return result;
}

if (Bun.argv[2] === "worker") {
	const path = Bun.argv[3];
	const log = Bun.argv[4];
	if (path === undefined || log === undefined) throw new Error("missing worker paths");
	await worker(path, log, Bun.argv[5] === "FULL" ? "FULL" : "NORMAL");
} else {
	const root = Bun.env.SQLITE_STRESS_DIR || join(import.meta.dir, ".runs");
	mkdirSync(root, { recursive: true });
	const results = [];
	for (const p of [1, 2, 4, 8]) results.push(await benchmark(p, "NORMAL", root));
	results.push(await benchmark(1, "FULL", root));
	console.log(
		JSON.stringify(
			{
				bun: Bun.version,
				executionsPerRun: count,
				limitedFraction: 0.1,
				elapsedSeconds: +((Date.now() - deadline + 150_000) / 1000).toFixed(3),
				results,
			},
			null,
			2,
		),
	);
}

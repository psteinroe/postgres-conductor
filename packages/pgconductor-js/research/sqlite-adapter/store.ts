import { Database } from "bun:sqlite";
import { readFileSync } from "node:fs";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type Bind = string | number | null;
export const INFINITY = Number.MAX_SAFE_INTEGER;
const BACKOFF = [15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200];
const encode = (value: Json | undefined): string | null =>
	value === undefined || value === null ? null : JSON.stringify(value);
const order = "priority asc, run_at asc, created_at asc, id asc";

export type Task = {
	key: string;
	queue: string;
	concurrency_limit: number | null;
	group_concurrency_limit: number | null;
	max_attempts: number;
	remove_on_complete_days: number | null;
	remove_on_fail_days: number | null;
};
export type Execution = {
	id: string;
	task_key: string;
	queue: string;
	payload: string | null;
	result: string | null;
	group: string | null;
	dedupe_key: string | null;
	singleton_on: number | null;
	priority: number;
	run_at: number;
	created_at: number;
	attempts: number;
	locked_by: string | null;
	locked_at: number | null;
	failed_at: number | null;
	completed_at: number | null;
	last_error: string | null;
	cancelled: number;
	waiting_on_execution_id: string | null;
	waiting_step_key: string | null;
	parent_execution_id: string | null;
	subscription_id: string | null;
};
export type InvokeSpec = {
	taskKey: string;
	queue?: string;
	payload?: Json;
	dedupeKey?: string;
	dedupeSeconds?: number;
	dedupeNextSlot?: boolean;
	runAt?: number;
	priority?: number;
	group?: string;
	traceContext?: Json;
	metadata?: Json;
	cronExpression?: string;
	now: number;
};
export type Ref = { id: string; queue: string; taskKey: string };
export const ref = (e: Execution): Ref => ({ id: e.id, queue: e.queue, taskKey: e.task_key });
type Completed = Ref & { result?: Json };
type Failed = Ref & { error?: string; permanent?: boolean };
type Released = Ref & { stepKey?: string; rescheduleInMs?: number | "infinity" };
type InvokeChild = Ref & {
	stepKey: string;
	child: Omit<InvokeSpec, "now" | "dedupeKey" | "dedupeSeconds" | "dedupeNextSlot">;
	timeoutMs?: number | "infinity";
};
export type Settlement = {
	orchestratorId: string;
	completed?: Completed[];
	failed?: Failed[];
	released?: Released[];
	invokeChild?: InvokeChild[];
	now: number;
};
type Input = (Completed | Failed | Released | InvokeChild) & { status: string };
type Valid = Execution & {
	ordinal: number;
	max_attempts: number | null;
	remove_on_complete_days: number | null;
	remove_on_fail_days: number | null;
};
type Terminal = {
	id: string;
	queue: string;
	error: string;
	remove: boolean;
	subscription_id: string | null;
};

/** Throwaway evidence, not an implementation of the public DatabaseClient interface.
 * All read/modify/write workflows serialize on SQLite's single writer. No awaits
 * are allowed inside transactions. JSON is encoded text; time comes from callers.
 */
export class SqliteStore {
	readonly db: Database;
	lastStatementCount = 0;
	lastActiveMax = 0;
	private statementCount = 0;
	private readonly checkLimits: boolean;

	constructor(
		path = ":memory:",
		options: { initialize?: boolean; synchronous?: "NORMAL" | "FULL"; checkLimits?: boolean } = {},
	) {
		this.db = new Database(path, { create: true, strict: true });
		this.checkLimits = options.checkLimits || false;
		try {
			this.db.exec("pragma busy_timeout = 1000");
			this.db.exec("pragma foreign_keys = on");
			this.db.exec("pragma journal_mode = wal");
			this.db.exec(`pragma synchronous = ${(options.synchronous || "NORMAL").toLowerCase()}`);
			if (options.initialize === false) return;
			this.db
				.transaction(() =>
					this.db.exec(readFileSync(new URL("schema.sql", import.meta.url), "utf8")),
				)
				.immediate();
		} catch (error) {
			this.db.close();
			throw error;
		}
	}

	close() {
		this.db.close();
	}
	private all<T>(sql: string, ...args: Bind[]): T[] {
		this.statementCount += 1;
		return this.db.query<T, Bind[]>(sql).all(...args);
	}
	private run(sql: string, ...args: Bind[]) {
		this.statementCount += 1;
		return this.db.query(sql).run(...args);
	}
	private atomic<T>(fn: () => T): T {
		this.statementCount = 0;
		try {
			return this.db.transaction(fn).immediate();
		} finally {
			this.lastStatementCount = this.statementCount;
		}
	}

	registerTask(spec: Partial<Task> & { key: string }) {
		this.atomic(() =>
			this.run(
				`insert into tasks
			(key, queue, concurrency_limit, group_concurrency_limit, max_attempts, remove_on_complete_days, remove_on_fail_days)
			values (?, ?, ?, ?, ?, ?, ?) on conflict (key, queue) do update set
			concurrency_limit = excluded.concurrency_limit, group_concurrency_limit = excluded.group_concurrency_limit,
			max_attempts = excluded.max_attempts, remove_on_complete_days = excluded.remove_on_complete_days,
			remove_on_fail_days = excluded.remove_on_fail_days`,
				spec.key,
				spec.queue || "default",
				spec.concurrency_limit === undefined ? null : spec.concurrency_limit,
				spec.group_concurrency_limit === undefined ? null : spec.group_concurrency_limit,
				spec.max_attempts === undefined ? 3 : spec.max_attempts,
				spec.remove_on_complete_days === undefined ? null : spec.remove_on_complete_days,
				spec.remove_on_fail_days === undefined ? null : spec.remove_on_fail_days,
			),
		);
	}

	invoke(spec: InvokeSpec): string | null {
		return this.atomic(() => {
			const queue = spec.queue || "default";
			// NULL keys never conflict in the regular unique index.
			if (spec.dedupeKey !== undefined) {
				this.run(
					`delete from executions where task_key = ? and queue = ? and dedupe_key = ?
					and locked_at is not null and exists (select 1 from tasks where key = ? and queue = ? and remove_on_fail_days = 0)`,
					spec.taskKey,
					queue,
					spec.dedupeKey,
					spec.taskKey,
					queue,
				);
				this.run(
					`update executions set dedupe_key = null, locked_by = null, locked_at = null,
					failed_at = ?, last_error = 'superseded by reinvoke'
					where task_key = ? and queue = ? and dedupe_key = ? and locked_at is not null`,
					spec.now,
					spec.taskKey,
					queue,
					spec.dedupeKey,
				);
			}
			let slot: number | null = null;
			let runAt = spec.runAt === undefined ? spec.now : spec.runAt;
			let conflict = "(task_key, dedupe_key, queue)";
			let action = `do update set payload = excluded.payload, trace_context = excluded.trace_context,
				metadata = excluded.metadata, run_at = excluded.run_at, priority = excluded.priority,
				cron_expression = excluded.cron_expression, "group" = excluded."group"`;
			if (spec.dedupeSeconds === undefined) action += " where executions.locked_at is null";
			else {
				if (spec.dedupeSeconds <= 0) throw new Error("dedupeSeconds must be positive");
				const width = spec.dedupeSeconds * 1000;
				slot = Math.floor(spec.now / width) * width;
				if (spec.dedupeNextSlot) {
					slot += width;
					runAt = slot;
				} else action = "do nothing";
				conflict = `(task_key, singleton_on, coalesce(dedupe_key, ''), queue)
					where singleton_on is not null and completed_at is null and failed_at is null and cancelled = false`;
			}
			return (
				this.all<{ id: string }>(
					`insert into executions
				(id, task_key, queue, payload, trace_context, metadata, run_at, created_at, dedupe_key,
				singleton_on, cron_expression, priority, "group") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				on conflict ${conflict} ${action} returning id`,
					Bun.randomUUIDv7(),
					spec.taskKey,
					queue,
					encode(spec.payload),
					encode(spec.traceContext),
					encode(spec.metadata),
					runAt,
					spec.now,
					spec.dedupeKey === undefined ? null : spec.dedupeKey,
					slot,
					spec.cronExpression || null,
					spec.priority || 0,
					spec.group === undefined ? null : spec.group,
				)[0]?.id || null
			);
		});
	}

	getExecutions(args: {
		orchestratorId: string;
		queue: string;
		batchSize: number;
		taskKeys: string[];
		now: number;
	}): Execution[] {
		return this.atomic(() => {
			if (Number.isInteger(args.batchSize) === false || args.batchSize < 0)
				throw new Error("invalid batchSize");
			const tasks = this.all<Task>("select * from tasks where queue = ?", args.queue);
			const enabled = tasks.some(
				(t) => t.concurrency_limit !== null || t.group_concurrency_limit !== null,
			);
			let rows: Execution[];
			if (enabled === false) {
				rows = this.all<Execution>(
					`with candidates as (
					select id from executions where queue = ? and is_available = true and run_at <= ?
					and task_key in (select value from json_each(?)) order by ${order} limit ?)
					update executions set attempts = attempts + 1, locked_by = ?, locked_at = ?,
					waiting_step_key = case when waiting_on_execution_id is null then null else waiting_step_key end
					where queue = ? and id in (select id from candidates) returning *`,
					args.queue,
					args.now,
					JSON.stringify(args.taskKeys),
					args.batchSize,
					args.orchestratorId,
					args.now,
					args.queue,
				);
			} else {
				const active = this.all<{ task_key: string; group: string | null; n: number }>(
					`select task_key, "group", count(*) as n
					from executions where queue = ? and locked_at is not null and failed_at is null and completed_at is null
					group by task_key, "group"`,
					args.queue,
				);
				const candidates: Execution[] = [];
				for (const task of tasks.filter((t) => args.taskKeys.includes(t.key))) {
					const groups = active.filter((a) => a.task_key === task.key);
					const activeCount = groups.reduce((n, g) => n + g.n, 0);
					const capacity =
						task.concurrency_limit === null
							? args.batchSize
							: Math.max(0, task.concurrency_limit - activeCount);
					if (capacity === 0) continue;
					const fullGroups = groups
						.filter(
							(g) =>
								g.group !== null &&
								task.group_concurrency_limit !== null &&
								g.n >= task.group_concurrency_limit,
						)
						.map((g) => g.group);
					candidates.push(
						...this.all<Execution>(
							`select * from executions where queue = ? and task_key = ?
						and is_available = true and run_at <= ? and ("group" is null or "group" not in (select value from json_each(?)))
						order by ${order} limit ?`,
							args.queue,
							task.key,
							args.now,
							JSON.stringify(fullGroups),
							Math.min(args.batchSize, capacity),
						),
					);
				}
				const counts = new Map(active.map((a) => [JSON.stringify([a.task_key, a.group]), a.n]));
				const selected = candidates
					.sort(compare)
					.slice(0, args.batchSize)
					.filter((e) => {
						const task = tasks.find((t) => t.key === e.task_key);
						if (task === undefined || e.group === null || task.group_concurrency_limit === null)
							return true;
						const key = JSON.stringify([e.task_key, e.group]);
						const count = counts.get(key) || 0;
						counts.set(key, count + 1);
						return count < task.group_concurrency_limit;
					});
				rows =
					selected.length === 0
						? []
						: this.all<Execution>(
								`update executions set attempts = attempts + 1,
					locked_by = ?, locked_at = ?, waiting_step_key = case when waiting_on_execution_id is null
					then null else waiting_step_key end where queue = ? and id in (select value from json_each(?)) returning *`,
								args.orchestratorId,
								args.now,
								args.queue,
								JSON.stringify(selected.map((e) => e.id)),
							);
			}
			if (this.checkLimits) {
				// Checking while holding the writer lock observes every post-claim state, not a sample.
				const active = this.all<{ key: string; group: string | null; n: number }>(
					`select task_key as key, "group", count(*) as n
					from executions where queue = ? and locked_at is not null and failed_at is null and completed_at is null
					group by task_key, "group"`,
					args.queue,
				);
				this.lastActiveMax = 0;
				for (const t of tasks) {
					const groups = active.filter((a) => a.key === t.key);
					const n = groups.reduce((sum, g) => sum + g.n, 0);
					if (t.concurrency_limit !== null) {
						this.lastActiveMax = Math.max(this.lastActiveMax, n);
						if (n > t.concurrency_limit) throw new Error("task concurrency violated");
					}
					const groupLimit = t.group_concurrency_limit;
					if (groupLimit !== null && groups.some((g) => g.group !== null && g.n > groupLimit))
						throw new Error("group concurrency violated");
				}
			}
			return rows.sort(compare);
		});
	}

	returnExecutions(args: Settlement) {
		return this.atomic(() => {
			const inputs: Input[] = [
				...(args.completed || []).map((r) => ({ ...r, status: "completed" })),
				...(args.failed || []).map((r) => ({ ...r, status: "failed" })),
				...(args.released || []).map((r) => ({ ...r, status: "released" })),
				...(args.invokeChild || []).map((r) => ({ ...r, status: "invoke_child" })),
			];
			if (inputs.length === 0) return;
			if (new Set(inputs.map((r) => JSON.stringify([r.id, r.queue]))).size !== inputs.length)
				throw new Error("duplicate settlement result");
			const valid = this.all<Valid>(
				`select e.*, cast(r.key as integer) as ordinal,
				t.max_attempts, t.remove_on_complete_days, t.remove_on_fail_days
				from json_each(?) r join executions e on e.id = json_extract(r.value, '$.id')
				and e.queue = json_extract(r.value, '$.queue') and e.task_key = json_extract(r.value, '$.taskKey')
				left join tasks t on t.key = e.task_key and t.queue = e.queue where e.locked_by = ?`,
				JSON.stringify(inputs),
				args.orchestratorId,
			);
			const completed: (Valid & { encodedResult: string | null })[] = [];
			const terminal: Terminal[] = [];
			const retries: { id: string; queue: string; error: string; runAt: number }[] = [];
			const released: (Released & { runAt: number })[] = [];
			const children: (InvokeChild & { childId: string; runAt: number })[] = [];
			for (const e of valid) {
				const input = inputs[e.ordinal];
				if (input === undefined) throw new Error("missing settlement input");
				if (e.cancelled === 1 || input.status === "failed") {
					if (e.max_attempts === null) continue;
					const r = input as Failed;
					const error = r.error || (e.cancelled === 1 ? e.last_error : null) || "unknown error";
					if (e.cancelled === 1 || r.permanent || e.attempts >= e.max_attempts)
						terminal.push({
							id: e.id,
							queue: e.queue,
							error,
							remove: e.remove_on_fail_days === 0,
							subscription_id: e.subscription_id,
						});
					else
						retries.push({
							id: e.id,
							queue: e.queue,
							error,
							runAt:
								Math.max(args.now, e.run_at) +
								(BACKOFF[Math.min(Math.max(e.attempts, 1), 10) - 1] || 7200) * 1000,
						});
				} else if (input.status === "completed") {
					if (e.max_attempts !== null)
						completed.push({ ...e, encodedResult: encode((input as Completed).result) });
				} else if (input.status === "released") {
					const r = input as Released;
					released.push({
						...r,
						runAt: r.rescheduleInMs === "infinity" ? INFINITY : args.now + (r.rescheduleInMs || 0),
					});
				} else {
					const r = input as InvokeChild;
					children.push({
						...r,
						childId: Bun.randomUUIDv7(),
						runAt:
							r.timeoutMs === "infinity" || r.timeoutMs === undefined
								? INFINITY
								: args.now + r.timeoutMs,
					});
				}
			}
			if (completed.length > 0) this.complete(completed, args.now);
			if (terminal.length > 0) this.fail(terminal, args.now);
			if (retries.length > 0)
				this.run(
					`update executions as e set last_error = json_extract(r.value, '$.error'),
				run_at = json_extract(r.value, '$.runAt'), locked_by = null, locked_at = null
				from json_each(?) r where e.id = json_extract(r.value, '$.id') and e.queue = json_extract(r.value, '$.queue')`,
					JSON.stringify(retries),
				);
			if (released.length > 0) {
				const steps = released
					.filter((r) => r.stepKey !== undefined)
					.map((r) => ({ id: r.id, queue: r.queue, key: r.stepKey, result: null }));
				if (steps.length > 0) this.insertSteps(steps, args.now);
				this.run(
					`update executions as e set attempts = max(attempts - 1, 0), run_at = json_extract(r.value, '$.runAt'),
					locked_by = null, locked_at = null from json_each(?) r
					where e.id = json_extract(r.value, '$.id') and e.queue = json_extract(r.value, '$.queue')`,
					JSON.stringify(released),
				);
			}
			if (children.length > 0) {
				const data = children.map((r) => ({
					...r,
					child: {
						...r.child,
						payload: encode(r.child.payload),
						traceContext: encode(r.child.traceContext),
						metadata: encode(r.child.metadata),
					},
				}));
				this.run(
					`insert into executions (id, task_key, queue, payload, trace_context, metadata, run_at, created_at, parent_execution_id, "group")
					select json_extract(value, '$.childId'), json_extract(value, '$.child.taskKey'),
					coalesce(json_extract(value, '$.child.queue'), 'default'), json_extract(value, '$.child.payload'),
					json_extract(value, '$.child.traceContext'), json_extract(value, '$.child.metadata'), ?, ?,
					json_extract(value, '$.id'), json_extract(value, '$.child.group') from json_each(?)`,
					args.now,
					args.now,
					JSON.stringify(data),
				);
				this.run(
					`update executions as e set waiting_on_execution_id = json_extract(r.value, '$.childId'),
					waiting_step_key = json_extract(r.value, '$.stepKey'), run_at = json_extract(r.value, '$.runAt'), locked_by = null, locked_at = null
					from json_each(?) r where e.id = json_extract(r.value, '$.id') and e.queue = json_extract(r.value, '$.queue')`,
					JSON.stringify(children),
				);
			}
		});
	}

	private complete(rows: (Valid & { encodedResult: string | null })[], now: number) {
		const parents = this.all<Execution>(
			`select * from executions where waiting_on_execution_id in
			(select json_extract(value, '$.id') from json_each(?) where json_extract(value, '$.subscription_id') is null)`,
			JSON.stringify(rows),
		);
		const eligible = parents.filter(
			(p) => p.completed_at === null && p.failed_at === null && p.locked_by === null,
		);
		const steps = eligible
			.filter((p) => p.waiting_step_key !== null)
			.map((p) => ({
				id: p.id,
				queue: p.queue,
				key: p.waiting_step_key,
				result: rows.find((r) => r.id === p.waiting_on_execution_id)?.encodedResult || null,
			}));
		if (steps.length > 0) this.insertSteps(steps, now);
		if (eligible.length > 0)
			this.run(
				`update executions as e set run_at = ?, waiting_on_execution_id = null,
			waiting_step_key = null, locked_by = null, locked_at = null from json_each(?) p
			where e.id = json_extract(p.value, '$.id') and e.queue = json_extract(p.value, '$.queue')`,
				now,
				JSON.stringify(eligible),
			);
		const orphans = rows.filter(
			(r) =>
				r.parent_execution_id !== null &&
				r.subscription_id === null &&
				parents.some((p) => p.waiting_on_execution_id === r.id) === false,
		);
		if (orphans.length > 0)
			this.run(
				`update executions as e set failed_at = ?, completed_at = null,
			last_error = 'Parent timed out before child completed', locked_by = null, locked_at = null
			from json_each(?) r where e.id = json_extract(r.value, '$.id') and e.queue = json_extract(r.value, '$.queue')`,
				now,
				JSON.stringify(orphans),
			);
		const ok = rows.filter((r) => orphans.includes(r) === false);
		const removed = ok.filter((r) => r.remove_on_complete_days === 0);
		const retained = ok.filter((r) => r.remove_on_complete_days !== 0);
		if (removed.length > 0) this.deleteRows(removed);
		if (retained.length > 0)
			this.run(
				`update executions as e set completed_at = ?, result = json_extract(r.value, '$.encodedResult'),
			locked_by = null, locked_at = null from json_each(?) r
			where e.id = json_extract(r.value, '$.id') and e.queue = json_extract(r.value, '$.queue')`,
				now,
				JSON.stringify(retained),
			);
	}

	private fail(roots: Terminal[], now: number) {
		// Read recursive ancestors once; number of SQL statements is independent of depth.
		const ancestors = this.all<Terminal>(
			`with recursive ancestors(id, error) as (
			select p.id, json_extract(r.value, '$.error') from json_each(?) r join executions p
			on p.waiting_on_execution_id = json_extract(r.value, '$.id')
			join tasks pt on pt.key = p.task_key and pt.queue = p.queue
			where json_extract(r.value, '$.subscription_id') is null and p.completed_at is null and p.failed_at is null and p.locked_by is null
			union all select p.id, a.error from ancestors a join executions p on p.waiting_on_execution_id = a.id
			join tasks pt on pt.key = p.task_key and pt.queue = p.queue
			where p.completed_at is null and p.failed_at is null and p.locked_by is null)
			select e.id, e.queue, 'Child execution failed: ' || a.error as error,
			coalesce(t.remove_on_fail_days = 0, false) as remove, e.subscription_id
			from ancestors a join executions e on e.id = a.id join tasks t on t.key = e.task_key and t.queue = e.queue`,
			JSON.stringify(roots),
		);
		const rows = [...roots, ...ancestors];
		const removed = rows.filter((r) => Boolean(r.remove));
		const retained = rows.filter((r) => Boolean(r.remove) === false);
		if (removed.length > 0) this.deleteRows(removed);
		if (retained.length > 0)
			this.run(
				`update executions as e set failed_at = ?, last_error = json_extract(r.value, '$.error'),
			waiting_on_execution_id = null, waiting_step_key = null, locked_by = null, locked_at = null
			from json_each(?) r where e.id = json_extract(r.value, '$.id') and e.queue = json_extract(r.value, '$.queue')`,
				now,
				JSON.stringify(retained),
			);
	}

	private deleteRows(rows: { id: string; queue: string }[]) {
		this.run(
			`delete from executions where exists (select 1 from json_each(?) r
			where executions.id = json_extract(r.value, '$.id') and executions.queue = json_extract(r.value, '$.queue'))`,
			JSON.stringify(rows),
		);
	}
	private insertSteps(
		rows: { id: string; queue: string; key: string | null | undefined; result: string | null }[],
		now: number,
	) {
		this.run(
			`insert into steps (execution_id, queue, key, result, created_at)
			select json_extract(value, '$.id'), json_extract(value, '$.queue'), json_extract(value, '$.key'), json_extract(value, '$.result'), ?
			from json_each(?) where true on conflict (execution_id, key) do nothing`,
			now,
			JSON.stringify(rows),
		);
	}

	saveStep(
		args: Ref & {
			orchestratorId: string;
			key: string;
			result: Json;
			now: number;
			runAtMs?: number;
		},
	) {
		return this.atomic(() => {
			const inserted = this.run(
				`insert into steps (execution_id, queue, key, result, created_at)
				select id, queue, ?, ?, ? from executions where id = ? and queue = ? and locked_by = ?
				on conflict (execution_id, key) do nothing`,
				args.key,
				encode(args.result),
				args.now,
				args.id,
				args.queue,
				args.orchestratorId,
			);
			if (inserted.changes > 0 && args.runAtMs)
				this.run(
					"update executions set run_at = ? where id = ? and queue = ? and locked_by = ?",
					args.now + args.runAtMs,
					args.id,
					args.queue,
					args.orchestratorId,
				);
			return inserted.changes;
		});
	}
	heartbeat(orchestratorId: string, now: number) {
		this.atomic(() =>
			this.run(
				`insert into orchestrators (id, last_heartbeat_at) values (?, ?)
			on conflict (id) do update set last_heartbeat_at = excluded.last_heartbeat_at`,
				orchestratorId,
				now,
			),
		);
	}
	recoverStaleOrchestrators(args: { now: number; maxAgeMs: number }) {
		return this.atomic(() => {
			const ids = this.all<{ id: string }>(
				"delete from orchestrators where last_heartbeat_at < ? returning id",
				args.now - args.maxAgeMs,
			);
			if (ids.length > 0)
				this.run(
					`update executions set locked_by = null, locked_at = null
				where locked_by in (select json_extract(value, '$.id') from json_each(?))`,
					JSON.stringify(ids),
				);
			return ids.length;
		});
	}
}

function compare(a: Execution, b: Execution): number {
	return (
		a.priority - b.priority ||
		a.run_at - b.run_at ||
		a.created_at - b.created_at ||
		(a.id < b.id ? -1 : a.id > b.id ? 1 : 0)
	);
}

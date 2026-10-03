import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { INFINITY, ref, SqliteStore, type Execution } from "./store";

let store: SqliteStore;
const now = 1_700_000_000_000;
beforeEach(() => {
	store = new SqliteStore();
	store.registerTask({ key: "task" });
});
afterEach(() => store.close());
const invoke = (extra = {}) => store.invoke({ taskKey: "task", now, ...extra });
const claim = (extra = {}) =>
	store.getExecutions({
		orchestratorId: "worker",
		queue: "default",
		batchSize: 50,
		taskKeys: ["task"],
		now,
		...extra,
	});
const settle = (completed: Execution[], at = now) =>
	store.returnExecutions({
		orchestratorId: "worker",
		completed: completed.map((e) => ({ ...ref(e), result: { ok: true } })),
		now: at,
	});
function row(id: string | null): Execution {
	const result = store.db
		.query<Execution, [string | null]>("select * from executions where id = ?")
		.get(id);
	if (result === null) throw new Error("missing execution");
	return result;
}
function first(rows: Execution[]): Execution {
	const e = rows[0];
	if (e === undefined) throw new Error("expected a claim");
	return e;
}
function childOf(parent: Execution): Execution {
	store.returnExecutions({
		orchestratorId: "worker",
		invokeChild: [
			{ ...ref(parent), stepKey: "child", child: { taskKey: "task" }, timeoutMs: "infinity" },
		],
		now,
	});
	return first(claim());
}

describe("SQLite serialized claim / settlement", () => {
	test("claims exactly once, ordered by priority/run_at/created_at/id; isolates queues and tasks", () => {
		const later = invoke({ runAt: now + 1 });
		const normal = invoke();
		const urgent = invoke({ priority: -1 });
		invoke({ queue: "other" });
		invoke({ taskKey: "other" });
		const rows = claim();
		expect(rows.map((e) => e.id)).toEqual([urgent || "", normal || ""]);
		expect(rows.map((e) => e.attempts)).toEqual([1, 1]);
		expect(claim({ orchestratorId: "second" })).toHaveLength(0);
		settle(rows);
		expect(claim({ now: now + 1 }).map((e) => e.id)).toEqual([later || ""]);
	});

	test("task concurrency is enforced across orchestrators and within a batch", () => {
		expect(() => store.registerTask({ key: "task", concurrency_limit: 0 })).toThrow();
		store.registerTask({ key: "task", concurrency_limit: 5 });
		for (let i = 0; i < 12; i += 1) invoke();
		const rows = claim();
		expect(rows).toHaveLength(5);
		expect(store.lastStatementCount).toBe(4);
		expect(claim({ orchestratorId: "second" })).toHaveLength(0);
		settle(rows.slice(0, 2));
		expect(claim({ orchestratorId: "second" })).toHaveLength(2);
	});

	test("group limits are per task, NULL groups are unlimited, existing active groups are excluded", () => {
		store.registerTask({ key: "task", group_concurrency_limit: 1 });
		store.registerTask({ key: "other", group_concurrency_limit: 1 });
		for (const group of ["a", "a", "b", "b"]) invoke({ group });
		invoke();
		invoke();
		invoke({ taskKey: "other", group: "a" });
		const rows = claim({ taskKeys: ["task", "other"] });
		expect(rows).toHaveLength(5);
		expect(rows.filter((e) => e.task_key === "task" && e.group === "a")).toHaveLength(1);
		expect(rows.filter((e) => e.task_key === "other" && e.group === "a")).toHaveLength(1);
		expect(claim({ orchestratorId: "second" })).toHaveLength(0);
		settle(rows);
		expect(claim()).toHaveLength(2);
	});

	test("retry uses all ten backoffs, capped at 7200s, relative to max(now, run_at)", () => {
		store.registerTask({ key: "task", max_attempts: 20 });
		const id = invoke();
		let at = now;
		for (const seconds of [15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200, 7200]) {
			const e = first(claim({ now: at }));
			store.saveStep({
				...ref(e),
				orchestratorId: "worker",
				key: `attempt-${e.attempts}`,
				result: null,
				now: at,
				runAtMs: 7,
			});
			store.returnExecutions({
				orchestratorId: "worker",
				failed: [{ ...ref(e), error: "retry" }],
				now: at,
			});
			at += seconds * 1000 + 7;
			expect(row(id).run_at).toBe(at);
			expect(row(id).failed_at).toBeNull();
			expect(claim({ now: at - 1 })).toHaveLength(0);
		}
	});

	test("max_attempts and explicit permanent failures are terminal", () => {
		store.registerTask({ key: "task", max_attempts: 1 });
		const a = invoke();
		const b = invoke();
		const rows = claim();
		store.returnExecutions({
			orchestratorId: "worker",
			failed: rows.map((e) => ({ ...ref(e), error: "boom" })),
			now,
		});
		expect(row(a).failed_at).toBe(now);
		expect(row(b).last_error).toBe("boom");
		store.registerTask({ key: "task", max_attempts: 10 });
		const c = invoke();
		store.returnExecutions({
			orchestratorId: "worker",
			failed: [{ ...ref(first(claim())), permanent: true }],
			now,
		});
		expect(row(c).last_error).toBe("unknown error");
		expect(claim()).toHaveLength(0);
		const cancelled = invoke();
		const active = claim();
		store.db
			.transaction(() =>
				store.db.run("update executions set cancelled = 1, last_error = 'cancelled' where id = ?", [
					cancelled,
				]),
			)
			.immediate();
		settle(active);
		expect(row(cancelled).failed_at).toBe(now);
		expect(row(cancelled).last_error).toBe("cancelled");
		expect(row(cancelled).completed_at).toBeNull();
	});

	test("child completion stores first parent step result and wakes an unlocked waiting parent", () => {
		const id = invoke();
		const parent = first(claim());
		const child = childOf(parent);
		expect(row(id).run_at).toBe(INFINITY);
		settle([child]);
		expect(store.lastStatementCount).toBe(5);
		expect(row(id).waiting_on_execution_id).toBeNull();
		expect(row(id).attempts).toBe(1);
		expect(row(child.id).completed_at).toBe(now);
		const step = store.db
			.query<{ result: string }, [string]>("select result from steps where execution_id = ?")
			.get(parent.id);
		expect(step?.result).toBe('{"ok":true}');
		expect(first(claim()).id).toBe(parent.id);
	});

	test("permanent child failure cascades through two parent levels with one error prefix each", () => {
		const id = invoke();
		const grandparent = first(claim());
		const parent = childOf(grandparent);
		const child = childOf(parent);
		store.returnExecutions({
			orchestratorId: "worker",
			failed: [{ ...ref(child), permanent: true, error: "leaf" }],
			now,
		});
		expect(store.lastStatementCount).toBe(3);
		for (const target of [id, parent.id]) {
			expect(row(target).failed_at).toBe(now);
			expect(row(target).last_error).toBe("Child execution failed: leaf");
			expect(row(target).waiting_on_execution_id).toBeNull();
		}
		expect(claim()).toHaveLength(0);
	});

	test("dedupe upserts an unlocked row, then supersedes a locked row and fences its result", () => {
		const a = invoke({ dedupeKey: "key", payload: 1 });
		expect(invoke({ dedupeKey: "key", payload: 2 })).toBe(a);
		expect(row(a).payload).toBe("2");
		const stale = first(claim());
		const b = invoke({ dedupeKey: "key", payload: 3 });
		expect(b === a).toBe(false);
		expect(row(a).failed_at).toBe(now);
		expect(row(a).dedupe_key).toBeNull();
		expect(row(a).last_error).toBe("superseded by reinvoke");
		settle([stale]);
		expect(row(a).completed_at).toBeNull();
		expect(first(claim()).id).toBe(b || "");
	});

	test("throttle inserts once into the current slot, allows next slot and allows reuse after completion", () => {
		const a = invoke({ dedupeSeconds: 10 });
		expect(invoke({ dedupeSeconds: 10, payload: "ignored" })).toBeNull();
		expect(row(a).singleton_on).toBe(now);
		settle(claim());
		expect(invoke({ dedupeSeconds: 10 }) === null).toBe(false);
		expect(invoke({ dedupeSeconds: 10, now: now + 10_000 }) === null).toBe(false);
	});

	test("debounce updates payload in the next slot, ignores supplied runAt, cannot claim early", () => {
		const a = invoke({ dedupeSeconds: 10, dedupeNextSlot: true, payload: 1, runAt: now });
		expect(invoke({ dedupeSeconds: 10, dedupeNextSlot: true, payload: 2, now: now + 1 })).toBe(a);
		expect(row(a).run_at).toBe(now + 10_000);
		expect(row(a).payload).toBe("2");
		expect(claim()).toHaveLength(0);
		expect(first(claim({ now: now + 10_000 })).id).toBe(a || "");
	});

	test("stale orchestrator fencing covers completed/failed/released/child and step writes", () => {
		store.heartbeat("worker", now);
		for (let i = 0; i < 4; i += 1) invoke();
		const rows = claim();
		expect(store.recoverStaleOrchestrators({ now: now + 100, maxAgeMs: 50 })).toBe(1);
		const fresh = claim({ orchestratorId: "new" });
		const refs = rows.map(ref);
		const a = refs[0];
		const b = refs[1];
		const c = refs[2];
		const d = refs[3];
		if (a === undefined || b === undefined || c === undefined || d === undefined)
			throw new Error("missing refs");
		store.returnExecutions({
			orchestratorId: "worker",
			completed: [a],
			failed: [{ ...b, permanent: true }],
			released: [c],
			invokeChild: [{ ...d, stepKey: "child", child: { taskKey: "task" } }],
			now,
		});
		expect(store.lastStatementCount).toBe(1);
		expect(store.saveStep({ ...a, orchestratorId: "worker", key: "stale", result: 1, now })).toBe(
			0,
		);
		expect(store.db.query("select * from steps").all()).toHaveLength(0);
		for (const e of fresh) {
			expect(row(e.id).locked_by).toBe("new");
			expect(row(e.id).attempts).toBe(2);
		}
		expect(store.db.query("select * from executions").all()).toHaveLength(4);
	});

	test("release memoizes step, decrements attempts, infinity waits; saveStep is idempotent and fenced", () => {
		const id = invoke();
		const e = first(claim());
		const args = { ...ref(e), orchestratorId: "worker", key: "saved", result: 42, runAtMs: 1, now };
		expect(store.saveStep(args)).toBe(1);
		expect(store.saveStep({ ...args, result: 100, runAtMs: 999 })).toBe(0);
		expect(row(id).run_at).toBe(now + 1);
		store.returnExecutions({
			orchestratorId: "worker",
			released: [{ ...ref(e), stepKey: "sleep", rescheduleInMs: "infinity" }],
			now,
		});
		expect(row(id).attempts).toBe(0);
		expect(row(id).run_at).toBe(INFINITY);
		expect(store.db.query("select * from steps").all()).toHaveLength(2);
		expect(claim({ now: now + 100_000 })).toHaveLength(0);
	});

	test("timed out / reclaimed parent cannot be woken; completed orphan is failed", () => {
		invoke();
		const parent = first(claim());
		store.returnExecutions({
			orchestratorId: "worker",
			invokeChild: [{ ...ref(parent), stepKey: "child", child: { taskKey: "task" }, timeoutMs: 1 }],
			now,
		});
		const child = first(claim());
		const resumed = first(claim({ now: now + 1 }));
		settle([resumed], now + 1);
		// The normal worker clears a timed-out wait; model that state change explicitly.
		store.db
			.transaction(() =>
				store.db.run("update executions set waiting_on_execution_id = null where id = ?", [
					parent.id,
				]),
			)
			.immediate();
		settle([child], now + 2);
		expect(row(child.id).last_error).toBe("Parent timed out before child completed");
		expect(row(child.id).failed_at).toBe(now + 2);
		expect(row(parent.id).completed_at).toBe(now + 1);
	});

	test("immediate retention applies to completed rows and cascading failures", () => {
		store.registerTask({ key: "task", remove_on_complete_days: 0, remove_on_fail_days: 0 });
		invoke();
		const rows = claim();
		store.saveStep({ ...ref(first(rows)), orchestratorId: "worker", key: "memo", result: 1, now });
		settle(rows);
		expect(store.db.query("select * from executions").all()).toHaveLength(0);
		expect(store.db.query("select * from steps").all()).toHaveLength(0);
		invoke();
		const child = childOf(first(claim()));
		store.returnExecutions({
			orchestratorId: "worker",
			failed: [{ ...ref(child), permanent: true }],
			now,
		});
		expect(store.db.query("select * from executions").all()).toHaveLength(0);
	});

	test("worst mixed settlement uses 15 statements, including cascade, independent of depth", () => {
		store.registerTask({ key: "remove", remove_on_complete_days: 0, remove_on_fail_days: 0 });
		const ids = new Map<string, string>();
		for (const name of [
			"done",
			"parent",
			"orphan",
			"deleted",
			"failure",
			"ancestor",
			"retry",
			"release",
			"invoke",
		])
			ids.set(name, Bun.randomUUIDv7());
		const id = (name: string) => {
			const value = ids.get(name);
			if (value === undefined) throw new Error("missing fixture id");
			return value;
		};
		store.db
			.transaction(() => {
				for (const [name, value] of ids) {
					const waiting = name === "parent" || name === "ancestor";
					store.db.run(
						`insert into executions (id, task_key, run_at, created_at, locked_by, locked_at,
					attempts, waiting_on_execution_id, waiting_step_key, parent_execution_id) values (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
						[
							value,
							name === "deleted" || name === "ancestor" ? "remove" : "task",
							waiting ? INFINITY : now,
							now,
							waiting ? null : "worker",
							waiting ? null : now,
							name === "parent" ? id("done") : name === "ancestor" ? id("failure") : null,
							waiting ? "child" : null,
							name === "orphan" ? "absent-parent" : null,
						],
					);
				}
			})
			.immediate();
		store.returnExecutions({
			orchestratorId: "worker",
			now,
			completed: [ref(row(id("done"))), ref(row(id("orphan"))), ref(row(id("deleted")))],
			failed: [
				{ ...ref(row(id("failure"))), permanent: true, error: "leaf" },
				ref(row(id("retry"))),
			],
			released: [{ ...ref(row(id("release"))), stepKey: "sleep" }],
			invokeChild: [{ ...ref(row(id("invoke"))), stepKey: "child", child: { taskKey: "task" } }],
		});
		expect(store.lastStatementCount).toBe(15);
		expect(row(id("done")).completed_at).toBe(now);
		expect(row(id("orphan")).failed_at).toBe(now);
		expect(row(id("failure")).last_error).toBe("leaf");
		expect(row(id("retry")).run_at).toBe(now + 15_000);
		expect(row(id("release")).attempts).toBe(0);
		expect(row(id("parent")).waiting_on_execution_id).toBeNull();
	});

	test("a late write failure rolls back the entire mixed settlement", () => {
		invoke();
		invoke();
		const rows = claim();
		const a = first(rows);
		const b = rows[1];
		if (b === undefined) throw new Error("missing second row");
		store.db
			.transaction(() =>
				store.db.exec(`create trigger reject_child before insert on executions
			when new.parent_execution_id is not null begin select raise(abort, 'injected'); end`),
			)
			.immediate();
		expect(() =>
			store.returnExecutions({
				orchestratorId: "worker",
				completed: [ref(a)],
				invokeChild: [{ ...ref(b), stepKey: "child", child: { taskKey: "task" } }],
				now,
			}),
		).toThrow("injected");
		expect(row(a.id).completed_at).toBeNull();
		expect(row(a.id).locked_by).toBe("worker");
		expect(row(b.id).waiting_on_execution_id).toBeNull();
		expect(store.db.query("select * from executions").all()).toHaveLength(2);
	});
});

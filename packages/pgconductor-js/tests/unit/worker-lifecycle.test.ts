import { expect, test } from "bun:test";
import { Worker } from "../../src/worker";
import { DefaultLogger } from "../../src/lib/logger";
import { MockDatabaseClient } from "../mocks/database-client.mock";
import type { AnyTask } from "../../src/task";

const task = {
	name: "lifecycle-task",
	triggers: [{ invocable: true }],
	maxAttempts: 3,
	execute: async () => {},
} as unknown as AnyTask;

test("clears dead-letter targets when a worker lifecycle resets", () => {
	const worker = new Worker("default", [task], new MockDatabaseClient(), new DefaultLogger());
	const targets = (worker as any).parentDeadLetterTargets as Map<string, unknown>;
	targets.set("child", {});

	(worker as any).resetLifecycle();

	expect(targets.size).toBe(0);
});

test("keeps dead-letter targets until a retried settlement commits", async () => {
	let calls = 0;
	const db = new MockDatabaseClient({
		returnExecutions: async () => {
			expect(targets.size).toBe(1);
			calls += 1;
			return calls === 1
				? Promise.reject(new Error("transient settlement failure"))
				: { outcomes: [], deliveries: [] };
		},
	});
	const worker = new Worker("default", [task], db, new DefaultLogger(), { flushBatchSize: 1 });
	const targets = (worker as any).parentDeadLetterTargets as Map<string, unknown>;
	targets.set("child", {});
	(worker as any)._abortController = new AbortController();
	(worker as any).orchestratorId = "orchestrator";
	const result = {
		execution_id: "child",
		orchestrator_id: "orchestrator",
		queue: "default",
		task_key: task.name,
		status: "completed" as const,
	};

	await (worker as any).flushResults(
		(async function* () {
			yield result;
		})(),
	);

	expect(calls).toBe(2);
	expect(targets.size).toBe(0);
});

test("resets a worker after registration failure and permits retry", async () => {
	let shouldFail = true;
	const db = new MockDatabaseClient({
		registerWorker: async () => {
			if (shouldFail) throw new Error("registration failed");
			return [];
		},
	});
	const worker = new Worker("default", [task], db, new DefaultLogger());

	await expect(worker.start("first-orchestrator")).rejects.toThrow("registration failed");
	// A failed startup is not a running worker: stopped must be already settled,
	// rather than an unhandled rejected lifecycle promise.
	await expect(worker.stopped).resolves.toBeUndefined();

	shouldFail = false;
	await worker.start("second-orchestrator");
	await worker.stop();
	await expect(worker.stopped).resolves.toBeUndefined();
});

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

test("resets a worker after registration failure and permits retry", async () => {
	let shouldFail = true;
	const db = new MockDatabaseClient({
		registerWorker: async () => {
			if (shouldFail) throw new Error("registration failed");
		},
	});
	const worker = new Worker("default", [task], db as never, new DefaultLogger());

	await expect(worker.start("first-orchestrator")).rejects.toThrow("registration failed");
	// A failed startup is not a running worker: stopped must be already settled,
	// rather than an unhandled rejected lifecycle promise.
	await expect(worker.stopped).resolves.toBeUndefined();

	shouldFail = false;
	await worker.start("second-orchestrator");
	await worker.stop();
	await expect(worker.stopped).resolves.toBeUndefined();
});

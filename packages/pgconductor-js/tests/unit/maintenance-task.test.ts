import { expect, test } from "vitest";
import { createMaintenanceTask } from "../../src/maintenance-task";
import { MockDatabaseClient } from "../mocks/database-client.mock";
import type { DatabaseClient } from "../../src/database-client";
import type { AnyTask } from "../../src/task";
import type { TaskContext } from "../../src/task-context";

test.each(["failUnregisteredExecutions", "removeExecutions"] as const)(
	"maintenance stops between batches of %s once its signal aborts",
	async (method) => {
		const db = new MockDatabaseClient();
		const controller = new AbortController();
		let batches = 0;
		db[method].mockImplementation(async () => {
			batches++;
			if (batches === 3) controller.abort();
			// Without the check the loop would never end
			if (batches > 10) throw new Error("kept going after the abort");
			return true;
		});
		const task = createMaintenanceTask("default");
		const tasks = new Map<string, AnyTask>([
			["retained", { removeOnComplete: true } as unknown as AnyTask],
		]);
		if (method === "removeExecutions") db.failUnregisteredExecutions.mockResolvedValue(false);
		const ctx = {
			db: db as unknown as DatabaseClient,
			tasks,
			signal: controller.signal,
		} as unknown as { db: DatabaseClient; tasks: Map<string, AnyTask> } & TaskContext;

		await expect(task.execute({ name: "pgconductor.maintenance" }, ctx)).rejects.toThrow();
		expect(batches).toBe(3);
	},
);

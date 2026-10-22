import { Task, type AnyTask } from "./task.js";
import type { DatabaseClient } from "./database-client.js";
import crypto from "crypto";
import type { TaskContext } from "./task-context.js";

function hashToJitter(str: string): number {
	const hash = crypto.createHash("sha256").update(str).digest();
	// Use the first 4 bytes as an unsigned integer
	const int = hash.readUInt32BE(0);
	return int % 60; // → 0–59 minutes after midnight
}

// we could make this configurable later
// should be lower for very high steps per task
const BATCH_SIZE = 1000;

// Executions of a task that registration left out are failed only after this grace: during a
// rolling deploy, orchestrators of the previous version still run them until they stop, and a
// rollback registers the task again. A day covers both; afterwards no worker runs them anymore.
const UNREGISTERED_GRACE_DAYS = 1;

export const createMaintenanceTask = <Queue extends string = "default">(queue: Queue) => {
	// Add consistent jitter based on queue name to spread load between midnight and 1am
	const jitterMinutes = hashToJitter(queue);

	return new Task<
		"pgconductor.maintenance",
		Queue,
		object,
		void,
		{ db: DatabaseClient; tasks: Map<string, AnyTask> } & TaskContext,
		{ name: "pgconductor.maintenance" }
	>(
		{
			name: "pgconductor.maintenance",
			queue,
		},
		{
			// Run daily between 00:00 and 01:00, with consistent jitter per queue
			cron: `${jitterMinutes} 0 * * *`,
			name: "pgconductor.maintenance",
		},
		async (_, ctx) => {
			const { db, tasks, signal } = ctx;

			let hasMore = true;
			while (hasMore) {
				hasMore = await db.failUnregisteredExecutions(
					{
						queueName: queue,
						graceDays: UNREGISTERED_GRACE_DAYS,
						batchSize: BATCH_SIZE,
					},
					{ signal },
				);
			}

			// Skip execution cleanup when no tasks have retention settings (check in-memory config)
			const hasRetention = Array.from(tasks.values()).some(
				(t) => t.removeOnComplete || t.removeOnFail,
			);
			if (!hasRetention) {
				return;
			}

			// Delete old completed/failed executions based on retention settings
			// Steps are automatically deleted via CASCADE foreign key
			hasMore = true;
			while (hasMore) {
				hasMore = await db.removeExecutions(
					{
						queueName: queue,
						batchSize: BATCH_SIZE,
					},
					{ signal },
				);
			}
		},
	);
};

import { Task, type AnyTask } from "./task";
import type { DatabaseClient } from "./database-client";
import crypto from "crypto";
import type { TaskContext } from "./task-context";

function hashToJitter(str: string): number {
	const hash = crypto.createHash("sha256").update(str).digest();
	// Use the first 4 bytes as an unsigned integer
	const int = hash.readUInt32BE(0);
	return int % 61; // → 0–60 minutes after midnight
}

// we could make this configurable later
// should be lower for very high steps per task
const BATCH_SIZE = 1000;
const EVENT_RETENTION_DAYS = 7;

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
			// Delete old completed/failed executions based on retention settings.
			// Steps and event deliveries are removed by their foreign-key cascades.
			const hasRetention = Array.from(tasks.values()).some(
				(t) => t.removeOnComplete || t.removeOnFail,
			);
			if (hasRetention) {
				let hasMore = true;
				while (hasMore) {
					hasMore = await db.removeExecutions(
						{ queueName: queue, batchSize: BATCH_SIZE },
						{ signal },
					);
				}
			}

			// Keep acknowledged inbox rows only for a bounded period. Process in
			// batches so maintenance cannot monopolize the queue.
			const before = new Date(
				(await db.getCurrentTime({ signal })).getTime() -
					EVENT_RETENTION_DAYS * 24 * 60 * 60 * 1000,
			);
			let eventsRemain = true;
			while (eventsRemain) {
				eventsRemain = await db.removeProcessedEvents(
					{ before, batchSize: BATCH_SIZE },
					{ signal },
				);
			}
		},
	);
};

import { Task, type TaskConfiguration } from "./task";
import type { DatabaseClient } from "./database-client";
import crypto from "crypto";

function hashToJitter(str: string): number {
	const hash = crypto.createHash("sha256").update(str).digest();
	// Use the first 4 bytes as an unsigned integer
	const int = hash.readUInt32BE(0);
	return int % 61; // → 0–60 minutes after midnight
}

// we could make this configurable later
// should be lower for very high steps per task
const BATCH_SIZE = 1000;

export const createMaintenanceTask = <Queue extends string = "default">(
	queue: Queue,
	tasks: Pick<
		TaskConfiguration<Queue>,
		"name" | "removeOnComplete" | "removeOnFail"
	>[],
) => {
	// Add consistent jitter based on queue name to spread load between midnight and 1am
	const jitterMinutes = hashToJitter(queue);

	return new Task<
		Queue,
		"pgconductor.maintenance",
		object,
		void,
		{ db: DatabaseClient },
		{ event: "pgconductor.cron" }
	>(
		{
			name: "pgconductor.maintenance",
			queue,
		},
		{
			// Run daily between 00:00 and 01:00, with consistent jitter per queue
			cron: `${jitterMinutes} 0 * * *`,
		},
		async (_, { db }) => {
			// Delete old completed/failed executions based on retention settings
			// Steps are automatically deleted via CASCADE foreign key
			let hasMore = true;
			while (hasMore) {
				hasMore = await db.removeExecutions(queue, tasks, BATCH_SIZE);
			}
		},
	);
};

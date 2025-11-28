import { Task, type AnyTask } from "./task";
import type { DatabaseClient } from "./database-client";
import crypto from "crypto";

function hashToJitter(str: string): number {
	const hash = crypto.createHash("sha256").update(str).digest();
	// Use the first 4 bytes as an unsigned integer
	const int = hash.readUInt32BE(0);
	return int % 61; // → 0–60 minutes after midnight
}

function createPartitionName(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, "0");
	const day = String(date.getUTCDate()).padStart(2, "0");
	return `_private_events_${year}${month}${day}`;
}

// we could make this configurable later
// should be lower for very high steps per task
const BATCH_SIZE = 1000;

const PARTITION_RETENTION_DAYS = 7;
const PARTITION_AHEAD_DAYS = 3;

export const createMaintenanceTask = <Queue extends string = "default">(queue: Queue) => {
	// Add consistent jitter based on queue name to spread load between midnight and 1am
	const jitterMinutes = hashToJitter(queue);

	return new Task<
		"pgconductor.maintenance",
		Queue,
		object,
		void,
		{ db: DatabaseClient; tasks: Map<string, AnyTask> },
		{ event: "pgconductor.maintenance" }
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
		async (_, { db, tasks }) => {
			// Skip if no tasks have retention settings (check in-memory config)
			const hasRetention = Array.from(tasks.values()).some(
				(t) => t.removeOnComplete || t.removeOnFail,
			);
			if (!hasRetention) {
				return;
			}

			// Delete old completed/failed executions based on retention settings
			// Steps are automatically deleted via CASCADE foreign key
			let hasMore = true;
			while (hasMore) {
				hasMore = await db.removeExecutions({
					queueName: queue,
					batchSize: BATCH_SIZE,
				});
			}

			// If we are running within the default queue, also
			// - remove unused triggers
			// - pre-create events partitions
			// - remove old event partitions
			if (queue === "default") {
				await db.cleanupTriggers();

				const partitions = await db.listEventPartitions();
				const now = new Date();
				for (let dayOffset = 0; dayOffset <= PARTITION_AHEAD_DAYS; dayOffset++) {
					const partitionDate = new Date(now);
					partitionDate.setUTCDate(now.getUTCDate() + dayOffset);
					const partitionName = createPartitionName(partitionDate);

					if (!partitions.some((p) => p.table_name === partitionName)) {
						await db.createEventPartition(partitionDate);
					}
				}

				const cutoffDate = new Date();
				cutoffDate.setUTCDate(cutoffDate.getUTCDate() - PARTITION_RETENTION_DAYS);
				const cutoffPartitionName = createPartitionName(cutoffDate);

				for (const partitionName of partitions) {
					if (partitionName.table_name < cutoffPartitionName) {
						await db.dropEventPartition(partitionName);
					}
				}
			}
		},
	);
};

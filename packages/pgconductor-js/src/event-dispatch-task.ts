import type { DatabaseClient, Execution, ExecutionResult } from "./database-client";
import { coerceError } from "./lib/coerce-error";
import { Task, type AnyTask } from "./task";

export const EVENT_DISPATCH_QUEUE = "pgconductor.internal";
export const EVENT_DISPATCH_TASK = "pgconductor.event-dispatch";
const EVENT_DISPATCH_BATCH_SIZE = 10;

export function createEventDispatchTask(): AnyTask {
	return new Task(
		{
			name: EVENT_DISPATCH_TASK,
			queue: EVENT_DISPATCH_QUEUE,
			maxAttempts: 3,
			removeOnComplete: true,
			batch: { size: EVENT_DISPATCH_BATCH_SIZE, timeoutMs: 10 },
		},
		{ invocable: true },
		// Internal dispatch needs claimed execution IDs, so the worker calls the
		// batch adapter below rather than the public payload-only task handler.
		async () => {
			throw new Error("Event dispatch must be executed by the internal worker");
		},
	);
}

export async function executeEventDispatchBatch(
	db: DatabaseClient,
	executions: Execution[],
	orchestratorId: string,
	signal: AbortSignal,
): Promise<ExecutionResult[]> {
	try {
		const dispatched = await db.dispatchCustomEvents(
			{
				eventIds: executions.map((execution) => execution.id),
				orchestratorId,
			},
			{ signal },
		);
		const dispatchedIds = new Set(dispatched);

		return executions.map((execution) => {
			if (!dispatchedIds.has(execution.id)) {
				return {
					execution_id: execution.id,
					orchestrator_id: execution.locked_by,
					queue: execution.queue,
					task_key: execution.task_key,
					status: "failed" as const,
					error: "Event dispatch claim is no longer valid",
				};
			}
			return {
				execution_id: execution.id,
				orchestrator_id: execution.locked_by,
				queue: execution.queue,
				task_key: execution.task_key,
				status: "completed" as const,
				result: undefined,
			};
		});
	} catch (error) {
		const message = coerceError(error).message;
		return executions.map((execution) => ({
			execution_id: execution.id,
			orchestrator_id: execution.locked_by,
			queue: execution.queue,
			task_key: execution.task_key,
			status: "failed" as const,
			error: message,
		}));
	}
}

import type { DatabaseClient, Execution, ExecutionResult, Payload } from "./database-client";
import { coerceError } from "./lib/coerce-error";
import { Task } from "./task";

export const EVENT_DISPATCH_QUEUE = "pgconductor.internal";
export const EVENT_DISPATCH_TASK = "pgconductor.event-dispatch";
const EVENT_DISPATCH_BATCH_SIZE = 10;

type EventDispatchPayload = {
	eventKey: string;
	payload: Payload;
};

type EventDispatchContext = {
	db: DatabaseClient;
	orchestratorId: string;
	signal: AbortSignal;
};

async function executeEventDispatch(
	executions: Execution[],
	context: EventDispatchContext,
): Promise<ExecutionResult[]> {
	try {
		const dispatched = await context.db.dispatchCustomEvents(
			{
				eventIds: executions.map((execution) => execution.id),
				orchestratorId: context.orchestratorId,
			},
			{ signal: context.signal },
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

export const eventDispatchTask = new Task<
	typeof EVENT_DISPATCH_TASK,
	typeof EVENT_DISPATCH_QUEUE,
	EventDispatchPayload,
	ExecutionResult[],
	EventDispatchContext,
	Execution[]
>(
	{
		name: EVENT_DISPATCH_TASK,
		queue: EVENT_DISPATCH_QUEUE,
		maxAttempts: 3,
		removeOnComplete: true,
		batch: { size: EVENT_DISPATCH_BATCH_SIZE, timeoutMs: 10 },
	},
	{ invocable: true },
	executeEventDispatch,
);

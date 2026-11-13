import type {
	Contained,
	DatabaseClient,
	EventInfo,
	ExecutionInfo,
	ListenerInfo,
	Payload,
} from "./database-client.js";
import type { InferPayload, InferReturns, InferState } from "./task-definition.js";
import type { TaskIdentifier } from "./task.js";

// The reads of `Conductor` and `TaskContext`: a task reads as the client does.

type ReadOptions = { signal?: AbortSignal };

/** An execution of a task, its payload, state and result typed by the task's definition. */
export type TaskExecutionInfo<TTask> = ExecutionInfo<
	InferReturns<TTask>,
	InferPayload<TTask>,
	InferState<TTask>
>;

/** An execution of a task an event would reach, typed by the task's definition. */
export type TaskListenerInfo<TTask> = ListenerInfo<
	InferReturns<TTask>,
	InferPayload<TTask>,
	InferState<TTask>
>;

/** Which executions `conductor.listExecutions()` reads. */
export type ExecutionQuery<TName extends string, TQueue extends string, TPayload> = {
	task: TaskIdentifier<TName, TQueue>;
	/** Payload fields the execution's payload contains, nested objects by their own fields */
	where?: Contained<TPayload>;
	/** Only executions that have not completed, failed or been cancelled */
	open?: boolean;
	/** Oldest first (`asc`, the default) or newest first */
	order?: "asc" | "desc";
	limit?: number;
	offset?: number;
};

/**
 * An execution `conductor.getExecution()` reads by key: the open execution of the task holding
 * `key` (see `signalWithStart()`), or the execution invoked with `dedupeKey`.
 */
export type ExecutionLookup<TName extends string, TQueue extends string> =
	| { task: TaskIdentifier<TName, TQueue>; key: string; dedupeKey?: never }
	| { task: TaskIdentifier<TName, TQueue>; dedupeKey: string; key?: never };

/** Which kept events `conductor.listEvents()` reads. */
export type EventQuery<TName extends string, TPayload> = {
	name: TName;
	/** Payload fields the event's payload contains, nested objects by their own fields */
	where?: Contained<TPayload>;
	/** Only events stored after this kept event */
	after?: string;
	/** Only events stored before this kept event */
	before?: string;
	/** Only events stored after this time */
	since?: Date;
	order?: "asc" | "desc";
	limit?: number;
	offset?: number;
};

export function eventFilter(
	query: Omit<EventQuery<string, unknown>, "order" | "limit" | "offset">,
) {
	return {
		eventKey: query.name,
		where: query.where as Payload | undefined,
		after: query.after,
		before: query.before,
		since: query.since,
	};
}

/** Reads an execution by id, by task and id, or by key or dedupe key. */
export function readExecution(
	db: DatabaseClient,
	target: TaskIdentifier<string, string> | ExecutionLookup<string, string> | string,
	executionId: string | undefined,
	opts?: ReadOptions,
): Promise<ExecutionInfo | null> {
	if (typeof target === "string") {
		return db.getExecution({ executionId: target }, opts);
	}
	if ("task" in target) {
		const task = { key: target.task.name, queue: target.task.queue || "default" };
		return db.getExecution(
			target.key === undefined ? { task, dedupeKey: target.dedupeKey } : { task, key: target.key },
			opts,
		);
	}
	return db.getExecution(
		{
			executionId: executionId as string,
			task: { key: target.name, queue: target.queue || "default" },
		},
		opts,
	);
}

export function readExecutions(
	db: DatabaseClient,
	query: ExecutionQuery<string, string, Payload>,
	opts?: ReadOptions,
): Promise<ExecutionInfo[]> {
	return db.listExecutions(
		{
			task: { key: query.task.name, queue: query.task.queue || "default" },
			where: query.where,
			open: query.open,
			order: query.order,
			limit: query.limit || 100,
			offset: query.offset || 0,
		},
		opts,
	);
}

export function readListeners(
	db: DatabaseClient,
	query: {
		event: string;
		payload: Payload;
		task?: TaskIdentifier<string, string>;
		limit?: number;
	},
	opts?: ReadOptions,
): Promise<ListenerInfo[]> {
	return db.listListeners(
		{
			event: query.event,
			payload: query.payload,
			task: query.task && { key: query.task.name, queue: query.task.queue || "default" },
			limit: query.limit || 100,
		},
		opts,
	);
}

export function readEvents(
	db: DatabaseClient,
	query: EventQuery<string, unknown>,
	opts?: ReadOptions,
): Promise<EventInfo[]> {
	return db.listEvents(
		{
			...eventFilter(query),
			order: query.order || "asc",
			limit: query.limit || 100,
			offset: query.offset || 0,
		},
		opts,
	);
}

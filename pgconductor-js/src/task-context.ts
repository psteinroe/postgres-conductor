import CronExpressionParser from "cron-parser";
import type { DatabaseClient, JsonValue, Execution } from "./database-client";
import type {
	TaskDefinition,
	TaskName,
	FindTaskByIdentifier,
	InferPayload,
	InferReturns,
} from "./task-definition";
import type { TaskIdentifier } from "./task";
import type { Logger } from "./lib/logger";
import type {
	CustomEventConfig,
	DatabaseEventConfig,
	DatabaseEventPayload,
	EventDefinition,
	EventName,
	FindEventByIdentifier,
	GenericDatabase,
	InferEventPayload,
	RowType,
	SchemaName,
	SharedEventConfig,
	TableName,
} from "./event-definition";

export type TaskContextOptions = {
	signal: AbortSignal;
	abortController: AbortController;
	db: DatabaseClient;
	execution: Execution;
	logger: Logger;
};

type ScheduleOptions = {
	cron: string;
	priority?: number;
};

// second argument for tasks
export class TaskContext<
	Tasks extends readonly TaskDefinition<
		string,
		any,
		any,
		string
	>[] = readonly TaskDefinition<string, any, any, string>[],
	Events extends readonly EventDefinition<
		string,
		any
	>[] = readonly EventDefinition<string, any>[],
	TDatabase extends GenericDatabase = GenericDatabase,
> {
	constructor(private readonly opts: TaskContextOptions) {}

	static create<
		Tasks extends readonly TaskDefinition<string, any, any, string>[],
		Events extends readonly EventDefinition<string, any>[],
		Extra extends object,
	>(opts: TaskContextOptions, extra?: Extra): TaskContext<Tasks> & Extra {
		const base = new TaskContext<Tasks, Events>(opts);
		return Object.assign(base, extra) as TaskContext<Tasks> & Extra;
	}

	get logger(): Logger {
		return this.opts.logger;
	}

	get signal(): AbortSignal {
		return this.opts.abortController.signal;
	}

	async step<T extends JsonValue | void>(
		name: string,
		fn: () => Promise<T> | T,
	): Promise<T> {
		// Check if step already completed
		const cached = await this.opts.db.loadStep({
			executionId: this.opts.execution.id,
			key: name,
		});

		if (cached !== undefined) {
			return cached as T;
		}

		// Execute and save
		const result = await fn();

		await this.opts.db.saveStep({
			executionId: this.opts.execution.id,
			queue: this.opts.execution.queue,
			key: name,
			result: { result: result as JsonValue },
			runAtMs: undefined,
		});

		return result;
	}

	async checkpoint(): Promise<void> {
		if (this.opts.signal.aborted) {
			return this.abortAndHangup();
		}
	}

	async sleep(id: string, ms: number): Promise<void> {
		// Check if we already slept
		const cached = await this.opts.db.loadStep({
			executionId: this.opts.execution.id,
			key: id,
		});

		if (cached !== undefined) {
			return; // Already slept, continue
		}

		// Save sleep step with ms - time calculation happens in SQL
		await this.opts.db.saveStep({
			executionId: this.opts.execution.id,
			queue: this.opts.execution.queue,
			key: id,
			result: null,
			runAtMs: ms,
		});

		return this.abortAndHangup();
	}

	async invoke<
		TName extends TaskName<Tasks>,
		TQueue extends string = "default",
		TDef extends FindTaskByIdentifier<
			Tasks,
			TName,
			TQueue
		> = FindTaskByIdentifier<Tasks, TName, TQueue>,
	>(
		id: string,
		task: TaskIdentifier<TName, TQueue>,
		payload: InferPayload<TDef> = {} as InferPayload<TDef>,
		timeout?: number,
	): Promise<InferReturns<TDef>> {
		const cached = await this.opts.db.loadStep({
			executionId: this.opts.execution.id,
			key: id,
		});

		if (cached !== undefined) {
			return cached as InferReturns<TDef>;
		}

		// Check if we're already waiting (distinguishes first invoke from timeout)
		if (this.opts.execution.waiting_on_execution_id !== null) {
			// we resumed but no step exists → timeout occurred
			// clear waiting state before throwing
			await this.opts.db.clearWaitingState({
				executionId: this.opts.execution.id,
			});

			throw new Error(
				timeout
					? `Child execution timed out after ${timeout}ms`
					: "Child execution timeout",
			);
		}

		const taskName = task.name;
		const queue = task.queue || "default";

		await this.opts.db.invokeChild({
			task_key: taskName,
			queue,
			payload,
			parent_execution_id: this.opts.execution.id,
			parent_step_key: id,
			parent_timeout_ms: timeout || null,
		});

		return this.abortAndHangup();
	}

	async schedule<
		TName extends TaskName<Tasks>,
		TQueue extends string = "default",
		TDef extends FindTaskByIdentifier<
			Tasks,
			TName,
			TQueue
		> = FindTaskByIdentifier<Tasks, TName, TQueue>,
	>(
		task: TaskIdentifier<TName, TQueue>,
		scheduleName: string,
		options: ScheduleOptions,
		payload: InferPayload<TDef> = {} as InferPayload<TDef>,
	): Promise<void> {
		if (!scheduleName) {
			throw new Error("scheduleName is required");
		}

		if (scheduleName.includes("::")) {
			throw new Error("scheduleName cannot contain '::'");
		}

		if (!options?.cron) {
			throw new Error("cron expression is required");
		}

		const interval = CronExpressionParser.parse(options.cron);
		const nextTimestamp = interval.next().toDate();
		const queue = task.queue || "default";

		await this.opts.db.scheduleCronExecution({
			spec: {
				task_key: task.name,
				queue,
				payload,
				run_at: nextTimestamp,
				cron_expression: options.cron,
				priority: options.priority || null,
			},
			scheduleName,
		});
	}

	async unschedule<
		TName extends TaskName<Tasks>,
		TQueue extends string = "default",
	>(
		task: TaskIdentifier<TName, TQueue>,
		scheduleName: string,
	): Promise<boolean> {
		if (!scheduleName) {
			throw new Error("scheduleName is required");
		}

		if (scheduleName.includes("::")) {
			throw new Error("scheduleName cannot contain '::'");
		}

		const queue = task.queue || "default";
		return this.opts.db.unscheduleCronExecution({
			taskKey: task.name,
			queue,
			scheduleName,
		});
	}

	/**
	 * Wait for a custom event to arrive.
	 */
	async waitForEvent<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<
			Events,
			TName
		>,
	>(
		id: string,
		config: CustomEventConfig<TName> & SharedEventConfig,
	): Promise<InferEventPayload<TDef>>;

	/**
	 * Wait for a database change event.
	 */
	async waitForEvent<
		TSchema extends SchemaName<TDatabase>,
		TTable extends TableName<TDatabase, TSchema>,
		TOp extends "insert" | "update" | "delete",
		const TSelection extends string | undefined = undefined,
	>(
		id: string,
		config: DatabaseEventConfig<TDatabase, TSchema, TTable, TOp, TSelection>,
	): Promise<
		DatabaseEventPayload<RowType<TDatabase, TSchema, TTable>, TOp, TSelection>
	>;

	async waitForEvent(
		id: string,
		config: (
			| CustomEventConfig<string>
			| DatabaseEventConfig<TDatabase, any, any, any, any>
		) &
			SharedEventConfig,
	): Promise<unknown> {
		// Check if we already received the event
		const cached = await this.opts.db.loadStep({
			executionId: this.opts.execution.id,
			key: id,
		});

		if (cached !== undefined) {
			return cached;
		}

		// Check if we're already waiting (distinguishes first call from timeout)
		if (this.opts.execution.waiting_step_key !== null) {
			// We resumed but no step exists → timeout occurred
			// Clear waiting state before throwing
			await this.opts.db.clearWaitingState({
				executionId: this.opts.execution.id,
			});

			throw new Error(
				config.timeout
					? `Event wait timed out after ${config.timeout}ms`
					: "Event wait timeout",
			);
		}

		// Determine if this is a custom event or database event
		if ("event" in config) {
			// Custom event
			await this.opts.db.subscribeEvent({
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				stepKey: id,
				eventKey: config.event,
				timeoutMs: config.timeout,
			});
		} else {
			// Database event
			// Parse columns string into array if provided
			const columnsStr = config.columns as string | undefined;
			const columns = columnsStr
				? columnsStr.split(",").map((c: string) => c.trim().toLowerCase())
				: undefined;

			await this.opts.db.subscribeDbChange({
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				stepKey: id,
				schemaName: config.schema,
				tableName: config.table,
				operation: config.operation,
				timeoutMs: config.timeout,
				columns,
			});
		}

		return this.abortAndHangup();
	}

	/**
	 * Emit a typed custom event.
	 * @param config - Event configuration with event name
	 * @param payload - Typed event payload
	 */
	async emit<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<
			Events,
			TName
		>,
	>(
		{ event }: CustomEventConfig<TName>,
		payload: InferEventPayload<TDef>,
	): Promise<string> {
		return this.opts.db.emitEvent({ eventKey: event, payload });
	}

	/**
	 * Abort the current execution and hang up indefinitely
	 * @return Promise that never resolves
	 **/
	private abortAndHangup(): Promise<never> {
		this.opts.abortController.abort();
		return new Promise(() => {});
	}
}

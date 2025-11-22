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
import type { SelectionInput } from "./select-columns";

export type TaskContextOptions = {
	signal: AbortSignal;
	abortController: AbortController;
	db: DatabaseClient;
	execution: Execution;
	logger: Logger;
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

	async step<T extends JsonValue | void>(
		name: string,
		fn: () => Promise<T> | T,
	): Promise<T> {
		// Check if step already completed
		const cached = await this.opts.db.loadStep(
			this.opts.execution.id,
			name,
			this.opts.signal,
		);

		if (cached !== undefined) {
			return cached as T;
		}

		// Execute and save
		const result = await fn();

		await this.opts.db.saveStep(
			this.opts.execution.id,
			this.opts.execution.queue,
			name,
			{ result: result as JsonValue },
			undefined,
			this.opts.signal,
		);

		return result;
	}

	async checkpoint(): Promise<void> {
		if (this.opts.signal.aborted) {
			return this.abortAndHangup();
		}
	}

	/**
	 * Abort the current execution and hang up indefinitely
	 * @return Promise that never resolves
	 **/
	private abortAndHangup(): Promise<never> {
		this.opts.abortController.abort();
		return new Promise(() => {});
	}

	async sleep(id: string, ms: number): Promise<void> {
		// Check if we already slept
		const cached = await this.opts.db.loadStep(
			this.opts.execution.id,
			id,
			this.opts.signal,
		);

		if (cached !== undefined) {
			return; // Already slept, continue
		}

		// Save sleep step with ms - time calculation happens in SQL
		await this.opts.db.saveStep(
			this.opts.execution.id,
			this.opts.execution.queue,
			id,
			null,
			ms,
			this.opts.signal,
		);

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
		const cached = await this.opts.db.loadStep(
			this.opts.execution.id,
			id,
			this.opts.signal,
		);

		if (cached !== undefined) {
			return cached as InferReturns<TDef>;
		}

		// Check if we're already waiting (distinguishes first invoke from timeout)
		if (this.opts.execution.waiting_on_execution_id !== null) {
			// we resumed but no step exists → timeout occurred
			// clear waiting state before throwing
			await this.opts.db.clearWaitingState(
				this.opts.execution.id,
				this.opts.signal,
			);

			throw new Error(
				timeout
					? `Child execution timed out after ${timeout}ms`
					: "Child execution timeout",
			);
		}

		const taskName = task.name;
		const queue = task.queue || "default";

		await this.opts.db.invokeChild(
			{
				task_key: taskName,
				queue,
				payload,
				parent_execution_id: this.opts.execution.id,
				parent_step_key: id,
				parent_timeout_ms: timeout || null,
			},
			this.opts.signal,
		);

		return this.abortAndHangup();
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
		TSelection extends
			| SelectionInput<RowType<TDatabase, TSchema, TTable>>
			| undefined = undefined,
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
		const cached = await this.opts.db.loadStep(
			this.opts.execution.id,
			id,
			this.opts.signal,
		);

		if (cached !== undefined) {
			return cached;
		}

		// Check if we're already waiting (distinguishes first call from timeout)
		if (this.opts.execution.waiting_step_key !== null) {
			// We resumed but no step exists → timeout occurred
			// Clear waiting state before throwing
			await this.opts.db.clearWaitingState(
				this.opts.execution.id,
				this.opts.signal,
			);

			throw new Error(
				config.timeout
					? `Event wait timed out after ${config.timeout}ms`
					: "Event wait timeout",
			);
		}

		// Determine if this is a custom event or database event
		if ("event" in config) {
			// Custom event
			await this.opts.db.subscribeEvent(
				this.opts.execution.id,
				this.opts.execution.queue,
				id,
				config.event,
				config.timeout,
				this.opts.signal,
			);
		} else {
			// Database event
			await this.opts.db.subscribeDbChange(
				this.opts.execution.id,
				this.opts.execution.queue,
				id,
				config.schema,
				config.table,
				config.operation,
				config.timeout,
				this.opts.signal,
			);
		}

		return this.abortAndHangup();
	}

	/**
	 * Emit a custom event that can wake up waiting executions.
	 * @param eventKey - The event key to emit
	 * @param payload - Optional payload to send with the event
	 */
	async emitEvent(eventKey: string, payload?: JsonValue): Promise<void> {
		await this.opts.db.emitEvent(eventKey, payload, this.opts.signal);
	}
}

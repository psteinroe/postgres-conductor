import type { Sql } from "postgres";
import { DatabaseClient, type ExecutionInfo, type ExecutionSpec } from "./database-client";
import {
	Task,
	type TaskConfiguration,
	type AnyTask,
	type TaskEventFromTriggers,
	type ValidateTasksQueue,
	type BatchConfig,
	type ExecuteFunction,
	type TaskIdentifier,
	type ValidateDeadLetterConfiguration,
} from "./task";
import type { TaskContext, BatchTaskContext, BatchTaskEvent } from "./task-context";
import {
	type FindTaskByIdentifier,
	type InferPayload,
	type InferReturns,
	type NonEmptyArray,
	type TaskDefinition,
	type TaskName,
	type Trigger,
	type ValidateTriggers,
	type ValidateEventTriggers,
} from "./task-definition";
import { EVENT_DISPATCH_QUEUE, EVENT_DISPATCH_TASK } from "./event-dispatch-task";
import { Worker, type WorkerConfig } from "./worker";
import { DefaultLogger, type Logger } from "./lib/logger";
import { waitFor } from "./lib/wait-for";
import { SchemaManager } from "./schema-manager";
import { Telemetry } from "./telemetry";
import {
	validateEventPayload,
	type EventDefinition,
	type EventName,
	type FindEventByIdentifier,
	type InferEventPayload,
} from "./event-definition";
import {
	TaskSchemas,
	EventSchemas,
	type InferTasksFromSchema,
	type InferEventsFromSchema,
} from "./schemas";

export type WaitForResultOptions = {
	/** Reject with a `TimeoutError` after this many milliseconds */
	timeout?: number;
	/** Delay between polls (default: 1000) */
	pollIntervalMs?: number;
	signal?: AbortSignal;
};

type ConnectionOptions =
	| { connectionString: string; sql?: never }
	| { sql: Sql; connectionString?: never };

// Helper types to avoid repetition in createTask
type ResolvedQueue<TDef extends { readonly queue?: string }> = TDef["queue"] extends string
	? TDef["queue"]
	: "default";

type ResolvedTaskDef<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TDef extends { readonly name: string; readonly queue?: string },
> = FindTaskByIdentifier<Tasks, TDef["name"], ResolvedQueue<TDef>>;

type ResolvedPayload<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TDef extends { readonly name: string; readonly queue?: string },
> = TDef["name"] extends TaskName<Tasks> ? InferPayload<ResolvedTaskDef<Tasks, TDef>> : {};

type ResolvedReturns<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TDef extends { readonly name: string; readonly queue?: string },
> = TDef["name"] extends TaskName<Tasks> ? InferReturns<ResolvedTaskDef<Tasks, TDef>> : void;

type ResolvedTaskEvent<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	Events extends readonly EventDefinition<string, any, any>[],
	TDef extends { readonly name: string; readonly queue?: string },
	TTriggers,
> = TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>, Events>;

type ResolvedBatchTaskEvent<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	Events extends readonly EventDefinition<string, any, any>[],
	TDef extends { readonly name: string; readonly queue?: string },
	TTriggers,
> = BatchTaskEvent<ResolvedTaskEvent<Tasks, Events, TDef, TTriggers>>;

export type ConductorOptions<
	TTaskSchemas extends TaskSchemas<any> | undefined,
	TEventSchemas extends EventSchemas<any> | undefined,
	ExtraContext extends object,
> = ConnectionOptions & {
	tasks?: TTaskSchemas;

	events?: TEventSchemas;

	context: ExtraContext;

	logger?: Logger;
	/** Disable OpenTelemetry instrumentation. The default is enabled and uses the global API provider. */
	telemetry?: false;
};

// similar to inngest client
// exposes the main createTask methods and handles types
export class Conductor<
	TTaskSchemas extends TaskSchemas<any> | undefined = undefined,
	TEventSchemas extends EventSchemas<any> | undefined = undefined,
	ExtraContext extends object = {},
	// Inferred types from schemas
	Tasks extends readonly TaskDefinition<string, any, any, string>[] =
		InferTasksFromSchema<TTaskSchemas>,
	Events extends readonly EventDefinition<string, any, any>[] =
		InferEventsFromSchema<TEventSchemas>,
> {
	/**
	 * @internal
	 * Internal database client
	 */
	readonly db: DatabaseClient;

	/**
	 * @internal
	 * Internal logger
	 */
	readonly logger: Logger;

	/** @internal */
	readonly telemetry: Telemetry;

	private constructor(
		public readonly options: ConductorOptions<TTaskSchemas, TEventSchemas, ExtraContext>,
	) {
		this.logger = options.logger || new DefaultLogger();
		this.telemetry = new Telemetry(options.telemetry !== false);

		if ("sql" in options && options.sql) {
			this.db = new DatabaseClient({ sql: options.sql, logger: this.logger });
		} else if ("connectionString" in options && options.connectionString) {
			this.db = new DatabaseClient({
				connectionString: options.connectionString,
				logger: this.logger,
			});
		} else {
			throw new Error("Conductor requires either a connectionString or sql instance");
		}
	}

	static create<
		TTaskSchemas extends TaskSchemas<any> | undefined = undefined,
		TEventSchemas extends EventSchemas<any> | undefined = undefined,
		TExtraContext extends object = {},
	>(
		options: ConnectionOptions & {
			tasks?: TTaskSchemas;
			events?: TEventSchemas;
			context: TExtraContext;
			logger?: Logger;
			telemetry?: false;
		},
	): Conductor<TTaskSchemas, TEventSchemas, TExtraContext> {
		return new Conductor<TTaskSchemas, TEventSchemas, TExtraContext>(options);
	}

	/**
	 * Ensure schema is at latest version.
	 * Useful for tests to initialize schema without starting orchestrator.
	 */
	async ensureInstalled(): Promise<void> {
		const schemaManager = new SchemaManager(this.db, {});
		const signal = new AbortController().signal;
		await schemaManager.ensureLatest(signal);
	}

	/**
	 * Close the connection pool created from `connectionString`.
	 * A user-supplied `sql` instance is left open.
	 */
	async close(): Promise<void> {
		await this.db.close();
	}

	createTask<
		const TDef extends {
			readonly name: string;
			readonly queue?: string;
			readonly batch?: BatchConfig;
		},
		const TTriggers extends object | readonly object[],
	>(
		definition: TDef & ValidateDeadLetterConfiguration<TDef, ResolvedPayload<Tasks, TDef>>,
		triggers: TTriggers &
			ValidateTriggers<Tasks, TDef["name"], TTriggers, ResolvedQueue<TDef>> &
			ValidateEventTriggers<Events, TTriggers>,
		fn: TDef extends { readonly batch: BatchConfig }
			? ResolvedReturns<Tasks, TDef> extends void
				? (
						events: ResolvedBatchTaskEvent<Tasks, Events, TDef, TTriggers>[],
						ctx: BatchTaskContext,
					) => Promise<void>
				: (
						events: ResolvedBatchTaskEvent<Tasks, Events, TDef, TTriggers>[],
						ctx: BatchTaskContext,
					) => Promise<Array<ResolvedReturns<Tasks, TDef>>>
			: (
					event: ResolvedTaskEvent<Tasks, Events, TDef, TTriggers>,
					ctx: TaskContext<Tasks, Events> & ExtraContext,
				) => Promise<ResolvedReturns<Tasks, TDef>>,
	): Task<
		TDef["name"],
		ResolvedQueue<TDef>,
		ResolvedPayload<Tasks, TDef>,
		ResolvedReturns<Tasks, TDef>,
		TaskContext<Tasks, Events> & ExtraContext,
		TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>, Events>
	> {
		return Task.create<
			TDef["name"],
			ResolvedQueue<TDef>,
			ResolvedPayload<Tasks, TDef>,
			ResolvedReturns<Tasks, TDef>,
			TaskContext<Tasks, Events> & ExtraContext,
			TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>, Events>
		>(
			definition as TaskConfiguration<
				TDef["name"],
				ResolvedQueue<TDef>,
				ResolvedPayload<Tasks, TDef>
			>,
			triggers as NonEmptyArray<Trigger> | Trigger,
			fn as ExecuteFunction<
				TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>, Events>,
				ResolvedReturns<Tasks, TDef>,
				TaskContext<Tasks, Events> & ExtraContext
			>,
			this.options.events?.definitions ?? [],
		);
	}

	createWorker<
		const TQueue extends string,
		const TTasks extends readonly Task<any, any, any, any, any, any>[],
	>(options: {
		queue: TQueue;
		tasks: ValidateTasksQueue<TQueue, TTasks>;
		config?: Partial<WorkerConfig>;
	}): Worker<Tasks> {
		if (options.queue === EVENT_DISPATCH_QUEUE) {
			throw new Error(`Queue "${EVENT_DISPATCH_QUEUE}" is reserved for internal use`);
		}
		return new Worker<Tasks>(
			options.queue,
			options.tasks as AnyTask[],
			this.db,
			this.logger,
			options.config,
			this.options.context,
			this.telemetry,
		);
	}

	async invoke<const TTask extends { readonly name: string; readonly queue?: string }>(
		task: TTask,
		payload: InferPayload<
			FindTaskByIdentifier<
				Tasks,
				TTask["name"],
				TTask["queue"] extends string ? TTask["queue"] : "default"
			>
		>,
		opts?: Omit<ExecutionSpec, "task_key" | "payload" | "queue">,
	): Promise<string>;
	async invoke<const TTask extends { readonly name: string; readonly queue?: string }>(
		task: TTask,
		items: Array<
			{
				payload: InferPayload<
					FindTaskByIdentifier<
						Tasks,
						TTask["name"],
						TTask["queue"] extends string ? TTask["queue"] : "default"
					>
				>;
			} & Omit<ExecutionSpec, "task_key" | "payload" | "queue">
		>,
	): Promise<string[]>;
	async invoke<const TTask extends { readonly name: string; readonly queue?: string }>(
		task: TTask,
		payloadOrItems: any,
		opts?: Omit<ExecutionSpec, "task_key" | "payload" | "queue">,
	): Promise<string | null | string[]> {
		const taskName = task.name;
		const queue = task.queue || "default";

		if (Array.isArray(payloadOrItems)) {
			return this.telemetry.send({
				taskKey: taskName,
				queue,
				batchMessageCount: payloadOrItems.length,
				run: async () => {
					const traceContext = this.telemetry.traceContext();
					return this.db.invokeBatch(
						payloadOrItems.map((item) => ({
							task_key: taskName,
							queue,
							payload: item.payload,
							run_at: item.run_at,
							dedupe_key: item.dedupe_key,
							throttle: item.throttle,
							debounce: item.debounce,
							cron_expression: item.cron_expression,
							priority: item.priority,
							group: item.group,
							trace_context: traceContext,
						})),
					);
				},
			});
		}

		return this.telemetry.send({
			taskKey: taskName,
			queue,
			run: async (span) => {
				const id = await this.db.invoke({
					task_key: taskName,
					queue,
					payload: payloadOrItems,
					...opts,
					trace_context: this.telemetry.traceContext(),
				});
				if (id) span?.setAttribute("messaging.message.id", id);
				return id;
			},
		});
	}

	/**
	 * Emit a typed custom event.
	 * @param event - Event name to emit
	 * @param payload - Typed event payload
	 * @param opts.dedupe_key - Repeating a key for the same event name emits nothing and returns the original event ID
	 * @returns Event ID
	 */
	async emit<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		event: TName,
		payload: InferEventPayload<TDef>,
		opts?: { dedupe_key?: string },
	): Promise<string> {
		payload = await validateEventPayload(this.options.events?.definitions || [], event, payload);

		return this.telemetry.send({
			taskKey: EVENT_DISPATCH_TASK,
			queue: EVENT_DISPATCH_QUEUE,
			run: async (span) => {
				const id = await this.db.emitEvent({
					eventKey: event,
					payload: payload as any,
					traceContext: this.telemetry.traceContext(),
					dedupeKey: opts?.dedupe_key,
				});
				span?.setAttribute("messaging.message.id", id);
				return id;
			},
		});
	}

	async cancel(executionId: string, options?: { reason?: string }): Promise<boolean> {
		return this.db.cancelExecution(executionId, options || {});
	}

	/**
	 * Read an execution. Returns null for an unknown id or an execution removed by retention.
	 * Pass the task to type the result.
	 */
	async getExecution(executionId: string): Promise<ExecutionInfo | null>;
	async getExecution<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		task: TaskIdentifier<TName, TQueue>,
		executionId: string,
	): Promise<ExecutionInfo<InferReturns<FindTaskByIdentifier<Tasks, TName, TQueue>>> | null>;
	async getExecution(
		taskOrId: TaskIdentifier<string, string> | string,
		executionId?: string,
	): Promise<ExecutionInfo | null> {
		if (typeof taskOrId === "string") {
			return this.db.getExecution({ executionId: taskOrId });
		}
		return this.db.getExecution({
			executionId: executionId as string,
			task: { key: taskOrId.name, queue: taskOrId.queue || "default" },
		});
	}

	/**
	 * Poll an execution until it settles. Resolves with the result of a completed execution and
	 * rejects if it failed, was cancelled, does not exist, or the timeout or signal fires first.
	 * Pass the task to type the result.
	 */
	async waitForResult(executionId: string, options?: WaitForResultOptions): Promise<unknown>;
	async waitForResult<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		task: TaskIdentifier<TName, TQueue>,
		executionId: string,
		options?: WaitForResultOptions,
	): Promise<InferReturns<FindTaskByIdentifier<Tasks, TName, TQueue>>>;
	async waitForResult(
		taskOrId: TaskIdentifier<string, string> | string,
		idOrOptions?: string | WaitForResultOptions,
		options?: WaitForResultOptions,
	): Promise<unknown> {
		const [task, executionId, { timeout, pollIntervalMs = 1000, signal } = {}] =
			typeof taskOrId === "string"
				? [undefined, taskOrId, idOrOptions as WaitForResultOptions | undefined]
				: [taskOrId, idOrOptions as string, options];
		const stop = AbortSignal.any([
			...(signal ? [signal] : []),
			...(timeout ? [AbortSignal.timeout(timeout)] : []),
		]);

		while (true) {
			stop.throwIfAborted();
			const execution = await this.db.getExecution(
				{
					executionId,
					task: task && { key: task.name, queue: task.queue || "default" },
				},
				{ signal: stop },
			);
			if (!execution) {
				throw new Error(`Execution ${executionId} not found`);
			}
			if (execution.status === "completed") {
				return execution.result;
			}
			if (execution.status === "failed" || execution.status === "cancelled") {
				throw new Error(execution.error || `Execution ${execution.status}`);
			}
			await waitFor(pollIntervalMs, { signal: stop });
		}
	}
}

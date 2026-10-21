import type { Sql } from "postgres";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import {
	DatabaseClient,
	type ExecutionInfo,
	type ExecutionSpec,
	type Payload,
} from "./database-client.js";
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
} from "./task.js";
import type {
	TaskContext,
	BatchTaskContext,
	BatchTaskEvent,
	SignalMessage,
} from "./task-context.js";
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
} from "./task-definition.js";
import { EVENT_DISPATCH_QUEUE, EVENT_DISPATCH_TASK } from "./event-dispatch-task.js";
import { Worker, type WorkerConfig } from "./worker.js";
import { DefaultLogger, type Logger } from "./lib/logger.js";
import type { AddedContext, Middleware } from "./middleware.js";
import { waitFor } from "./lib/wait-for.js";
import { parseDuration, type DurationInput } from "./lib/duration.js";
import { SchemaManager } from "./schema-manager.js";
import { Telemetry } from "./telemetry.js";
import { validateSchema } from "./lib/validate-schema.js";
import {
	validateEventPayload,
	type EventDefinition,
	type EventName,
	type FindEventByIdentifier,
	type InferEventPayload,
} from "./event-definition.js";
import {
	TaskSchemas,
	EventSchemas,
	type EventCatalog,
	type InferTasksFromSchema,
	type InferEventsFromSchema,
} from "./schemas.js";

export type WaitForResultOptions = {
	/** Reject with a `TimeoutError` after this long; numbers are milliseconds */
	timeout?: DurationInput;
	/** Delay between polls (default: 1000) */
	pollIntervalMs?: number;
	signal?: AbortSignal;
};

type MetadataSchema = StandardSchemaV1<unknown, object>;

type InferMetadata<T> = T extends StandardSchemaV1<any, infer O extends object> ? O : Payload;

type InvokeOptions<Metadata extends object> = Omit<
	ExecutionSpec,
	"task_key" | "payload" | "queue" | "metadata"
> & { metadata?: Metadata };

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
	TEventSchemas extends EventCatalog | undefined,
	ExtraContext extends object,
> = ConnectionOptions & {
	tasks?: TTaskSchemas;

	events?: TEventSchemas;

	context: ExtraContext;

	/** Standard Schema for execution metadata, validated wherever metadata is set. */
	metadata?: MetadataSchema;

	/** Runs around every attempt of a non-batch task. Added context is typed in handlers. */
	middleware?: readonly Middleware<any, object, any>[];

	logger?: Logger;
	/**
	 * Time source for cron schedules and time windows, corrected to database time like the system clock.
	 * Tests pass `createTestClock(sql)` from `pgconductor-js/testing`.
	 */
	clock?: { now(): Date };
	/** Disable OpenTelemetry instrumentation. The default is enabled and uses the global API provider. */
	telemetry?: false;
};

// similar to inngest client
// exposes the main createTask methods and handles types
export class Conductor<
	TTaskSchemas extends TaskSchemas<any> | undefined = undefined,
	TEventSchemas extends EventCatalog | undefined = undefined,
	ExtraContext extends object = {},
	// Inferred types from schemas
	Tasks extends readonly TaskDefinition<string, any, any, string>[] =
		InferTasksFromSchema<TTaskSchemas>,
	Events extends readonly EventDefinition<string, any, any>[] =
		InferEventsFromSchema<TEventSchemas>,
	Metadata extends object = Payload,
	MiddlewareContext extends object = {},
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

	/** @internal */
	readonly events: readonly EventDefinition<string, any, any>[];

	private constructor(
		public readonly options: ConductorOptions<TTaskSchemas, TEventSchemas, ExtraContext>,
	) {
		this.logger = options.logger || new DefaultLogger();
		this.telemetry = new Telemetry(options.telemetry !== false);
		this.events =
			options.events instanceof EventSchemas ? options.events.definitions : options.events || [];

		this.db = new DatabaseClient({ ...options, logger: this.logger });
	}

	static create<
		TTaskSchemas extends TaskSchemas<any> | undefined = undefined,
		const TEventSchemas extends EventCatalog | undefined = undefined,
		TExtraContext extends object = {},
		TMetadataSchema extends MetadataSchema | undefined = undefined,
		TAdded extends readonly object[] = readonly object[],
	>(
		options: ConnectionOptions & {
			tasks?: TTaskSchemas;
			events?: TEventSchemas;
			context: TExtraContext;
			metadata?: TMetadataSchema;
			middleware?: {
				[K in keyof TAdded]: Middleware<
					TaskContext<
						InferTasksFromSchema<TTaskSchemas>,
						InferEventsFromSchema<TEventSchemas>,
						InferMetadata<TMetadataSchema>
					> &
						TExtraContext,
					TAdded[K],
					InferMetadata<TMetadataSchema>
				>;
			};
			logger?: Logger;
			clock?: { now(): Date };
			telemetry?: false;
		},
	): Conductor<
		TTaskSchemas,
		TEventSchemas,
		TExtraContext,
		InferTasksFromSchema<TTaskSchemas>,
		InferEventsFromSchema<TEventSchemas>,
		InferMetadata<TMetadataSchema>,
		AddedContext<
			TAdded,
			TaskContext<
				InferTasksFromSchema<TTaskSchemas>,
				InferEventsFromSchema<TEventSchemas>,
				InferMetadata<TMetadataSchema>
			> &
				TExtraContext
		>
	> {
		return new Conductor(options);
	}

	/**
	 * Ensure schema is at latest version.
	 * Useful for tests to initialize schema without starting orchestrator.
	 */
	async ensureInstalled(): Promise<void> {
		const schemaManager = new SchemaManager(this.db, {});
		const signal = new AbortController().signal;
		const { shouldShutdown } = await schemaManager.ensureLatest(signal);
		if (shouldShutdown) {
			throw new Error("The database schema is newer than this version of Postgres Conductor");
		}
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
					ctx: TaskContext<Tasks, Events, Metadata> & ExtraContext & MiddlewareContext,
				) => Promise<ResolvedReturns<Tasks, TDef>>,
	): Task<
		TDef["name"],
		ResolvedQueue<TDef>,
		ResolvedPayload<Tasks, TDef>,
		ResolvedReturns<Tasks, TDef>,
		TaskContext<Tasks, Events, Metadata> & ExtraContext & MiddlewareContext,
		TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>, Events>
	> {
		const queue = definition.queue || "default";
		const schemas = this.options.tasks?.definitions.find(
			(task: TaskDefinition<string, unknown, unknown, string>) =>
				task.name === definition.name && task.queue === queue,
		);
		return Task.create<
			TDef["name"],
			ResolvedQueue<TDef>,
			ResolvedPayload<Tasks, TDef>,
			ResolvedReturns<Tasks, TDef>,
			TaskContext<Tasks, Events, Metadata> & ExtraContext & MiddlewareContext,
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
				TaskContext<Tasks, Events, Metadata> & ExtraContext & MiddlewareContext
			>,
			this.events,
			this.options.metadata,
			schemas,
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
			this.options.middleware,
			this.options.clock,
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
		opts?: InvokeOptions<Metadata>,
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
			} & InvokeOptions<Metadata>
		>,
	): Promise<string[]>;
	async invoke<const TTask extends { readonly name: string; readonly queue?: string }>(
		task: TTask,
		payloadOrItems: any,
		opts?: InvokeOptions<Metadata>,
	): Promise<string | null | string[]> {
		const taskName = task.name;
		const queue = task.queue || "default";

		if (Array.isArray(payloadOrItems)) {
			const metadata = await Promise.all(
				payloadOrItems.map((item) => this.validateMetadata(item.metadata)),
			);
			return this.telemetry.send({
				taskKey: taskName,
				queue,
				batchMessageCount: payloadOrItems.length,
				run: async () => {
					const traceContext = this.telemetry.traceContext();
					return this.db.invokeBatch(
						payloadOrItems.map((item, i) => ({
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
							metadata: metadata[i],
						})),
					);
				},
			});
		}

		const metadata = await this.validateMetadata(opts?.metadata);
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
					metadata,
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
	 * @param opts.metadata - Metadata inherited by the executions this event triggers
	 * @param opts.dedupe_key - Repeating a key for the same event name emits nothing and returns the original event ID
	 * @returns Event ID
	 */
	async emit<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		event: TName,
		payload: InferEventPayload<TDef>,
		opts?: { metadata?: Metadata; dedupe_key?: string },
	): Promise<string> {
		payload = await validateEventPayload(this.events, event, payload);
		const metadata = await this.validateMetadata(opts?.metadata);

		return this.telemetry.send({
			taskKey: EVENT_DISPATCH_TASK,
			queue: EVENT_DISPATCH_QUEUE,
			run: async (span) => {
				const id = await this.db.emitEvent({
					eventKey: event,
					payload: payload as any,
					traceContext: this.telemetry.traceContext(),
					metadata,
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
	 * Signal an execution. A running execution receives it right away in `ctx.onSignal`, any other
	 * when its handler next runs. Idempotent by signal id: resending an id stores the signal once.
	 * @returns false if the execution is completed, failed, cancelled or unknown
	 */
	async signal(executionId: string, signal: SignalMessage): Promise<boolean> {
		return this.db.signalExecution({ executionId, signal });
	}

	/**
	 * Signal the non-terminal execution of a task for `key`, or start one that receives the signal
	 * first. Runs in one transaction under a lock on (task, key), so concurrent calls for a key
	 * start at most one execution. A completed, failed or cancelled execution does not hold the key.
	 * @returns ID of the execution that received the signal
	 */
	async signalWithStart<TName extends TaskName<Tasks>, TQueue extends string = "default">(options: {
		task: TName;
		queue?: TQueue;
		key: string;
		signal: SignalMessage;
		/** Payload of the execution, used only when it starts one */
		input?: InferPayload<FindTaskByIdentifier<Tasks, TName, TQueue>>;
		priority?: number;
		metadata?: Metadata;
	}): Promise<string> {
		const queue = options.queue || "default";
		const metadata = await this.validateMetadata(options.metadata);
		return this.telemetry.send({
			taskKey: options.task,
			queue,
			run: async (span) => {
				const id = await this.db.signalWithStart({
					taskKey: options.task,
					queue,
					key: options.key,
					signal: options.signal,
					input: (options.input || {}) as Payload,
					priority: options.priority,
					traceContext: this.telemetry.traceContext(),
					metadata,
				});
				span?.setAttribute("messaging.message.id", id);
				return id;
			},
		});
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
			...(timeout === undefined ? [] : [AbortSignal.timeout(parseDuration(timeout))]),
		]);

		while (true) {
			stop.throwIfAborted();
			// The read is cancelled when stop fires, and a result read after that is not returned
			const execution = await this.db
				.getExecution(
					{
						executionId,
						task: task && { key: task.name, queue: task.queue || "default" },
					},
					{ signal: stop },
				)
				.catch((error: unknown) => {
					stop.throwIfAborted();
					throw error;
				});
			stop.throwIfAborted();
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

	private async validateMetadata(metadata: Metadata | undefined): Promise<Payload | null> {
		if (metadata === undefined) return null;
		return validateSchema(this.options.metadata, metadata, "metadata") as Promise<Payload>;
	}
}

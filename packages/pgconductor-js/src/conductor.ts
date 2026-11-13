import type { Sql } from "postgres";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import {
	DatabaseClient,
	type EventInfo,
	type ExecutionInfo,
	type ListenerInfo,
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
	type RetentionSettings,
} from "./task.js";
import {
	signalTarget,
	type TaskContext,
	type BatchTaskContext,
	type BatchTaskEvent,
	type KeyedExecution,
	type SignalMessage,
	type SignalOptions,
} from "./task-context.js";
import type { StartedExecution } from "./query-builder.js";
import {
	type FindTaskByIdentifier,
	type InferPayload,
	type InferReturns,
	type InferState,
	type NonEmptyArray,
	type TaskDefinition,
	type TaskName,
	type Trigger,
	type ValidateTriggers,
	type ValidateEventTriggers,
	validateState,
} from "./task-definition.js";
import { EVENT_DISPATCH_QUEUE, EVENT_DISPATCH_TASK } from "./event-dispatch-task.js";
import { compileListens, type Listen } from "./event-trigger-validation.js";
import { Worker, type WorkerConfig } from "./worker.js";
import { DefaultLogger, type Logger } from "./lib/logger.js";
import type { AddedContext, Middleware } from "./middleware.js";
import { waitFor } from "./lib/wait-for.js";
import { parseDuration, type DurationInput } from "./lib/duration.js";
import { SchemaManager } from "./schema-manager.js";
import { Telemetry } from "./telemetry.js";
import {
	eventFilter,
	readEvents,
	readExecution,
	readExecutions,
	readListeners,
	type EventQuery,
	type ExecutionLookup,
	type ExecutionQuery,
	type TaskExecutionInfo,
	type TaskListenerInfo,
} from "./reads.js";

export type { EventQuery, ExecutionLookup, ExecutionQuery } from "./reads.js";
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

type ResolvedState<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TDef extends { readonly name: string; readonly queue?: string },
> = TDef["name"] extends TaskName<Tasks> ? InferState<ResolvedTaskDef<Tasks, TDef>> : Payload;

type ResolvedContext<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	Events extends readonly EventDefinition<string, any, any>[],
	Metadata extends object,
	TDef extends { readonly name: string; readonly queue?: string },
> = TaskContext<Tasks, Events, Metadata, ResolvedState<Tasks, TDef>, ResolvedReturns<Tasks, TDef>>;

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

	/**
	 * How long emitted events are kept after dispatch, for `getEvent()` and `listEvents()` and to
	 * remember `dedupe_key`s; default one day.
	 */
	eventRetention?: RetentionSettings;

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
			eventRetention?: RetentionSettings;
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
		AddedContext<TAdded>
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
			readonly actor?: boolean;
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
					ctx: ResolvedContext<Tasks, Events, Metadata, TDef> & ExtraContext & MiddlewareContext,
					// An activation of an actor returns nothing: it completes with ctx.complete()
				) => Promise<TDef extends { readonly actor: true } ? void : ResolvedReturns<Tasks, TDef>>,
	): Task<
		TDef["name"],
		ResolvedQueue<TDef>,
		ResolvedPayload<Tasks, TDef>,
		ResolvedReturns<Tasks, TDef>,
		ResolvedContext<Tasks, Events, Metadata, TDef> & ExtraContext & MiddlewareContext,
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
			ResolvedContext<Tasks, Events, Metadata, TDef> & ExtraContext & MiddlewareContext,
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
				ResolvedContext<Tasks, Events, Metadata, TDef> & ExtraContext & MiddlewareContext
			>,
			this.events,
			this.options.metadata,
			schemas,
			this.options.tasks?.definitions,
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

	/** Read an emitted event. Returns null for an unknown id or an event removed by retention. */
	async getEvent(eventId: string): Promise<EventInfo | null> {
		return this.db.getEvent(eventId);
	}

	/**
	 * Read the kept events named `name`, in the order they were stored (`order: "desc"` for newest
	 * first), optionally only those whose payload contains `where` or that were stored after
	 * `since`, or after or before a kept event. Defaults to the first 100.
	 */
	async listEvents<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		query: EventQuery<TName, InferEventPayload<TDef>>,
	): Promise<EventInfo<TName, InferEventPayload<TDef>>[]> {
		const events = await readEvents(this.db, query);
		return events as EventInfo<TName, InferEventPayload<TDef>>[];
	}

	/** Count the kept events `listEvents()` would read, without its order, limit and offset. */
	async countEvents<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		query: Omit<EventQuery<TName, InferEventPayload<TDef>>, "order" | "limit" | "offset">,
	): Promise<number> {
		return this.db.countEvents(eventFilter(query));
	}

	async cancel(executionId: string, options?: { reason?: string }): Promise<boolean> {
		return this.db.cancelExecution(executionId, options || {});
	}

	/**
	 * Signal an execution by id, or the open execution of `{ task, key }` that holds the key (see
	 * `signalWithStart()`). A running execution receives it right away in `ctx.onSignal`, any other
	 * when its handler next runs. Idempotent by signal id: resending an id stores the signal once.
	 * By key, it runs under the lock `signalWithStart()` takes, so it waits for one starting the
	 * execution for that key.
	 *
	 * With `at`, the signal is stored now and delivered at that time: a running execution receives
	 * it with the first poll after it, an idle actor wakes then (or at its alarm, if earlier).
	 * @returns false if the execution is completed, failed, cancelled or unknown, or no open
	 * execution holds the key
	 */
	async signal<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		target: string | KeyedExecution<TName, TQueue>,
		signal: SignalMessage,
		options: SignalOptions = {},
	): Promise<boolean> {
		return this.db.signalExecution({
			...signalTarget(target),
			signal: { ...signal, at: options.at, traceContext: this.telemetry.traceContext() },
		});
	}

	/**
	 * Signal the non-terminal execution of a task for `key`, or start one that receives the signal
	 * first. Runs in one transaction under a lock on (task, key), so concurrent calls for a key
	 * start at most one execution. A completed, failed or cancelled execution does not hold the key.
	 * An exclusive listen in `listen` whose name another open execution of the task holds throws a
	 * `ListenHeldError` and starts nothing.
	 * @returns ID of the execution that received the signal, and whether this call started it
	 */
	async signalWithStart<TName extends TaskName<Tasks>, TQueue extends string = "default">(options: {
		task: TName;
		queue?: TQueue;
		key: string;
		signal: SignalMessage;
		/** Payload of the execution, used only when it starts one */
		input?: InferPayload<FindTaskByIdentifier<Tasks, TName, TQueue>>;
		/**
		 * State of the execution (see `ctx.setState()`), used only when it starts one. Validated
		 * against the task's state schema.
		 */
		state?: InferState<FindTaskByIdentifier<Tasks, TName, TQueue>>;
		/** Listens of the execution by name (see `ctx.listen()`), registered only when it starts one */
		listen?: Record<string, Listen>;
		priority?: number;
		metadata?: Metadata;
	}): Promise<StartedExecution> {
		const queue = options.queue || "default";
		const metadata = await this.validateMetadata(options.metadata);
		const state =
			options.state &&
			(await validateState(
				this.options.tasks?.definitions || [],
				{ name: options.task, queue },
				options.state,
			));
		return this.telemetry.send({
			taskKey: options.task,
			queue,
			run: async (span) => {
				const started = await this.db.signalWithStart({
					taskKey: options.task,
					queue,
					key: options.key,
					signal: options.signal,
					input: (options.input || {}) as Payload,
					state,
					listens: compileListens(options.listen || {}, this.events),
					priority: options.priority,
					traceContext: this.telemetry.traceContext(),
					metadata,
				});
				span?.setAttribute("messaging.message.id", started.id);
				return started;
			},
		});
	}

	/**
	 * Read an execution by id, the open execution of a task holding a key (`{ task, key }`, see
	 * `signalWithStart()`), or the execution of a task invoked with a dedupe key
	 * (`{ task, dedupeKey }`). Returns null when none matches or the execution was removed by
	 * retention. Pass the task to type its payload, state and result.
	 */
	async getExecution(executionId: string): Promise<ExecutionInfo | null>;
	async getExecution<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		task: TaskIdentifier<TName, TQueue>,
		executionId: string,
	): Promise<TaskExecutionInfo<FindTaskByIdentifier<Tasks, TName, TQueue>> | null>;
	async getExecution<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		lookup: ExecutionLookup<TName, TQueue>,
	): Promise<TaskExecutionInfo<FindTaskByIdentifier<Tasks, TName, TQueue>> | null>;
	async getExecution(
		target: TaskIdentifier<string, string> | ExecutionLookup<string, string> | string,
		executionId?: string,
	): Promise<ExecutionInfo | null> {
		return readExecution(this.db, target, executionId);
	}

	/**
	 * List a task's executions in the order they were created (or newest first with `order: "desc"`),
	 * with their payloads and states: only open ones (pending, waiting or running) with `open`, only
	 * those whose payload contains `where`. Executions removed by retention are gone. Defaults to the
	 * first 100.
	 */
	async listExecutions<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		query: ExecutionQuery<TName, TQueue, InferPayload<FindTaskByIdentifier<Tasks, TName, TQueue>>>,
	): Promise<TaskExecutionInfo<FindTaskByIdentifier<Tasks, TName, TQueue>>[]>;
	async listExecutions(query: ExecutionQuery<string, string, Payload>): Promise<ExecutionInfo[]> {
		return readExecutions(this.db, query);
	}

	/**
	 * The open executions an event named `event` with `payload` would reach through their listens
	 * (`ctx.listen()`), each with `matched`, the names of the listens it would reach, in the order
	 * they were created. Matches as dispatch does, and emits nothing. Pass `task` to read only its
	 * executions, typed by its definition. Defaults to the first 100.
	 */
	async listListeners<TName extends TaskName<Tasks>, TQueue extends string = "default">(query: {
		event: EventName<Events>;
		payload: Payload;
		task: TaskIdentifier<TName, TQueue>;
		limit?: number;
	}): Promise<TaskListenerInfo<FindTaskByIdentifier<Tasks, TName, TQueue>>[]>;
	async listListeners(query: {
		event: EventName<Events>;
		payload: Payload;
		task?: undefined;
		limit?: number;
	}): Promise<ListenerInfo[]>;
	async listListeners(query: {
		event: string;
		payload: Payload;
		task?: TaskIdentifier<string, string>;
		limit?: number;
	}): Promise<ListenerInfo[]> {
		return readListeners(this.db, query);
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

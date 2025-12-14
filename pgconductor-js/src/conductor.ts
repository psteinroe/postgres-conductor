import type { Sql } from "postgres";
import { DatabaseClient, type ExecutionSpec } from "./database-client";
import {
	Task,
	type TaskConfiguration,
	type AnyTask,
	type TaskEventFromTriggers,
	type ValidateTasksQueue,
	type BatchConfig,
	type ExecuteFunction,
} from "./task";
import type { TaskContext, BatchTaskContext } from "./task-context";
import {
	type FindTaskByIdentifier,
	type InferPayload,
	type InferReturns,
	type NonEmptyArray,
	type TaskDefinition,
	type TaskName,
	type Trigger,
	type ValidateTriggers,
} from "./task-definition";
import { Worker, type WorkerConfig } from "./worker";
import { DefaultLogger, type Logger } from "./lib/logger";
import { SchemaManager } from "./schema-manager";
// import type {
// 	CustomEventConfig,
// 	EventDefinition,
// 	EventName,
// 	FindEventByIdentifier,
// 	GenericDatabase,
// 	InferEventPayload,
// } from "./event-definition";
import {
	TaskSchemas,
	// EventSchemas,
	// DatabaseSchema,
	type InferTasksFromSchema,
	// type InferEventsFromSchema,
	// type InferDatabaseFromSchema,
} from "./schemas";

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

// type ResolvedEvent<
// 	Tasks extends readonly TaskDefinition<string, any, any, string>[],
// 	Events extends readonly EventDefinition<string, any>[],
// 	Database extends GenericDatabase,
// 	TDef extends { readonly name: string; readonly queue?: string },
// 	TTriggers,
// > = TaskEventFromTriggers<
// 	TTriggers,
// 	ResolvedPayload<Tasks, TDef>,
// 	Events,
// 	Database
// >;

export type ConductorOptions<
	TTaskSchemas extends TaskSchemas<any> | undefined,
	// TEventSchemas extends EventSchemas<any> | undefined,
	// TDatabaseSchema extends DatabaseSchema<any> | undefined,
	ExtraContext extends object,
> = ConnectionOptions & {
	tasks?: TTaskSchemas;

	// events?: TEventSchemas;

	// database?: TDatabaseSchema;

	context: ExtraContext;

	logger?: Logger;
};

// similar to inngest client
// exposes the main createTask methods and handles types
export class Conductor<
	TTaskSchemas extends TaskSchemas<any> | undefined = undefined,
	// TEventSchemas extends EventSchemas<any> | undefined = undefined,
	// TDatabaseSchema extends DatabaseSchema<any> | undefined = undefined,
	ExtraContext extends object = {},
	// Inferred types from schemas
	Tasks extends readonly TaskDefinition<string, any, any, string>[] =
		InferTasksFromSchema<TTaskSchemas>,
	// Events extends readonly EventDefinition<
	// 	string,
	// 	any
	// >[] = InferEventsFromSchema<TEventSchemas>,
	// Database extends GenericDatabase = InferDatabaseFromSchema<TDatabaseSchema>,
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

	private constructor(
		public readonly options: ConductorOptions<
			TTaskSchemas,
			// TEventSchemas,
			// TDatabaseSchema,
			ExtraContext
		>,
	) {
		this.logger = options.logger || new DefaultLogger();

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
		// TEventSchemas extends EventSchemas<any> | undefined = undefined,
		// TDatabaseSchema extends DatabaseSchema<any> | undefined = undefined,
		TExtraContext extends object = {},
	>(
		options: ConnectionOptions & {
			tasks?: TTaskSchemas;
			// events?: TEventSchemas;
			// database?: TDatabaseSchema;
			context: TExtraContext;
			logger?: Logger;
		},
	): Conductor<TTaskSchemas, /* TEventSchemas, TDatabaseSchema, */ TExtraContext> {
		return new Conductor<
			TTaskSchemas,
			// TEventSchemas,
			// TDatabaseSchema,
			TExtraContext
		>(options);
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

	createTask<
		const TDef extends {
			readonly name: string;
			readonly queue?: string;
			readonly batch?: BatchConfig;
		},
		const TTriggers extends object | readonly object[],
	>(
		definition: TDef,
		triggers: TTriggers &
			ValidateTriggers<
				Tasks,
				// Events,
				TDef["name"],
				TTriggers,
				ResolvedQueue<TDef>
			>,
		fn: TDef extends { readonly batch: BatchConfig }
			? ResolvedReturns<Tasks, TDef> extends void
				? (
						events: Array<TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>>>,
						ctx: BatchTaskContext,
					) => Promise<void>
				: (
						events: Array<TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>>>,
						ctx: BatchTaskContext,
					) => Promise<Array<ResolvedReturns<Tasks, TDef>>>
			: (
					event: TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>>,
					ctx: TaskContext<Tasks> & ExtraContext,
				) => Promise<ResolvedReturns<Tasks, TDef>>,
	): Task<
		TDef["name"],
		ResolvedQueue<TDef>,
		ResolvedPayload<Tasks, TDef>,
		ResolvedReturns<Tasks, TDef>,
		TaskContext<Tasks> & ExtraContext,
		TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>>
	> {
		return Task.create<
			TDef["name"],
			ResolvedQueue<TDef>,
			ResolvedPayload<Tasks, TDef>,
			ResolvedReturns<Tasks, TDef>,
			TaskContext<Tasks> & ExtraContext,
			TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>>
		>(
			definition as TaskConfiguration<TDef["name"], ResolvedQueue<TDef>>,
			triggers as NonEmptyArray<Trigger> | Trigger,
			fn as ExecuteFunction<
				TaskEventFromTriggers<TTriggers, ResolvedPayload<Tasks, TDef>>,
				ResolvedReturns<Tasks, TDef>,
				TaskContext<Tasks> & ExtraContext
			>,
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
		return new Worker<Tasks>(
			options.queue,
			options.tasks as AnyTask[],
			this.db,
			this.logger,
			options.config,
			this.options.context,
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
			const specs = payloadOrItems.map((item) => ({
				task_key: taskName,
				queue,
				payload: item.payload,
				run_at: item.run_at,
				dedupe_key: item.dedupe_key,
				throttle: item.throttle,
				debounce: item.debounce,
				cron_expression: item.cron_expression,
				priority: item.priority,
			}));
			return this.db.invokeBatch(specs);
		}

		return this.db.invoke({
			task_key: taskName,
			queue,
			payload: payloadOrItems,
			...opts,
		});
	}

	// async emit<
	// 	TName extends EventName<Events>,
	// 	TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<
	// 		Events,
	// 		TName
	// 	>,
	// >(
	// 	{ event }: CustomEventConfig<TName>,
	// 	payload: InferEventPayload<TDef>,
	// ): Promise<string> {
	// 	return this.db.emitEvent({ eventKey: event, payload });
	// }

	async cancel(executionId: string, options?: { reason?: string }): Promise<boolean> {
		return this.db.cancelExecution(executionId, options || {});
	}
}

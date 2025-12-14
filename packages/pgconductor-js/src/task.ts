import type {
	NonEmptyArray,
	Trigger,
	HasInvocable,
	HasCron,
	// HasCustomEvent,
	// HasDatabaseEvent,
	CronTrigger,
	// CustomEventTrigger,
} from "./task-definition";
// import type {
// 	EventDefinition,
// 	FindEventByIdentifier,
// 	InferEventPayload,
// 	GenericDatabase,
// 	DatabaseEventPayload,
// 	SchemaName,
// 	TableName,
// 	RowType,
// } from "./event-definition";

export type TaskIdentifier<TName extends string = string, TQueue extends string = "default"> = {
	readonly name: TName;
	readonly queue?: TQueue;
};

export type BatchConfig = {
	size: number;
	timeoutMs: number;
};

export type TaskConfiguration<
	TName extends string = string,
	TQueue extends string = "default",
> = TaskIdentifier<TName, TQueue> & {
	maxAttempts?: number;
	window?: [string, string];
	removeOnComplete?: RetentionSettings;
	removeOnFail?: RetentionSettings;
	concurrency?: number;
	batch?: BatchConfig;
};

export type RetentionSettings = boolean | { days: number };

export type TaskEvent<P extends object = object> =
	| { event: "pgconductor.cron" }
	| { event: "pgconductor.invoke"; payload: P };

// Extract cron triggers from array
type ExtractCronTriggers<TTriggers> = TTriggers extends readonly any[]
	? Extract<TTriggers[number], CronTrigger>
	: TTriggers extends CronTrigger
		? TTriggers
		: never;

// Extract custom event triggers from array
// type ExtractCustomEventTriggers<TTriggers> = TTriggers extends readonly any[]
// 	? Extract<TTriggers[number], CustomEventTrigger>
// 	: TTriggers extends CustomEventTrigger
// 		? TTriggers
// 		: never;

// Extract database event triggers from array
// type ExtractDatabaseEventTriggers<TTriggers> = TTriggers extends readonly any[]
// 	? TTriggers[number] extends infer T
// 		? T extends { schema: string; table: string; operation: "insert" | "update" | "delete" }
// 			? T
// 			: never
// 		: never
// 	: TTriggers extends { schema: string; table: string; operation: "insert" | "update" | "delete" }
// 		? TTriggers
// 		: never;

// Build cron event union from triggers (extracts schedule names)
type CronEventUnion<TTriggers> =
	ExtractCronTriggers<TTriggers> extends infer T
		? T extends { name: infer TName extends string }
			? { event: TName }
			: never
		: never;

// Build custom event union from triggers
// type CustomEventUnion<TTriggers, Events extends readonly EventDefinition<string, any>[]> =
// 	ExtractCustomEventTriggers<TTriggers> extends infer T
// 		? T extends { event: infer TName extends string }
// 			? FindEventByIdentifier<Events, TName> extends infer TEvent
// 				? TEvent extends EventDefinition<string, any>
// 					? { event: TName; payload: InferEventPayload<TEvent> }
// 					: { event: TName; payload: {} }
// 				: { event: TName; payload: {} }
// 			: never
// 		: never;

// Build database event union from triggers
// type DatabaseEventUnion<TTriggers, Database extends GenericDatabase> =
// 	ExtractDatabaseEventTriggers<TTriggers> extends infer T
// 		? T extends { schema: infer TSchema extends string }
// 			? T extends { table: infer TTable extends string }
// 				? T extends { operation: infer TOp extends "insert" | "update" | "delete" }
// 					? TSchema extends SchemaName<Database>
// 						? TTable extends TableName<Database, TSchema>
// 							? {
// 									event: `${TSchema}.${TTable}.${TOp}`;
// 									payload: DatabaseEventPayload<
// 										RowType<Database, TSchema, TTable>,
// 										TOp,
// 										T extends { columns: infer TColumns extends string } ? TColumns : undefined
// 									>;
// 								}
// 							: {
// 									event: `${TSchema}.${TTable}.${TOp}`;
// 									payload: {
// 										old: any;
// 										new: any;
// 										tg_table: string;
// 										tg_op: string;
// 									};
// 								}
// 						: {
// 								event: `${TSchema}.${TTable}.${TOp}`;
// 								payload: {
// 									old: any;
// 									new: any;
// 									tg_table: string;
// 									tg_op: string;
// 								};
// 							}
// 					: never
// 				: never
// 			: never
// 		: never;

// Conditional event type based on triggers
export type TaskEventFromTriggers<
	TTriggers,
	TPayload extends object,
	// Events extends readonly EventDefinition<string, any>[] = [],
	// Database extends GenericDatabase = {},
> =
	| (HasInvocable<TTriggers> extends true
			? { event: "pgconductor.invoke"; payload: TPayload }
			: never)
	| (HasCron<TTriggers> extends true ? CronEventUnion<TTriggers> : never);
// | (HasCustomEvent<TTriggers> extends true ? CustomEventUnion<TTriggers, Events> : never)
// | (HasDatabaseEvent<TTriggers> extends true ? DatabaseEventUnion<TTriggers, Database> : never);

// Conditional execute function type based on whether task has batch config
export type ExecuteFunction<
	EventType,
	Returns extends object | void,
	Context extends object,
	HasBatch extends boolean = false,
> = HasBatch extends true
	? Returns extends void
		? (events: EventType[], context: Context) => Promise<void>
		: (events: EventType[], context: Context) => Promise<Returns[]>
	: (event: EventType, context: Context) => Promise<Returns>;

// Represents a task definition that can be invoked or triggered by events
export class Task<
	Key extends string = string,
	Queue extends string = "default",
	Payload extends object = object,
	Returns extends object | void = void,
	Context extends object = object,
	EventType = TaskEvent<Payload>,
> {
	public readonly name: Key;
	public readonly queue: Queue;
	public readonly maxAttempts?: number;
	public readonly window?: [string, string];
	public readonly removeOnComplete: RetentionSettings;
	public readonly removeOnFail: RetentionSettings;
	public readonly concurrency?: number;
	public readonly batch?: BatchConfig;

	public readonly triggers: NonEmptyArray<Trigger>;

	constructor(
		definition: TaskConfiguration<Key, Queue>,
		triggers: NonEmptyArray<Trigger> | Trigger,
		public readonly execute: ExecuteFunction<EventType, Returns, Context>,
	) {
		const { name, queue, ...config } = definition;
		this.name = name;
		this.queue = (queue || "default") as Queue;

		this.maxAttempts = config.maxAttempts;
		this.window = config.window;
		this.removeOnComplete = config.removeOnComplete ?? false;
		this.removeOnFail = config.removeOnFail ?? false;
		this.concurrency = config.concurrency;
		this.batch = config.batch;

		this.triggers = Array.isArray(triggers) ? triggers : [triggers];
	}

	static create<
		Key extends string,
		Queue extends string,
		Payload extends object,
		Returns extends object | void,
		Context extends object,
		EventType,
	>(
		definition: TaskConfiguration<Key, Queue>,
		triggers: NonEmptyArray<Trigger> | Trigger,
		execute: ExecuteFunction<EventType, Returns, Context>,
	): Task<Key, Queue, Payload, Returns, Context, EventType> {
		return new Task<Key, Queue, Payload, Returns, Context, EventType>(
			definition,
			triggers,
			execute,
		);
	}
}

export type AnyTask = Task<string, string, any, any, any, any>; // Task<Key, Queue, ...>

// Type-level check that all tasks in array belong to a specific queue
export type ValidateTasksQueue<
	TQueue extends string,
	TTasks extends readonly Task<any, any, any, any, any, any>[],
> = TTasks extends readonly Task<any, infer Q, any, any, any, any>[]
	? Q extends TQueue
		? TTasks
		: `All tasks must belong to queue "${TQueue}". Found task with queue "${Q & string}".`
	: TTasks;

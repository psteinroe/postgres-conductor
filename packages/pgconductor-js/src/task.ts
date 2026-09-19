import type {
	NonEmptyArray,
	Trigger,
	HasInvocable,
	HasCron,
	HasCustomEvent,
	CronTrigger,
} from "./task-definition";
import type { EventDefinition, FindEventByIdentifier, InferEventPayload } from "./event-definition";
import type { SelectedRow } from "./select-columns";
import * as assert from "./lib/assert";

export type TaskIdentifier<TName extends string = string, TQueue extends string = "default"> = {
	readonly name: TName;
	readonly queue?: TQueue;
};

type QualifiedTaskIdentifier = Required<TaskIdentifier<string, string>>;

function hasSameTaskIdentity(
	left: QualifiedTaskIdentifier,
	right: QualifiedTaskIdentifier,
): boolean {
	return left.queue === right.queue && left.name === right.name;
}

export type BatchConfig = {
	size: number;
	timeoutMs: number;
};

export type DeadLetterConfiguration<TPayload extends object = object> = {
	readonly queue: string;
	readonly task?: Task<string, string, TPayload, object | void, object, unknown>;
};

export type TaskConfiguration<
	TName extends string = string,
	TQueue extends string = "default",
	TPayload extends object = object,
> = TaskIdentifier<TName, TQueue> & {
	maxAttempts?: number;
	window?: [string, string];
	removeOnComplete?: RetentionSettings;
	removeOnFail?: RetentionSettings;
	concurrency?: number;
	groupConcurrency?: number;
	batch?: BatchConfig;
	deadLetter?: DeadLetterConfiguration<TPayload>;
};

/** Type-level validation for a dead-letter destination. */
type ExactType<TLeft, TRight> = [TLeft] extends [TRight]
	? [TRight] extends [TLeft]
		? true
		: false
	: false;

type ValidateDeadLetterTarget<TTarget, TPayload extends object, TQueue> =
	TTarget extends Task<any, infer TTargetQueue, infer TTargetPayload, any, any, any>
		? TPayload extends TTargetPayload
			? ExactType<TQueue, TTargetQueue> extends true
				? unknown
				: "deadLetter.queue must match deadLetter.task.queue"
			: "deadLetter.task must accept the source task payload"
		: "deadLetter.task must be a Task";

export type ValidateDeadLetterConfiguration<T, TPayload extends object> = T extends {
	readonly deadLetter: infer TDeadLetter;
}
	? TDeadLetter extends { readonly queue: infer TQueue }
		? TQueue extends string
			? TDeadLetter extends { readonly task: infer TTarget }
				? ValidateDeadLetterTarget<TTarget, TPayload, TQueue>
				: unknown
			: "deadLetter.queue must be a string"
		: "deadLetter must include a queue string"
	: unknown;

export type RetentionSettings = boolean | { days: number };

export type TaskEvent<P extends object = object> =
	| { name: "pgconductor.cron" }
	| { name: "pgconductor.invoke"; payload: P };

// Extract cron triggers from array
type ExtractCronTriggers<TTriggers> = TTriggers extends readonly any[]
	? Extract<TTriggers[number], CronTrigger>
	: TTriggers extends CronTrigger
		? TTriggers
		: never;

// Extract custom event triggers from array.
type ExtractCustomEventTriggers<TTriggers> = TTriggers extends readonly any[]
	? Extract<TTriggers[number], { event: string }>
	: TTriggers extends { event: string }
		? TTriggers
		: never;

// Build cron event union from triggers (extracts schedule names)
type CronEventUnion<TTriggers> =
	ExtractCronTriggers<TTriggers> extends infer T
		? T extends { name: infer TName extends string }
			? { name: TName }
			: never
		: never;

// Build custom event union from triggers
type CustomEventUnion<TTriggers, Events extends readonly EventDefinition<string, any, any>[]> =
	ExtractCustomEventTriggers<TTriggers> extends infer T
		? T extends { event: infer TName extends string }
			? FindEventByIdentifier<Events, TName> extends infer TEvent
				? TEvent extends EventDefinition<string, any, any>
					? T extends { fields: infer TFields extends string }
						? {
								name: TName;
								payload: SelectedRow<InferEventPayload<TEvent>, TFields>;
							}
						: { name: TName; payload: InferEventPayload<TEvent> }
					: { name: TName; payload: {} }
				: { name: TName; payload: {} }
			: never
		: never;

// Conditional event type based on triggers
export type TaskEventFromTriggers<
	TTriggers,
	TPayload extends object,
	Events extends readonly EventDefinition<string, any, any>[] = [],
> =
	| (HasInvocable<TTriggers> extends true
			? { name: "pgconductor.invoke"; payload: TPayload }
			: never)
	| (HasCron<TTriggers> extends true ? CronEventUnion<TTriggers> : never)
	| (HasCustomEvent<TTriggers> extends true ? CustomEventUnion<TTriggers, Events> : never);

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
	public readonly groupConcurrency?: number;
	public readonly batch?: BatchConfig;
	public readonly deadLetter?: DeadLetterConfiguration<Payload>;

	public readonly triggers: NonEmptyArray<Trigger>;

	constructor(
		definition: TaskConfiguration<Key, Queue, Payload>,
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
		this.concurrency = assert.positiveInteger(config.concurrency, "concurrency");
		this.groupConcurrency = assert.positiveInteger(config.groupConcurrency, "groupConcurrency");
		this.batch = config.batch;
		this.deadLetter = config.deadLetter;
		if (
			this.deadLetter &&
			hasSameTaskIdentity(this, {
				queue: this.deadLetter.queue,
				name: this.deadLetter.task?.name ?? this.name,
			})
		) {
			throw new Error("A task cannot dead-letter directly to itself");
		}

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
		definition: TaskConfiguration<Key, Queue, Payload>,
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

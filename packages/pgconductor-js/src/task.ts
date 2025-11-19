import type {
	NonEmptyArray,
	Trigger,
	HasInvocable,
	HasCron,
} from "./task-definition";

export type TaskIdentifier<
	TName extends string = string,
	TQueue extends string = "default",
> = {
	readonly name: TName;
	readonly queue?: TQueue;
};

export type TaskConfiguration<
	TName extends string = string,
	TQueue extends string = "default",
> = TaskIdentifier<TName, TQueue> & {
	maxAttempts?: number;
	window?: [string, string];
	removeOnComplete?: RetentionSettings;
	removeOnFail?: RetentionSettings;
};

export type RetentionSettings = boolean | { days: number };

export type TaskEvent<P extends object = object> =
	| { event: "pgconductor.cron" }
	| { event: "pgconductor.invoke"; payload: P };

// Conditional event type based on triggers
export type TaskEventFromTriggers<
	TTriggers,
	TPayload extends object,
> = HasInvocable<TTriggers> extends true
	? HasCron<TTriggers> extends true
		? TaskEvent<TPayload> // Both invocable and cron
		: { event: "pgconductor.invoke"; payload: TPayload } // Only invocable
	: { event: "pgconductor.cron" }; // Only cron

export type ExecuteFunction<
	EventType,
	Returns extends object | void,
	Context extends object,
> = (event: EventType, context: Context) => Promise<Returns>;

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

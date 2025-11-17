import type {
	NonEmptyArray,
	Trigger,
	HasInvocable,
	HasCron,
} from "./task-definition";

export type TaskConfiguration<
	TName extends string = string,
	TQueue extends string = "default",
> = {
	// identifier
	name: TName;
	queue?: TQueue;

	// configuration
	maxAttempts?: number;
	window?: [string, string];
};

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
	Queue extends string = "default",
	Key extends string = string,
	Payload extends object = object,
	Returns extends object | void = void,
	Context extends object = object,
	EventType = TaskEvent<Payload>,
> {
	public readonly name: Key;
	public readonly queue: Queue;
	public readonly maxAttempts?: number;
	public readonly window?: [string, string];
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
		this.triggers = Array.isArray(triggers) ? triggers : [triggers];
	}
}

export type AnyTask = Task<string, string, any, any, any, any>;

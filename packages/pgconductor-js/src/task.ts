export type TaskConfig = {
	maxAttempts?: number;
	partition?: boolean;
	window?: [string, string];
	flushInterval?: number;
	pollInterval?: number;
};

export type TaskConfiguration<
	TName extends string = string,
	TQueue extends string = "default",
> = {
	name: TName;
	queue?: TQueue;
} & TaskConfig;

// Represents a task definition that can be invoked or triggered by events
export type ExecuteFunction<
	Payload extends object,
	Returns extends object | void,
	Context extends object,
> = (payload: Payload, context: Context) => Promise<Returns>;

// Represents a task definition that can be invoked or triggered by events
export class Task<
	Queue extends string = "default",
	Key extends string = string,
	Payload extends object = object,
	Returns extends object | void = void,
	Context extends object = object,
> {
	public readonly key: Key;
	public readonly queue: Queue;
	public readonly config: TaskConfig;

	constructor(
		definition: TaskConfiguration<Key, Queue>,
		public readonly execute: ExecuteFunction<Payload, Returns, Context>,
	) {
		this.key = definition.name;
		this.queue = (definition.queue ?? "default") as Queue;
		const { name, queue, ...config } = definition;
		this.config = config;
	}
}

export type AnyTask = Task<string, string, any, any, any>;

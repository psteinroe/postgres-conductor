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
	public readonly name: Key;
	public readonly queue: Queue;
	public readonly maxAttempts?: number;
	public readonly window?: [string, string];

	constructor(
		definition: TaskConfiguration<Key, Queue>,
		public readonly execute: ExecuteFunction<Payload, Returns, Context>,
	) {
		const { name, queue, ...config } = definition;
		this.name = name;
		this.queue = (queue || "default") as Queue;

		this.maxAttempts = config.maxAttempts;
		this.window = config.window;
	}
}

export type AnyTask = Task<string, string, any, any, any>;

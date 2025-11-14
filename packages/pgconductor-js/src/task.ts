export type TaskConfig = {
	maxAttempts?: number;
	partition?: boolean;
	window?: [string, string];
	flushInterval?: number;
	pollInterval?: number;
};

// Represents a task definition that can be invoked or triggered by events
export type ExecuteFunction<
	Payload extends object,
	Returns extends object | void,
	Context extends object,
> = (payload: Payload, context: Context) => Promise<Returns>;

// Represents a task definition that can be invoked or triggered by events
export class Task<
	Key extends string,
	Payload extends object,
	Returns extends object | void,
	Context extends object,
> {
	constructor(
		public readonly key: Key,
		public readonly execute: ExecuteFunction<Payload, Returns, Context>,
		public readonly config: TaskConfig = {},
	) {}
}

export type AnyTask = Task<string, any, any, any>;

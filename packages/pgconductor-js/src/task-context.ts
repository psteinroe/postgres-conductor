import type { DatabaseClient } from "./database-client";

export type TaskContextOptions = {
	signal: AbortSignal;
    abortController: AbortController;
	db: DatabaseClient;
};

const hangup = <T>() => new Promise<T>(() => {});

// second argument for tasks
export class TaskContext {
	// should receive AbortSignal for cancellation
	constructor(private readonly opts: TaskContextOptions) {}

	static create<Extra extends object>(
		opts: TaskContextOptions,
		extra?: Extra,
	): TaskContext & Extra {
		const base = new TaskContext(opts);
		return Object.assign(base, extra);
	}

	async step<T>(name: string, fn: () => Promise<T> | T): Promise<T> {
		// check if step is already completed

		// execute if not

		// store result

		return fn();
	}

	async checkpoint(): Promise<void> {
		// just marks progress, doesn't hangup
	}

	// use ms package
	async sleep(id: string | number, ms: number): Promise<void> {}

	async sleepUntil(id: string, datetime: Date | string): Promise<void> {}

	async invoke<T>(name: string, task: string): Promise<T> {
		return {} as T;
	}

	// async waitForEvent(name: string): Promise<void> {}
	//
	// async emitEvent(name: string): Promise<void> {}
}

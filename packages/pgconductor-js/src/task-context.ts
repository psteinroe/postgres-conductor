import type { DatabaseClient } from "./database-client";

export type TaskContextOptions = {
	signal: AbortSignal;
	db: DatabaseClient;
};

// second argument for tasks
export class TaskContext {
	// ctx.step()
	// ctx.checkpoint()
	// ...dependencies
	private _aborted: boolean = false;

	// should receive AbortSignal for cancellation
	constructor(private readonly opts: TaskContextOptions) {}

	static create<Extra extends object>(
		opts: TaskContextOptions,
		extra?: Extra,
	): TaskContext & Extra {
		const base = new TaskContext(opts);
		return Object.assign(base, extra);
	}

	get aborted(): boolean {
		return this._aborted;
	}

	async step<T>(name: string, fn: () => Promise<T> | T): Promise<T> {
		if (this.opts.signal.aborted) {
			this._aborted = true;
			return new Promise(() => {});
		}

		// check if step is already completed

		// execute if not

		// store result

		return fn();
	}

	async checkpoint(): Promise<void> {
		if (this.opts.signal.aborted) {
			this._aborted = true;
			return new Promise(() => {});
		}
	}

	// use ms package
	async sleep(id: string | number, ms: number): Promise<void> {}

	async sleepUntil(id: string, datetime: Date | string): Promise<void> {}

	async invoke<T>(name: string, task: string): Promise<T> {
		return {} as T;
	}

	async waitForEvent(name: string): Promise<void> {}

	async emitEvent(name: string): Promise<void> {}
}

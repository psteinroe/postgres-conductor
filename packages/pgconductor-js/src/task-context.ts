// second argument for tasks
export class TaskContext {
	// ctx.step()
	// ctx.checkpoint()
	// ...dependencies

	// should receive AbortSignal for cancellation
	constructor({ signal }: { signal: AbortSignal }) {}

	static create<Extra extends object>(
		signal: AbortSignal,
		extra?: Extra,
	): TaskContext & Extra {
		const base = new TaskContext({ signal });
		return Object.assign(base, extra);
	}

	async step<T>(name: string, fn: () => Promise<T> | T): Promise<T> {
		// implement step tracking logic here
		return fn();
	}

	async checkpoint(): Promise<void> {
		// shutdown if aborted
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

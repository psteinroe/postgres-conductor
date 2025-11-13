import type { DatabaseClient, Payload, JsonValue } from "./database-client";

export type TaskContextOptions = {
	signal: AbortSignal;
	abortController: AbortController;
	db: DatabaseClient;
	executionId: string;
};

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

	async step<T extends JsonValue | void>(
		name: string,
		fn: () => Promise<T> | T,
	): Promise<T> {
		// Check if step already completed
		const cached = await this.opts.db.loadStep(
			this.opts.executionId,
			name,
			this.opts.signal,
		);

		if (cached) {
			return cached.result;
		}

		// Execute and save
		const result = await fn();

		await this.opts.db.saveStep(
			this.opts.executionId,
			name,
			{ result },
			undefined,
			this.opts.signal,
		);

		return result;
	}

	async checkpoint(): Promise<void> {
		if (this.opts.signal.aborted) {
			return this.abortAndHangup();
		}
	}

	private abortAndHangup(): Promise<never> {
		this.opts.abortController.abort();
		return new Promise(() => {});
	}

	async sleep(id: string, ms: number): Promise<void> {
		return this.sleepUntil(id, new Date(Date.now() + ms));
	}

	async sleepUntil(id: string, datetime: Date | string): Promise<void> {
		// Check if we already slept
		const cached = await this.opts.db.loadStep(
			this.opts.executionId,
			id,
			this.opts.signal,
		);

		if (cached) {
			return; // Already slept, continue
		}

		// Save sleep step with run_at
		const runAt = typeof datetime === "string" ? new Date(datetime) : datetime;
		await this.opts.db.saveStep(
			this.opts.executionId,
			id,
			null,
			runAt,
			this.opts.signal,
		);

		return this.abortAndHangup();
	}

	async invoke<T extends JsonValue | void>(
		name: string,
		task: string,
	): Promise<T> {
		throw new Error("Not implemented");
	}

	// async waitForEvent(name: string): Promise<void> {}
	//
	// async emitEvent(name: string): Promise<void> {}
}

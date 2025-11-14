import type {
	DatabaseClient,
	Payload,
	JsonValue,
	Execution,
} from "./database-client";

export type TaskContextOptions = {
	signal: AbortSignal;
	abortController: AbortController;
	db: DatabaseClient;
	execution: Execution;
};

// second argument for tasks
export class TaskContext {
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
			this.opts.execution.id,
			name,
			this.opts.signal,
		);

		if (cached) {
			return cached.result;
		}

		// Execute and save
		const result = await fn();

		await this.opts.db.saveStep(
			this.opts.execution.id,
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

	/**
	 * Abort the current execution and hang up indefinitely
	 * @return Promise that never resolves
	 **/
	private abortAndHangup(): Promise<never> {
		this.opts.abortController.abort();
		return new Promise(() => {});
	}

	async sleep(id: string, ms: number): Promise<void> {
		// Check if we already slept
		const cached = await this.opts.db.loadStep(
			this.opts.execution.id,
			id,
			this.opts.signal,
		);

		if (cached) {
			return; // Already slept, continue
		}

		// Save sleep step with ms - time calculation happens in SQL
		await this.opts.db.saveStep(
			this.opts.execution.id,
			id,
			null,
			ms,
			this.opts.signal,
		);

		return this.abortAndHangup();
	}

	async invoke<T extends JsonValue | void>(
		id: string,
		taskKey: string,
		payload: Payload = {},
		timeout?: number,
	): Promise<T> {
		// Step is the source of truth - check if child already completed
		const cached = await this.opts.db.loadStep(
			this.opts.execution.id,
			id,
			this.opts.signal,
		);

		if (cached) {
			// Child completed successfully
			if ("result" in cached) {
				return cached.result;
			}
			// Note: errors are handled via cascade - parent would already be failed
		}

		// Check if we're already waiting (distinguishes first invoke from timeout)
		if (this.opts.execution.waiting_on_execution_id !== null) {
			// we resumed but no step exists → timeout occurred
			// clear waiting state before throwing
			await this.opts.db.clearWaitingState(
				this.opts.execution.id,
				this.opts.signal,
			);

			throw new Error(
				timeout
					? `Child execution timed out after ${timeout}ms`
					: "Child execution timeout",
			);
		}

		await this.opts.db.invoke({
			task_key: taskKey,
			payload,
			parent_execution_id: this.opts.execution.id,
			parent_step_key: id,
			parent_timeout_ms: timeout ?? null,
		});

		return this.abortAndHangup();
	}

	// async waitForEvent(name: string): Promise<void> {}
	//
	// async emitEvent(name: string): Promise<void> {}
}

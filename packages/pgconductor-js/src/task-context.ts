import type {
	DatabaseClient,
	Payload,
	JsonValue,
	Execution,
} from "./database-client";
import type {
	TaskDefinition,
	TaskName,
	FindTaskByIdentifier,
	InferPayload,
	InferReturns,
} from "./task-definition";
import type { TaskIdentifier } from "./task";

export type TaskContextOptions = {
	signal: AbortSignal;
	abortController: AbortController;
	db: DatabaseClient;
	execution: Execution;
};

// second argument for tasks
export class TaskContext<
	Tasks extends readonly TaskDefinition<string, any, any, string>[] = readonly TaskDefinition<
		string,
		any,
		any,
		string
	>[],
> {
	constructor(private readonly opts: TaskContextOptions) {}

	static create<
		Tasks extends readonly TaskDefinition<string, any, any, string>[],
		Extra extends object,
	>(
		opts: TaskContextOptions,
		extra?: Extra,
	): TaskContext<Tasks> & Extra {
		const base = new TaskContext<Tasks>(opts);
		return Object.assign(base, extra) as TaskContext<Tasks> & Extra;
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

		if (cached !== undefined) {
			return cached as T;
		}

		// Execute and save
		const result = await fn();

		await this.opts.db.saveStep(
			this.opts.execution.id,
			this.opts.execution.queue,
			name,
			{ result: result as JsonValue },
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

		if (cached !== undefined) {
			return; // Already slept, continue
		}

		// Save sleep step with ms - time calculation happens in SQL
		await this.opts.db.saveStep(
			this.opts.execution.id,
			this.opts.execution.queue,
			id,
			null,
			ms,
			this.opts.signal,
		);

		return this.abortAndHangup();
	}

	async invoke<
		TName extends TaskName<Tasks>,
		TQueue extends string = "default",
		TDef extends FindTaskByIdentifier<
			Tasks,
			TName,
			TQueue
		> = FindTaskByIdentifier<Tasks, TName, TQueue>,
	>(
		id: string,
		task: TaskIdentifier<TName, TQueue>,
		payload: InferPayload<TDef> = {} as InferPayload<TDef>,
		timeout?: number,
	): Promise<InferReturns<TDef>> {
		const cached = await this.opts.db.loadStep(
			this.opts.execution.id,
			id,
			this.opts.signal,
		);

		if (cached !== undefined) {
			return cached as InferReturns<TDef>;
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

		const taskName = task.name;
		const queue = task.queue || "default";

		await this.opts.db.invokeChild(
			{
				task_key: taskName,
				queue,
				payload,
				parent_execution_id: this.opts.execution.id,
				parent_step_key: id,
				parent_timeout_ms: timeout || null,
			},
			this.opts.signal,
		);

		return this.abortAndHangup();
	}

	// async waitForEvent(name: string): Promise<void> {}
	//
	// async emitEvent(name: string): Promise<void> {}
}

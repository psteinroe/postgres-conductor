import CronExpressionParser from "cron-parser";
import type { DatabaseClient, JsonValue, Execution, Payload } from "./database-client";
import type {
	TaskDefinition,
	TaskName,
	FindTaskByIdentifier,
	InferPayload,
	InferReturns,
} from "./task-definition";
import type { TaskIdentifier } from "./task";
import type { Logger } from "./lib/logger";
import { WindowChecker } from "./lib/window-checker";
import { TypedAbortController } from "./lib/typed-abort-controller";
import type {
	EventDefinition,
	EventName,
	FindEventByIdentifier,
	InferEventPayload,
} from "./event-definition";

export type TaskAbortReasons =
	// if cancelled by a user
	| { reason: "cancelled"; __pgconductorTaskAborted: true }
	// the task is released
	| {
			reason: "released";
			reschedule_in_ms: number | "infinity";
			step_key?: string;
			__pgconductorTaskAborted: true;
	  }
	// the worker wants to shut down
	| { reason: "parent-aborted"; __pgconductorTaskAborted: true }
	// the task invoked a child
	| {
			reason: "child-invocation";
			timeout_ms: number | "infinity";
			step_key: string;
			task: TaskIdentifier<string, string>;
			payload: Payload | null;
			__pgconductorTaskAborted: true;
	  };

type DistributiveOmit<T, K extends PropertyKey> = T extends any ? Omit<T, K> : never;

export function isTaskAbortReason(result: unknown): result is TaskAbortReasons {
	return (
		typeof result === "object" &&
		result !== null &&
		"__pgconductorTaskAborted" in result &&
		(result as TaskAbortReasons).__pgconductorTaskAborted === true
	);
}

export function createTaskSignal(
	parentSignal: AbortSignal,
): TypedAbortController<TaskAbortReasons> {
	const controller = new TypedAbortController<TaskAbortReasons>();

	if (parentSignal.aborted) {
		controller.abort({
			__pgconductorTaskAborted: true,
			reason: "parent-aborted",
		});
	} else {
		parentSignal.addEventListener("abort", () => {
			controller.abort({
				__pgconductorTaskAborted: true,
				reason: "parent-aborted",
			});
		});
	}

	return controller;
}

export type TaskContextOptions = {
	abortController: TypedAbortController<TaskAbortReasons>;
	db: DatabaseClient;
	execution: Execution;
	logger: Logger;
	window?: [string, string];
};

type ScheduleOptions = {
	cron: string;
	priority?: number;
};

// second argument for tasks
export class TaskContext<
	Tasks extends readonly TaskDefinition<string, any, any, string>[] = readonly TaskDefinition<
		string,
		any,
		any,
		string
	>[],
	Events extends readonly EventDefinition<string, any>[] = readonly EventDefinition<string, any>[],
> {
	private readonly windowChecker?: WindowChecker;

	constructor(private readonly opts: TaskContextOptions) {
		if (opts.window) {
			this.windowChecker = new WindowChecker(opts.window);
		}
	}

	static create<
		Tasks extends readonly TaskDefinition<string, any, any, string>[],
		Events extends readonly EventDefinition<string, any>[],
		Extra extends object,
	>(opts: TaskContextOptions, extra?: Extra): TaskContext<Tasks, Events> & Extra {
		const base = new TaskContext<Tasks, Events>(opts);
		return Object.assign(base, extra) as TaskContext<Tasks, Events> & Extra;
	}

	get logger(): Logger {
		return this.opts.logger;
	}

	get signal(): AbortSignal {
		return this.opts.abortController.signal;
	}

	async step<T extends JsonValue | void>(name: string, fn: () => Promise<T> | T): Promise<T> {
		// Check abort signal
		if (this.signal.aborted) {
			return this.abortAndHangup({
				reason: "released",
				reschedule_in_ms: 0,
			});
		}

		// Check window boundaries
		if (this.windowChecker) {
			const now = await this.getNow();
			if (!this.windowChecker.isWithinWindow(now)) {
				const nextRunAt = this.windowChecker.getNextValidRunAt(now);
				const delay = Math.max(nextRunAt.getTime() - now.getTime(), 0);
				return this.abortAndHangup({
					reason: "released",
					reschedule_in_ms: delay,
				});
			}
		}

		// Check if step already completed
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				key: name,
			},
			{ signal: this.signal },
		);

		if (cached !== undefined) {
			return (cached as { result: T }).result;
		}

		// Execute and save
		const result = await fn();

		await this.opts.db.saveStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				key: name,
				result: { result: result as JsonValue },
				runAtMs: undefined,
			},
			{ signal: this.signal },
		);

		return result;
	}

	async checkpoint(): Promise<void> {
		// Check abort signal
		if (this.signal.aborted) {
			return this.abortAndHangup({
				reason: "released",
				reschedule_in_ms: 0,
			});
		}

		// Check window boundaries
		if (this.windowChecker) {
			const now = await this.getNow();
			if (!this.windowChecker.isWithinWindow(now)) {
				const nextRunAt = this.windowChecker.getNextValidRunAt(now);
				const delay = Math.max(nextRunAt.getTime() - now.getTime(), 0);
				return this.abortAndHangup({
					reason: "released",
					reschedule_in_ms: delay,
				});
			}
		}
	}

	async sleep(id: string, ms: number): Promise<void> {
		// Check if we already slept
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				key: id,
			},
			{ signal: this.signal },
		);

		if (cached !== undefined) {
			return; // Already slept, continue
		}

		return this.abortAndHangup({
			reason: "released",
			reschedule_in_ms: ms,
			step_key: id,
		});
	}

	async invoke<
		TName extends TaskName<Tasks>,
		TQueue extends string = "default",
		TDef extends FindTaskByIdentifier<Tasks, TName, TQueue> = FindTaskByIdentifier<
			Tasks,
			TName,
			TQueue
		>,
	>(
		key: string,
		task: TaskIdentifier<TName, TQueue>,
		payload: InferPayload<TDef> = {} as InferPayload<TDef>,
		timeout?: number,
	): Promise<InferReturns<TDef>> {
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				key,
			},
			{ signal: this.signal },
		);

		if (cached !== undefined) {
			return cached as InferReturns<TDef>;
		}

		// Check if we're already waiting (distinguishes first invoke from timeout)
		if (this.opts.execution.waiting_on_execution_id !== null) {
			// we resumed but no step exists → timeout occurred
			// clear waiting state before throwing

			await this.opts.db.clearWaitingState(
				{
					executionId: this.opts.execution.id,
				},
				{ signal: this.signal },
			);

			throw new Error(
				timeout ? `Child execution timed out after ${timeout}ms` : "Child execution timeout",
			);
		}

		return this.abortAndHangup({
			reason: "child-invocation",
			timeout_ms: timeout || "infinity",
			task,
			step_key: key,
			payload,
		});
	}

	async schedule<
		TName extends TaskName<Tasks>,
		TQueue extends string = "default",
		TDef extends FindTaskByIdentifier<Tasks, TName, TQueue> = FindTaskByIdentifier<
			Tasks,
			TName,
			TQueue
		>,
	>(
		task: TaskIdentifier<TName, TQueue>,
		scheduleName: string,
		options: ScheduleOptions,
		payload: InferPayload<TDef> = {} as InferPayload<TDef>,
	): Promise<void> {
		if (!scheduleName) {
			throw new Error("scheduleName is required");
		}

		if (scheduleName.includes("::")) {
			throw new Error("scheduleName cannot contain '::'");
		}

		if (!options?.cron) {
			throw new Error("cron expression is required");
		}

		const interval = CronExpressionParser.parse(options.cron);
		const nextTimestamp = interval.next().toDate();
		const queue = task.queue || "default";

		await this.opts.db.scheduleCronExecution(
			{
				spec: {
					task_key: task.name,
					queue,
					payload,
					run_at: nextTimestamp,
					cron_expression: options.cron,
					priority: options.priority || null,
				},
				scheduleName,
			},
			{ signal: this.signal },
		);
	}

	async unschedule<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		task: TaskIdentifier<TName, TQueue>,
		scheduleName: string,
	): Promise<void> {
		if (!scheduleName) {
			throw new Error("scheduleName is required");
		}

		if (scheduleName.includes("::")) {
			throw new Error("scheduleName cannot contain '::'");
		}

		const queue = task.queue || "default";
		await this.opts.db.unscheduleCronExecution(
			{
				taskKey: task.name,
				queue,
				scheduleName,
			},
			{ signal: this.signal },
		);
	}

	/**
	 * Cancel an execution by ID.
	 * @param executionId - The execution ID to cancel
	 * @param options - Optional cancellation options
	 * @param options.reason - Cancellation reason (defaults to "Cancelled by user")
	 * @returns true if the execution was cancelled, false if it was already completed/failed
	 */
	async cancel(executionId: string, options?: { reason?: string }): Promise<boolean> {
		return this.opts.db.cancelExecution(executionId, {
			...options,
			signal: this.signal,
		});
	}

	/**
	 * Emit a typed custom event.
	 * @param event - Event name to emit
	 * @param payload - Typed event payload
	 * @returns Event ID
	 */
	async emit<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(event: TName, payload: InferEventPayload<TDef>): Promise<string> {
		return this.opts.db.emitEvent(
			{
				eventKey: event,
				payload: payload as any,
			},
			{ signal: this.signal },
		);
	}

	/**
	 * Get current time. In tests, use database time (respects fake_now).
	 * In production, use system time for performance.
	 */
	private async getNow(): Promise<Date> {
		if (process.env.NODE_ENV === "test") {
			return this.opts.db.getCurrentTime({ signal: this.signal });
		}
		return new Date();
	}

	/**
	 * Abort the current execution and hang up indefinitely
	 * @return Promise that never resolves
	 **/
	private abortAndHangup(
		reason: DistributiveOmit<TaskAbortReasons, "__pgconductorTaskAborted">,
	): Promise<never> {
		this.opts.abortController.abort({
			__pgconductorTaskAborted: true,
			...reason,
		} as TaskAbortReasons);
		return new Promise(() => {});
	}
}

/**
 * Context for batch task handlers.
 * Batch tasks cannot use step() or invoke() since batch composition
 * is non-deterministic across retries.
 */
export class BatchTaskContext {
	constructor(
		private readonly abortController: TypedAbortController<TaskAbortReasons>,
		public readonly logger: Logger,
	) {}

	get signal(): AbortSignal {
		return this.abortController.signal;
	}

	/**
	 * Sleep reschedules ALL executions in the batch.
	 * After sleep, executions may batch with different peers.
	 */
	async sleep(id: string, ms: number): Promise<void> {
		this.abortController.abort({
			__pgconductorTaskAborted: true,
			reason: "released",
			reschedule_in_ms: ms,
			step_key: id,
		} as TaskAbortReasons);
		return new Promise(() => {});
	}
}

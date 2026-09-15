import type {
	DatabaseClient,
	Execution,
	Payload,
	DeadLetterMetadata,
	JsonValue,
} from "./database-client";
import { nextCronOccurrence } from "./lib/cron";
import type { Clock } from "./lib/clock";
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
import { parseDuration, type DurationInput } from "./lib/duration";
import type {
	EventDefinition,
	EventName,
	FindEventByIdentifier,
	InferEventPayload,
	FilterForEvent,
} from "./event-definition";
import { compileEventFilter } from "./event-trigger-validation";

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
			group?: string | null;
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

export class WaitForEventTimeoutError extends Error {
	readonly code = "PGCONDUCTOR_WAIT_FOR_EVENT_TIMEOUT";
	constructor(public readonly stepKey: string) {
		super(`Timed out waiting for event at step "${stepKey}"`);
		this.name = "WaitForEventTimeoutError";
	}
}

type ResolvedEventWaitStepResult<TDef extends EventDefinition<string, any, any>> = {
	status: "resolved";
	event: { name: TDef["name"]; payload: InferEventPayload<TDef> };
};

type TimedOutEventWaitStepResult = { status: "timed_out" };

function isJsonValue(value: unknown): value is JsonValue {
	if (value === null || ["string", "number", "boolean"].includes(typeof value)) return true;
	if (Array.isArray(value)) return value.every(isJsonValue);
	return isPayload(value);
}

function isPayload(value: unknown): value is Payload {
	return (
		typeof value === "object" &&
		value !== null &&
		!Array.isArray(value) &&
		Object.values(value).every(isJsonValue)
	);
}

function isTimedOutEventWaitStepResult(value: unknown): value is TimedOutEventWaitStepResult {
	return isPayload(value) && value.status === "timed_out";
}

function isResolvedEventWaitStepResult<TDef extends EventDefinition<string, any, any>>(
	value: unknown,
	event: TDef,
): value is ResolvedEventWaitStepResult<TDef> {
	return (
		isPayload(value) &&
		value.status === "resolved" &&
		isPayload(value.event) &&
		value.event.name === event.name &&
		isPayload(value.event.payload)
	);
}

export type TaskContextOptions = {
	abortController: TypedAbortController<TaskAbortReasons>;
	db: DatabaseClient;
	clock: Clock;
	execution: Execution;
	logger: Logger;
	eventDefinitions: readonly EventDefinition<string, any, any>[];
	window?: [string, string];
};

type ScheduleOptions = {
	cron: string;
	priority?: number;
	group?: string;
};

// second argument for tasks
export class TaskContext<
	Tasks extends readonly TaskDefinition<string, any, any, string>[] = readonly TaskDefinition<
		string,
		any,
		any,
		string
	>[],
	Events extends readonly EventDefinition<string, any, any>[] = readonly EventDefinition<
		string,
		any,
		any
	>[],
> {
	private readonly windowChecker?: WindowChecker;

	constructor(private readonly opts: TaskContextOptions) {
		if (opts.window) {
			this.windowChecker = new WindowChecker(opts.window);
		}
	}

	static create<
		Tasks extends readonly TaskDefinition<string, any, any, string>[],
		Events extends readonly EventDefinition<string, any, any>[],
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

	/** Metadata describing the source execution when this is a DLQ delivery. */
	get deadLetter(): DeadLetterMetadata | null {
		const execution = this.opts.execution;
		if (
			!execution.dead_letter_source_execution_id ||
			!execution.dead_letter_source_queue ||
			!execution.dead_letter_source_task_key ||
			execution.dead_letter_attempts == null ||
			!execution.dead_letter_failed_at
		) {
			return null;
		}
		return {
			sourceExecutionId: execution.dead_letter_source_execution_id,
			sourceQueue: execution.dead_letter_source_queue,
			sourceTaskKey: execution.dead_letter_source_task_key,
			error: execution.dead_letter_error ?? null,
			attempts: execution.dead_letter_attempts,
			failedAt: execution.dead_letter_failed_at,
		};
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
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
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
				orchestratorId: this.opts.execution.locked_by,
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
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
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

	async waitForEvent<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		stepKey: string,
		options: {
			event: TDef;
			filter?: FilterForEvent<TDef>;
			timeout?: DurationInput;
		},
	): Promise<{ name: TDef["name"]; payload: InferEventPayload<TDef> }> {
		if (!stepKey) throw new Error("waitForEvent stepKey is required");
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
				key: stepKey,
			},
			{ signal: this.signal },
		);
		if (cached !== undefined) {
			if (isTimedOutEventWaitStepResult(cached)) throw new WaitForEventTimeoutError(stepKey);
			if (isResolvedEventWaitStepResult(cached, options.event)) return cached.event;
			throw new Error(`Invalid waitForEvent step result at "${stepKey}"`);
		}

		const compiled = compileEventFilter(
			options.event.name,
			options.filter,
			this.opts.eventDefinitions,
		);
		const timeoutMs = options.timeout === undefined ? null : parseDuration(options.timeout);
		const registration = await this.opts.db.registerEventWait(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				taskKey: this.opts.execution.task_key,
				eventKey: options.event.name,
				stepKey,
				requiredFieldCount: compiled.required_field_count,
				terms: compiled.terms,
				timeoutMs,
				orchestratorId: this.opts.execution.locked_by,
			},
			{ signal: this.signal },
		);
		// A timeout wakes and reruns the execution. Registration then replaces the expired
		// subscription with a timed-out step before reporting that outcome here.
		if (registration.timedOut) throw new WaitForEventTimeoutError(stepKey);
		return this.abortAndHangup({
			reason: "released",
			reschedule_in_ms: registration.timeoutMs ?? "infinity",
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
		group?: string,
	): Promise<InferReturns<TDef>> {
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
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
					queue: this.opts.execution.queue,
					orchestratorId: this.opts.execution.locked_by,
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
			group,
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

		const nextTimestamp = nextCronOccurrence(options.cron, this.opts.clock.now());
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
					group: options.group || null,
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
	 * Get current time.
	 * DatabaseClient handles test vs production: returns fake time in tests, system time in production.
	 */
	private async getNow(): Promise<Date> {
		return this.opts.db.getCurrentTime({ signal: this.signal });
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
export type BatchTaskEvent<Event extends object> = Event & {
	readonly execution: Readonly<Pick<Execution, "id" | "queue" | "task_key" | "locked_by">>;
};

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

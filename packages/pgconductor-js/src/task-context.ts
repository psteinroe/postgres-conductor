import type { StandardSchemaV1 } from "@standard-schema/spec";
import type {
	DatabaseClient,
	Execution,
	Payload,
	DeadLetterMetadata,
	EventWaitResult,
	JsonValue,
} from "./database-client.js";
import { nextCronOccurrence } from "./lib/cron.js";
import type { Clock } from "./lib/clock.js";
import type {
	TaskDefinition,
	TaskName,
	FindTaskByIdentifier,
	InferPayload,
	InferReturns,
} from "./task-definition.js";
import type { TaskIdentifier } from "./task.js";
import type { Logger } from "./lib/logger.js";
import { WindowChecker } from "./lib/window-checker.js";
import type { TypedAbortController } from "./lib/typed-abort-controller.js";
import { parseDuration, type DurationInput } from "./lib/duration.js";
import { SpanKind } from "@opentelemetry/api";
import type { Telemetry, TraceContextCarrier } from "./telemetry.js";
import {
	validateEventPayload,
	type EventDefinition,
	type EventName,
	type FindEventByIdentifier,
	type InferEventPayload,
	type FilterForEvent,
} from "./event-definition.js";
import { compileEventFilter } from "./event-trigger-validation.js";
import { validateSchema } from "./lib/validate-schema.js";
import type { SignalHandler, SignalInbox } from "./signal-inbox.js";

export type TaskAbortReasons =
	// if cancelled by a user
	| { reason: "cancelled"; __pgconductorTaskAborted: true }
	// the database already released the task
	| { reason: "suspended"; __pgconductorTaskAborted: true }
	// the task is released
	| {
			reason: "released";
			reschedule_in_ms: number | "infinity";
			step_key?: string;
			__pgconductorTaskAborted: true;
	  }
	// the task ran longer than its timeoutMs
	| { reason: "timed-out"; __pgconductorTaskAborted: true }
	// a signal handler threw
	| { reason: "signal-failed"; error: string; __pgconductorTaskAborted: true }
	// the task invoked a child
	| {
			reason: "child-invocation";
			timeout_ms: number | "infinity";
			step_key: string;
			task: TaskIdentifier<string, string>;
			payload: Payload | null;
			group?: string | null;
			trace_context: TraceContextCarrier | null;
			metadata: Payload | null;
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

export class WaitForEventTimeoutError extends Error {
	readonly code = "PGCONDUCTOR_WAIT_FOR_EVENT_TIMEOUT";
	constructor(public readonly stepKey: string) {
		super(`Timed out waiting for event at step "${stepKey}"`);
		this.name = "WaitForEventTimeoutError";
	}
}

/** A durable event subscription created by `ctx.subscribe()`. */
export type EventSubscription<TEvent> = {
	/** The step key the subscription is memoized under. */
	readonly key: string;
	/**
	 * Resolve with the matching event kept since the subscription, suspending
	 * until one arrives. The timeout starts when `wait()` is first called.
	 */
	wait(options?: { timeout?: DurationInput }): Promise<TEvent>;
};

/** The branch of a `ctx.waitForAny()` that won, with its event, or the timeout. */
export type WaitForAnyResult<TBranches> =
	| {
			[K in keyof TBranches & string]: TBranches[K] extends EventSubscription<infer TEvent>
				? { key: K; event: TEvent }
				: never;
	  }[keyof TBranches & string]
	| { key: "timeout" };

export type TaskContextOptions = {
	abortController: TypedAbortController<TaskAbortReasons>;
	signal: AbortSignal;
	db: DatabaseClient;
	clock: Clock;
	execution: Execution;
	logger: Logger;
	eventDefinitions: readonly EventDefinition<string, any, any>[];
	metadataSchema?: StandardSchemaV1<unknown, object>;
	window?: [string, string];
	telemetry: Telemetry;
	signals: SignalInbox;
};

/** A signal sent to an execution with `conductor.signal()` or `signalWithStart()`. */
export type SignalMessage = {
	name: string;
	/** Identifies the signal: a signal id is stored once per execution */
	id: string;
	payload?: JsonValue;
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
	Metadata extends object = Payload,
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
		return this.opts.signal;
	}

	/** Metadata of this execution, inherited from the event, parent or schedule that created it. */
	get metadata(): Readonly<Metadata> | undefined {
		return (this.opts.execution.metadata || undefined) as Readonly<Metadata> | undefined;
	}

	/** Metadata describing the source execution when this is a DLQ delivery. */
	get deadLetter(): DeadLetterMetadata | null {
		const deadLetter = this.opts.execution.dead_letter;
		return deadLetter ? { ...deadLetter, failedAt: new Date(deadLetter.failedAt) } : null;
	}

	async step<T extends JsonValue | void>(name: string, fn: () => Promise<T> | T): Promise<T> {
		await this.checkpoint();

		// Check if step already completed
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				key: name,
			},
			{ signal: this.signal },
		);

		if (cached !== undefined) {
			return (cached as { result: T }).result;
		}

		// Execute and save. A cache hit intentionally has no span.
		return this.opts.telemetry.trace({
			name: `step ${this.opts.execution.task_key}`,
			kind: SpanKind.INTERNAL,
			attributes: {
				"pgconductor.task.name": this.opts.execution.task_key,
				"pgconductor.step.name": name,
				"pgconductor.queue": this.opts.execution.queue,
			},
			run: async () => {
				const result = await fn();
				await this.opts.db.saveStep(
					{
						executionId: this.opts.execution.id,
						queue: this.opts.execution.queue,
						orchestratorId: this.opts.execution.locked_by,
						claimToken: this.opts.execution.claim_token,
						key: name,
						result: { result: result as JsonValue },
					},
					{ signal: this.signal },
				);
				return result;
			},
		});
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
			const now = this.opts.clock.now();
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
		const cached = (await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				key: stepKey,
			},
			{ signal: this.signal },
		)) as EventWaitResult | undefined;
		const result = cached || (await this.registerEventWait(stepKey, options, true));
		if (!result) return this.abortAndHangup({ reason: "suspended" });
		if (result.status === "timed_out") throw new WaitForEventTimeoutError(stepKey);
		if (result.status === "closed") {
			throw new Error(`Event subscription "${stepKey}" was closed by ctx.waitForAny()`);
		}
		return result.event as { name: TDef["name"]; payload: InferEventPayload<TDef> };
	}

	/**
	 * Subscribe to a custom event without suspending. A matching event emitted
	 * after this resolves is kept for `wait()`, including events caused by steps
	 * that run in between. The subscription is memoized by the step key.
	 */
	async subscribe<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		stepKey: string,
		options: {
			event: TDef;
			filter?: FilterForEvent<TDef>;
		},
	): Promise<EventSubscription<{ name: TDef["name"]; payload: InferEventPayload<TDef> }>> {
		await this.registerEventWait(stepKey, options, false);
		return {
			key: stepKey,
			wait: ({ timeout } = {}) => this.waitForEvent<TName, TDef>(stepKey, { ...options, timeout }),
		};
	}

	/**
	 * Wait for the first event among several `ctx.subscribe()` subscriptions, or
	 * the timeout. An event a subscription kept before this call wins at once;
	 * when several did, the one dispatched first wins. All subscriptions are
	 * removed when the wait settles, and the winner is memoized by the step key.
	 */
	async waitForAny<TBranches extends Record<string, EventSubscription<unknown>>>(
		stepKey: string,
		branches: TBranches & { timeout?: never },
		options: { timeout?: DurationInput } = {},
	): Promise<WaitForAnyResult<TBranches>> {
		const result = await this.opts.db.registerEventRace(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				taskKey: this.opts.execution.task_key,
				stepKey,
				branches: Object.entries(branches).map(([key, subscription]) => ({
					key,
					stepKey: subscription.key,
				})),
				timeoutMs: options.timeout === undefined ? null : parseDuration(options.timeout),
				orchestratorId: this.opts.execution.locked_by,
				claimToken: this.opts.execution.claim_token,
			},
			{ signal: this.signal },
		);
		if (!result) return this.abortAndHangup({ reason: "suspended" });
		return result as WaitForAnyResult<TBranches>;
	}

	async invokeAndWait<
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
		{
			timeout,
			group,
			metadata,
		}: {
			timeout?: number;
			group?: string;
			/** Metadata for this child only. Defaults to the metadata of this execution. */
			metadata?: Metadata | ((metadata: Readonly<Metadata> | undefined) => Metadata);
		} = {},
	): Promise<InferReturns<TDef>> {
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
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
					claimToken: this.opts.execution.claim_token,
				},
				{ signal: this.signal },
			);

			throw new Error(
				timeout ? `Child execution timed out after ${timeout}ms` : "Child execution timeout",
			);
		}

		const childMetadata = await this.childMetadata(metadata);
		return this.abortAndHangup({
			reason: "child-invocation",
			timeout_ms: timeout || "infinity",
			task,
			step_key: key,
			payload,
			group,
			trace_context: this.opts.telemetry.traceContext(),
			metadata: childMetadata,
		});
	}

	/**
	 * Start an execution and return its id without waiting for it.
	 * Memoized by the step key: retries and resumes return the same execution.
	 * Its outcome does not affect this execution. It is not cancelled with this one unless
	 * `cancelWithParent` is set.
	 */
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
		{
			dedupe_key,
			metadata,
			run_at,
			priority,
			group,
			cancelWithParent = false,
		}: {
			/**
			 * Returns the execution that holds this key instead of starting a new one.
			 * Defaults to a key derived from this execution and the step key.
			 */
			dedupe_key?: string;
			/** Metadata for the started execution. Defaults to the metadata of this execution. */
			metadata?: Metadata | ((metadata: Readonly<Metadata> | undefined) => Metadata);
			run_at?: Date;
			priority?: number;
			group?: string;
			/**
			 * Cancel the started execution with this one's reason when this execution is cancelled
			 * or fails permanently. Applies only when this call starts the execution.
			 */
			cancelWithParent?: boolean;
		} = {},
	): Promise<string> {
		const cached = await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				key,
			},
			{ signal: this.signal },
		);

		if (cached !== undefined) {
			return (cached as { id: string }).id;
		}

		const id = await this.opts.db.startExecution(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
				claimToken: this.opts.execution.claim_token,
				key,
				spec: {
					task_key: task.name,
					queue: task.queue || "default",
					payload,
					run_at,
					dedupe_key: dedupe_key || `${this.opts.execution.id}:${key}`,
					priority,
					group,
					trace_context: this.opts.telemetry.traceContext(),
					metadata: await this.childMetadata(metadata),
				},
				cancelWithParent,
			},
			{ signal: this.signal },
		);
		return id || this.abortAndHangup({ reason: "suspended" });
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
					metadata: this.opts.execution.metadata,
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
	 * Listen for signals by name. Signals stored before the handler started are delivered
	 * first, in order, and later ones while the handler runs. Handlers run one at a time.
	 * Register listeners before awaiting anything: a signal reached while its name has no listener
	 * is skipped, and re-queues the execution if a listener for it registers later.
	 *
	 * Delivery is at least once per signal id, so dedupe by `id`. A signal counts as delivered
	 * once its handler returns; a handler that throws fails the attempt like an uncaught error.
	 * When the handler returns, signals already read are delivered before the attempt settles.
	 * Ones stored later for a name this attempt listens for re-queue the execution, so a new
	 * attempt receives them when it starts.
	 */
	onSignal<T extends JsonValue = JsonValue>(name: string, handler: SignalHandler<T>): void {
		this.opts.signals.listen(name, handler as SignalHandler);
	}

	/**
	 * Signal an execution. Idempotent by signal id: resending an id stores the signal once.
	 * @returns false if the execution is completed, failed, cancelled or unknown
	 */
	async sendSignal(executionId: string, signal: SignalMessage): Promise<boolean> {
		return this.opts.db.signalExecution({ executionId, signal }, { signal: this.signal });
	}

	/**
	 * Signal the non-terminal execution of a task for `key`, or start one that receives the
	 * signal first. Memoized by the step key.
	 * @returns ID of the execution that received the signal
	 */
	async signalWithStart<
		TName extends TaskName<Tasks>,
		TQueue extends string = "default",
		TDef extends FindTaskByIdentifier<Tasks, TName, TQueue> = FindTaskByIdentifier<
			Tasks,
			TName,
			TQueue
		>,
	>(
		stepKey: string,
		options: {
			task: TName;
			queue?: TQueue;
			key: string;
			signal: SignalMessage;
			/** Payload of the execution, used only when it starts one */
			input?: InferPayload<TDef>;
			priority?: number;
		},
	): Promise<string> {
		return this.step(stepKey, () =>
			this.opts.db.signalWithStart(
				{
					taskKey: options.task,
					queue: options.queue || "default",
					key: options.key,
					signal: options.signal,
					input: (options.input || null) as Payload | null,
					priority: options.priority,
					traceContext: this.opts.telemetry.traceContext(),
					metadata: this.opts.execution.metadata,
				},
				{ signal: this.signal },
			),
		);
	}

	/**
	 * Emit a typed custom event.
	 * @param event - Event name to emit
	 * @param payload - Typed event payload
	 * @param opts.dedupe_key - Repeating a key for the same event name emits nothing and returns the original event ID
	 * @returns Event ID
	 */
	async emit<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		event: TName,
		payload: InferEventPayload<TDef>,
		opts?: { dedupe_key?: string },
	): Promise<string> {
		payload = await validateEventPayload(this.opts.eventDefinitions, event, payload);
		return this.opts.db.emitEvent(
			{
				eventKey: event,
				payload: payload as any,
				traceContext: this.opts.telemetry.traceContext(),
				metadata: this.opts.execution.metadata,
				dedupeKey: opts?.dedupe_key,
			},
			{ signal: this.signal },
		);
	}

	private async childMetadata(
		metadata: Metadata | ((metadata: Readonly<Metadata> | undefined) => Metadata) | undefined,
	): Promise<Payload | null> {
		if (!metadata) return this.opts.execution.metadata || null;
		const childMetadata = (await validateSchema(
			this.opts.metadataSchema,
			typeof metadata === "function" ? metadata(this.metadata) : metadata,
			"metadata",
		)) as Payload;
		await this.opts.db.checkMetadata(childMetadata, { signal: this.signal });
		return childMetadata;
	}

	private registerEventWait(
		stepKey: string,
		options: {
			event: EventDefinition<string, any, any>;
			filter?: unknown;
			timeout?: DurationInput;
		},
		suspend: boolean,
	): Promise<EventWaitResult | null> {
		const compiled = compileEventFilter(
			options.event.name,
			options.filter,
			this.opts.eventDefinitions,
		);
		return this.opts.db.registerEventWait(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				taskKey: this.opts.execution.task_key,
				eventKey: options.event.name,
				stepKey,
				requiredFieldCount: compiled.required_field_count,
				terms: compiled.terms,
				timeoutMs: options.timeout === undefined ? null : parseDuration(options.timeout),
				orchestratorId: this.opts.execution.locked_by,
				claimToken: this.opts.execution.claim_token,
				suspend,
			},
			{ signal: this.signal },
		);
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
		public readonly signal: AbortSignal,
		public readonly logger: Logger,
		private readonly db: Pick<DatabaseClient, "countSteps">,
		private readonly executions: Execution[],
	) {}

	/**
	 * Sleep reschedules ALL executions in the batch.
	 * After sleep, executions may batch with different peers, so it only
	 * returns once every execution in the current batch has slept at `id`.
	 */
	async sleep(id: string, ms: number): Promise<void> {
		const slept = await this.db.countSteps(
			{
				executionIds: this.executions.map((exec) => exec.id),
				key: id,
			},
			{ signal: this.signal },
		);

		if (slept === this.executions.length) {
			return;
		}

		this.abortController.abort({
			__pgconductorTaskAborted: true,
			reason: "released",
			reschedule_in_ms: ms,
			step_key: id,
		} as TaskAbortReasons);
		return new Promise(() => {});
	}
}

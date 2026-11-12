import type { StandardSchemaV1 } from "@standard-schema/spec";
import type {
	DatabaseClient,
	EventInfo,
	Execution,
	ExecutionInfo,
	ListenerInfo,
	Payload,
	DeadLetterMetadata,
	EventWaitResult,
	ExecutionOutcome,
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
	InferState,
} from "./task-definition.js";
import { validateState } from "./task-definition.js";
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
import { compileEventFilter, compileListens, type Listen } from "./event-trigger-validation.js";
import { validateSchema } from "./lib/validate-schema.js";
import type { ReceivedSignal, SignalHandler, SignalInbox } from "./signal-inbox.js";
import {
	eventFilter,
	readEvents,
	readExecution,
	readExecutions,
	readListeners,
	type EventQuery,
	type ExecutionLookup,
	type ExecutionQuery,
	type TaskExecutionInfo,
	type TaskListenerInfo,
} from "./reads.js";

/** Why an attempt was aborted, read from its abort controller and never from handler output */
export type TaskAbortReasons =
	// if cancelled by a user
	| { reason: "cancelled" }
	// the database already released the task, or the attempt no longer holds its claim
	| { reason: "suspended" }
	// the task is released
	| {
			reason: "released";
			reschedule_in_ms: number | "infinity";
			step_key?: string;
	  }
	// the task ran longer than its timeoutMs
	| { reason: "timed-out" }
	// a signal handler threw
	| { reason: "signal-failed"; error: string }
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
	  };

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

/**
 * A `ctx.waitForAny()` branch that waits for an execution to settle. Pass its task to type
 * the result; an execution of another task is not found.
 */
export type ExecutionBranch = { execution: string; task?: TaskIdentifier<string, string> };

declare const signalPayload: unique symbol;

/**
 * A `ctx.waitForAny()` branch that waits for a signal with this name. It does not consume the
 * signal: `ctx.onSignal()` handlers still receive it. The winner carries the signal's payload;
 * annotate the branch as `SignalBranch<T>` to type it, which is not checked, like the type of a
 * `ctx.onSignal<T>()` handler.
 */
export type SignalBranch<TPayload extends JsonValue = JsonValue> = {
	signal: string;
	/** Type only: the payload type of the signal that wins this branch */
	readonly [signalPayload]?: TPayload;
};

type SignalBranchPayload<TBranch> = TBranch extends {
	readonly [signalPayload]?: infer TPayload extends JsonValue;
}
	? unknown extends TPayload
		? JsonValue
		: TPayload
	: JsonValue;

type ExecutionBranchResult<
	TBranch,
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
> = TBranch extends { task: { name: infer TName extends string } }
	? InferReturns<
			FindTaskByIdentifier<
				Tasks,
				TName,
				TBranch extends { task: { queue: infer TQueue extends string } } ? TQueue : "default"
			>
		>
	: unknown;

/** The branch of a `ctx.waitForAny()` that won, with its event or outcome, or the timeout. */
export type WaitForAnyResult<
	TBranches,
	Tasks extends readonly TaskDefinition<string, any, any, string>[] = readonly TaskDefinition<
		string,
		any,
		any,
		string
	>[],
> =
	| {
			[K in keyof TBranches & string]: TBranches[K] extends EventSubscription<infer TEvent>
				? { key: K; event: TEvent }
				: TBranches[K] extends ExecutionBranch
					? { key: K; outcome: ExecutionOutcome<ExecutionBranchResult<TBranches[K], Tasks>> }
					: TBranches[K] extends SignalBranch
						? { key: K; signal: ReceivedSignal<SignalBranchPayload<TBranches[K]>> }
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
	/** Schema of this execution's state, validated by `ctx.setState()` and `ctx.updateState()` */
	stateSchema?: StandardSchemaV1;
	/** The conductor's task catalog, for the state schemas of the tasks `ctx.signalWithStart()` targets */
	taskDefinitions?: readonly TaskDefinition<string, unknown, unknown, string>[];
	window?: [string, string];
	telemetry: Telemetry;
	signals: SignalInbox;
	/** Step callbacks and their saves in flight */
	steps: Set<Promise<unknown>>;
	/** The activation of an actor, with the alarm it set */
	actor?: { alarm: Date | null };
};

/** A signal sent to an execution with `conductor.signal()` or `signalWithStart()`. */
export type SignalMessage = {
	name: string;
	/** Identifies the signal: a signal id is stored once per execution */
	id: string;
	payload?: JsonValue;
};

/** The open execution of a task holding `key`: the key `signalWithStart()` addresses it by. */
export type KeyedExecution<TName extends string = string, TQueue extends string = string> = {
	task: TaskIdentifier<TName, TQueue>;
	key: string;
};

/** @internal Addresses a signal by execution id or by key. */
export function signalTarget(
	target: string | KeyedExecution<string, string>,
): { executionId: string } | { task: { key: string; queue: string }; key: string } {
	if (typeof target === "string") return { executionId: target };
	return {
		task: { key: target.task.name, queue: target.task.queue || "default" },
		key: target.key,
	};
}

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
	State extends object = Payload,
> {
	private readonly windowChecker?: WindowChecker;
	/** The last state write of this attempt: writes run one after another */
	private stateWrites: Promise<unknown> = Promise.resolve();
	private listensWrite: Promise<unknown> = Promise.resolve();

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

	/** ID of this execution. */
	get executionId(): string {
		return this.opts.execution.id;
	}

	/** Queue of this execution. */
	get queue(): string {
		return this.opts.execution.queue;
	}

	get signal(): AbortSignal {
		return this.opts.signal;
	}

	/** Metadata of this execution, inherited from the event, parent or schedule that created it. */
	get metadata(): Readonly<Metadata> | undefined {
		return (this.opts.execution.metadata || undefined) as Readonly<Metadata> | undefined;
	}

	/**
	 * What this execution tells others about itself: the last `ctx.setState()` or
	 * `ctx.updateState()`, or its initial state. Read as stored, not validated.
	 */
	get state(): Readonly<State> | null {
		return (this.opts.execution.state || null) as Readonly<State> | null;
	}

	/**
	 * Names of this execution's listens in the order they were created: the ones it had when the
	 * attempt started, then as this attempt's `ctx.listen()` and `ctx.unlisten()` left them.
	 */
	get listens(): readonly string[] {
		return this.opts.execution.listens;
	}

	/** Metadata describing the source execution when this is a DLQ delivery. */
	get deadLetter(): DeadLetterMetadata | null {
		const deadLetter = this.opts.execution.dead_letter;
		return deadLetter ? { ...deadLetter, failedAt: new Date(deadLetter.failedAt) } : null;
	}

	/**
	 * Run `fn` and save its result under `name`. Retries and resumes return the saved result.
	 * `fn` runs again if the attempt ends before the result is saved, so external side effects
	 * need an idempotency key.
	 */
	async step<T extends JsonValue | void>(name: string, fn: () => Promise<T> | T): Promise<T> {
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

		// After the read, so fn never starts once the attempt aborted
		await this.checkpoint();

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
				const running = (async () => {
					const result = await fn();
					const saved = await this.opts.db.saveStep(
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
					return { result, saved };
				})();
				this.opts.steps.add(running);
				const { result, saved } = await running.finally(() => this.opts.steps.delete(running));
				return saved ? result : this.abortAndHangup({ reason: "suspended" });
			},
		});
	}

	/**
	 * Hang up if `ctx.signal` is aborted or the task is outside its time window. Saves nothing:
	 * a released execution runs its handler again from the start.
	 */
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

	/** Suspend for `duration`; numbers are milliseconds. Memoized by `id`. */
	async sleep(id: string, duration: DurationInput): Promise<void> {
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
			reschedule_in_ms: parseDuration(duration),
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
	 * Wait for the first of several branches, or the timeout: a `ctx.subscribe()`
	 * subscription receives an event, an `{ execution: id }` branch settles, or this
	 * execution receives a signal named by a `{ signal: name }` branch. A branch that
	 * already holds its event or outcome, or a signal whose handler has not returned,
	 * wins at once; when several do, the one stored first wins. Execution and signal
	 * branches are stored under `<stepKey>:<branch>`. All subscriptions are removed when the wait settles, and
	 * the winner is memoized by the step key.
	 */
	async waitForAny<
		const TBranches extends Record<
			string,
			EventSubscription<unknown> | ExecutionBranch | SignalBranch
		>,
	>(
		stepKey: string,
		branches: TBranches & { timeout?: never },
		options: { timeout?: DurationInput } = {},
	): Promise<WaitForAnyResult<TBranches, Tasks>> {
		// From the call on, no signal handler starts, and the one running is passed along: a signal
		// handled before the wait registers would neither win nor wake it
		const result = await this.opts.signals.hold(async (delivering) => {
			const cached = await this.opts.db.loadStep(
				{
					executionId: this.opts.execution.id,
					queue: this.opts.execution.queue,
					key: stepKey,
				},
				{ signal: this.signal },
			);
			if (cached !== undefined) return cached;
			return this.opts.db.registerEventRace(
				{
					executionId: this.opts.execution.id,
					queue: this.opts.execution.queue,
					taskKey: this.opts.execution.task_key,
					stepKey,
					branches: Object.entries(branches).map(([key, branch]) =>
						"execution" in branch
							? {
									key,
									stepKey: `${stepKey}:${key}`,
									executionId: branch.execution,
									taskKey: branch.task?.name,
									queue: branch.task?.queue || "default",
								}
							: "signal" in branch
								? { key, stepKey: `${stepKey}:${key}`, signal: branch.signal }
								: { key, stepKey: branch.key },
					),
					timeoutMs: options.timeout === undefined ? null : parseDuration(options.timeout),
					orchestratorId: this.opts.execution.locked_by,
					claimToken: this.opts.execution.claim_token,
					deliveringSignalId: delivering?.id || null,
				},
				{ signal: this.signal },
			);
		});
		if (!result) return this.abortAndHangup({ reason: "suspended" });
		return result as WaitForAnyResult<TBranches, Tasks>;
	}

	/**
	 * Wait until an execution settles and resolve with its outcome. A failed or
	 * cancelled execution does not fail this one, and a timeout resolves to
	 * `{ status: "timeout" }` without cancelling it. A `ctx.waitForAny()` with one
	 * branch, so the outcome is memoized by the step key and `<stepKey>:result`.
	 * Throws if the execution does not exist, including one removed by retention
	 * before the wait started. Pass the task to type the result; an execution of
	 * another task is not found.
	 */
	async waitForResult<TResult = unknown>(
		stepKey: string,
		executionId: string,
		options?: { timeout?: DurationInput },
	): Promise<ExecutionOutcome<TResult> | { status: "timeout" }>;
	async waitForResult<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		stepKey: string,
		task: TaskIdentifier<TName, TQueue>,
		executionId: string,
		options?: { timeout?: DurationInput },
	): Promise<
		| ExecutionOutcome<InferReturns<FindTaskByIdentifier<Tasks, TName, TQueue>>>
		| { status: "timeout" }
	>;
	async waitForResult(
		stepKey: string,
		taskOrId: TaskIdentifier<string, string> | string,
		idOrOptions?: string | { timeout?: DurationInput },
		options?: { timeout?: DurationInput },
	): Promise<ExecutionOutcome | { status: "timeout" }> {
		const [task, executionId, waitOptions] =
			typeof taskOrId === "string"
				? [undefined, taskOrId, idOrOptions as { timeout?: DurationInput } | undefined]
				: [taskOrId, idOrOptions as string, options];
		const winner = await this.waitForAny(
			stepKey,
			{ result: { execution: executionId, task } },
			waitOptions,
		);
		return winner.key === "timeout" ? { status: "timeout" } : winner.outcome;
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
			/** Throw and cancel the child when it has not completed after this long; numbers are milliseconds */
			timeout?: DurationInput;
			group?: string;
			/** Metadata for this child only. Defaults to the metadata of this execution. */
			metadata?: Metadata | ((metadata: Readonly<Metadata> | undefined) => Metadata);
		} = {},
	): Promise<InferReturns<TDef>> {
		const timeoutMs = timeout === undefined ? undefined : parseDuration(timeout);
		const cached = (await this.opts.db.loadStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				key,
			},
			{ signal: this.signal },
		)) as { status: "completed"; result: InferReturns<TDef> } | { status: "timed_out" } | undefined;

		if (cached?.status === "completed") {
			return cached.result;
		}

		// A claim keeps waiting_step_key only while the execution waits on a child
		if (cached === undefined && this.opts.execution.waiting_step_key !== key) {
			const childMetadata = await this.childMetadata(metadata);
			return this.abortAndHangup({
				reason: "child-invocation",
				timeout_ms: timeoutMs === undefined ? "infinity" : timeoutMs,
				task,
				step_key: key,
				payload,
				group,
				trace_context: this.opts.telemetry.traceContext(),
				metadata: childMetadata,
			});
		}

		// Resumed at this key without the child's result: the timeout passed. Clearing the wait
		// memoizes the timeout, so a replay throws it again.
		if (cached === undefined) {
			const cleared = await this.opts.db.clearWaitingState(
				{
					executionId: this.opts.execution.id,
					queue: this.opts.execution.queue,
					orchestratorId: this.opts.execution.locked_by,
					claimToken: this.opts.execution.claim_token,
				},
				{ signal: this.signal },
			);
			if (!cleared) return this.abortAndHangup({ reason: "suspended" });
		}

		throw new Error(
			timeoutMs === undefined
				? "Child execution timeout"
				: `Child execution timed out after ${timeoutMs}ms`,
		);
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

		await this.opts.db.scheduleCronExecution({
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
		});
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

	/*
	 * Reads, as `conductor`'s: they read the current rows on every call and are not memoized like
	 * steps. A read that decides what a durable step does belongs inside that step.
	 */

	/** Like `conductor.getExecution()`: an execution by id, by task and id, or by key or dedupe key. */
	async getExecution(executionId: string): Promise<ExecutionInfo | null>;
	async getExecution<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		task: TaskIdentifier<TName, TQueue>,
		executionId: string,
	): Promise<TaskExecutionInfo<FindTaskByIdentifier<Tasks, TName, TQueue>> | null>;
	async getExecution<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		lookup: ExecutionLookup<TName, TQueue>,
	): Promise<TaskExecutionInfo<FindTaskByIdentifier<Tasks, TName, TQueue>> | null>;
	async getExecution(
		target: TaskIdentifier<string, string> | ExecutionLookup<string, string> | string,
		executionId?: string,
	): Promise<ExecutionInfo | null> {
		return readExecution(this.opts.db, target, executionId, { signal: this.signal });
	}

	/** Like `conductor.listExecutions()`: a task's executions, typed by the task. */
	async listExecutions<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		query: ExecutionQuery<TName, TQueue, InferPayload<FindTaskByIdentifier<Tasks, TName, TQueue>>>,
	): Promise<TaskExecutionInfo<FindTaskByIdentifier<Tasks, TName, TQueue>>[]>;
	async listExecutions(query: ExecutionQuery<string, string, Payload>): Promise<ExecutionInfo[]> {
		return readExecutions(this.opts.db, query, { signal: this.signal });
	}

	/** Like `conductor.listListeners()`: the open executions an event would reach. */
	async listListeners<TName extends TaskName<Tasks>, TQueue extends string = "default">(query: {
		event: EventName<Events>;
		payload: Payload;
		task: TaskIdentifier<TName, TQueue>;
		limit?: number;
	}): Promise<TaskListenerInfo<FindTaskByIdentifier<Tasks, TName, TQueue>>[]>;
	async listListeners(query: {
		event: EventName<Events>;
		payload: Payload;
		task?: undefined;
		limit?: number;
	}): Promise<ListenerInfo[]>;
	async listListeners(query: {
		event: string;
		payload: Payload;
		task?: TaskIdentifier<string, string>;
		limit?: number;
	}): Promise<ListenerInfo[]> {
		return readListeners(this.opts.db, query, { signal: this.signal });
	}

	/** Like `conductor.getEvent()`: an emitted event, or null once retention removed it. */
	async getEvent(eventId: string): Promise<EventInfo | null> {
		return this.opts.db.getEvent(eventId, { signal: this.signal });
	}

	/** Like `conductor.listEvents()`: the kept events named `name`. */
	async listEvents<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		query: EventQuery<TName, InferEventPayload<TDef>>,
	): Promise<EventInfo<TName, InferEventPayload<TDef>>[]> {
		const events = await readEvents(this.opts.db, query, { signal: this.signal });
		return events as EventInfo<TName, InferEventPayload<TDef>>[];
	}

	/** Like `conductor.countEvents()`: how many kept events `listEvents()` would read. */
	async countEvents<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		query: Omit<EventQuery<TName, InferEventPayload<TDef>>, "order" | "limit" | "offset">,
	): Promise<number> {
		return this.opts.db.countEvents(eventFilter(query), { signal: this.signal });
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
	 * Wait until `fn` resolves while no signal is delivered, then stop delivering signals to this
	 * attempt. `fn` runs again whenever a signal handler started while it ran, or one is still to
	 * run. Signals stored later stay undelivered: they re-queue the execution when the attempt
	 * settles, or wake an actor's next activation.
	 *
	 * An actor uses it to know that it may release what its activation holds, since no handler
	 * can use it any more.
	 */
	async untilIdle(fn: () => Promise<unknown>): Promise<void> {
		const signals = this.opts.signals;
		for (;;) {
			const since = signals.started;
			await fn();
			await this.checkpoint();
			if (signals.stopIfQuiet(since)) return;
			await signals.whenQuiet();
		}
	}

	/**
	 * Receive the events that match `filter` as signals, from now on and across attempts and
	 * activations, until `ctx.unlisten(name)` or the execution ends. Each arrives as a signal named
	 * after the event, with the event's payload and its id as signal id, so handle it with
	 * `ctx.onSignal(event.name, ...)`; a signal also wakes an idle actor. Listening again under a
	 * name replaces its listen, unless event and filter stay the same. Not memoized: a replay
	 * listens again. Hangs up when this attempt no longer holds the execution.
	 */
	async listen<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(name: string, options: { event: TDef; filter?: FilterForEvent<TDef> }): Promise<void> {
		await this.setListens({ [name]: options }, []);
	}

	/** Stop receiving the events of the listen `name`, if there is one. */
	async unlisten(name: string): Promise<void> {
		await this.setListens({}, [name]);
	}

	/**
	 * One write at a time, so `ctx.listens` is the list the last write returned even when an
	 * attempt listens concurrently.
	 */
	private setListens(listens: Record<string, Listen>, unlisten: string[]): Promise<void> {
		const write = this.listensWrite.then(() => this.writeListens(listens, unlisten));
		this.listensWrite = write.catch(() => {});
		return write;
	}

	private async writeListens(listens: Record<string, Listen>, unlisten: string[]): Promise<void> {
		const current = await this.opts.db.setListens(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
				claimToken: this.opts.execution.claim_token,
				listens: compileListens(listens, this.opts.eventDefinitions),
				unlisten,
			},
			{ signal: this.signal },
		);
		if (!current) return this.abortAndHangup({ reason: "suspended" });
		this.opts.execution.listens = current;
	}

	/**
	 * Replace what this execution tells others about itself: `conductor.getExecution()` and
	 * `listExecutions()` return it. Validated against the task's state schema, which throws on
	 * issues and writes nothing; the schema's output is written. `null` clears the state.
	 * Written at once and not memoized, so a replay writes it again. Runs after the state writes
	 * this attempt started before. Hangs up when this attempt no longer holds the execution.
	 */
	async setState(state: State | null): Promise<void> {
		await this.queueStateWrite(() => this.writeState(state));
	}

	/**
	 * Apply `fn` to the current state and write what it returns like `ctx.setState()`; returning
	 * `undefined` writes nothing. Runs after the state writes this attempt started before, so each
	 * `fn` sees the state the previous one left, and concurrent updates (from signal handlers, say)
	 * are not lost. `fn` must not write the state itself, or it waits for itself.
	 * @returns The state written, as the schema output it, or `undefined` if `fn` wrote nothing
	 */
	async updateState(
		fn: (
			state: Readonly<State> | null,
		) => State | null | undefined | Promise<State | null | undefined>,
	): Promise<State | null | undefined> {
		return this.queueStateWrite(async () => {
			const state = await fn(this.state);
			if (state === undefined) return undefined;
			await this.writeState(state);
			return this.state as State | null;
		});
	}

	private queueStateWrite<T>(write: () => Promise<T>): Promise<T> {
		const queued = this.stateWrites.then(write);
		// A failed write leaves the state as it was, and the next one runs
		this.stateWrites = queued.catch(() => {});
		return queued;
	}

	private async writeState(state: State | null): Promise<void> {
		const validated =
			state === null
				? null
				: ((await validateSchema(
						this.opts.stateSchema,
						state,
						`state for task "${this.opts.execution.task_key}"`,
					)) as Payload);
		const saved = await this.opts.db.setState(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
				claimToken: this.opts.execution.claim_token,
				state: validated,
			},
			{ signal: this.signal },
		);
		if (!saved) return this.abortAndHangup({ reason: "suspended" });
		this.opts.execution.state = validated;
	}

	/**
	 * Wake this actor at `at` once its activation returned, unless a signal wakes it earlier.
	 * Each activation sets its own alarm; `null`, the default, sleeps until a signal.
	 */
	setAlarm(at: Date | null): void {
		if (!this.opts.actor) {
			throw new Error("Only an actor can set an alarm");
		}
		this.opts.actor.alarm = at;
	}

	/**
	 * Signal an execution by id, or the open execution of `{ task, key }` that holds the key.
	 * Idempotent by signal id: resending an id stores the signal once.
	 * @returns false if the execution is completed, failed, cancelled or unknown, or no open
	 * execution holds the key
	 */
	async sendSignal<TName extends TaskName<Tasks>, TQueue extends string = "default">(
		target: string | KeyedExecution<TName, TQueue>,
		signal: SignalMessage,
	): Promise<boolean> {
		return this.opts.db.signalExecution(
			{ ...signalTarget(target), signal },
			{ signal: this.signal },
		);
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
			/**
			 * State of the execution (see `ctx.setState()`), used only when it starts one. Validated
			 * against the task's state schema.
			 */
			state?: InferState<TDef>;
			/** Listens of the execution by name (see `ctx.listen()`), registered only when it starts one */
			listen?: Record<string, Listen>;
			priority?: number;
			/**
			 * Metadata of the execution, used only when it starts one. Defaults to the metadata of
			 * this execution.
			 */
			metadata?: Metadata | ((metadata: Readonly<Metadata> | undefined) => Metadata);
		},
	): Promise<string> {
		await this.checkpoint();
		const metadata = await this.childMetadata(options.metadata);
		const queue = options.queue || "default";
		const state =
			options.state &&
			(await validateState(
				this.opts.taskDefinitions || [],
				{ name: options.task, queue },
				options.state,
			));

		const id = await this.opts.db.signalWithStartStep(
			{
				executionId: this.opts.execution.id,
				queue: this.opts.execution.queue,
				orchestratorId: this.opts.execution.locked_by,
				claimToken: this.opts.execution.claim_token,
				key: stepKey,
				target: {
					taskKey: options.task,
					queue,
					key: options.key,
					signal: options.signal,
					input: (options.input || {}) as Payload,
					state,
					listens: compileListens(options.listen || {}, this.opts.eventDefinitions),
					priority: options.priority,
					traceContext: this.opts.telemetry.traceContext(),
					metadata,
				},
			},
			{ signal: this.signal },
		);
		return id || this.abortAndHangup({ reason: "suspended" });
	}

	/**
	 * Emit a typed custom event.
	 * @param event - Event name to emit
	 * @param payload - Typed event payload
	 * @param opts.dedupe_key - Repeating a key for the same event name emits nothing and returns the original event ID
	 * @param opts.metadata - Metadata of the event, passed to the executions it triggers. Defaults to the metadata of this execution
	 * @returns Event ID
	 */
	async emit<
		TName extends EventName<Events>,
		TDef extends FindEventByIdentifier<Events, TName> = FindEventByIdentifier<Events, TName>,
	>(
		event: TName,
		payload: InferEventPayload<TDef>,
		opts?: {
			dedupe_key?: string;
			metadata?: Metadata | ((metadata: Readonly<Metadata> | undefined) => Metadata);
		},
	): Promise<string> {
		payload = await validateEventPayload(this.opts.eventDefinitions, event, payload);
		const metadata = await this.childMetadata(opts?.metadata);
		return this.opts.db.emitEvent(
			{
				eventKey: event,
				payload: payload as any,
				traceContext: this.opts.telemetry.traceContext(),
				metadata,
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
	private abortAndHangup(reason: TaskAbortReasons): Promise<never> {
		this.opts.abortController.abort(reason);
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
	async sleep(id: string, duration: DurationInput): Promise<void> {
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
			reason: "released",
			reschedule_in_ms: parseDuration(duration),
			step_key: id,
		});
		return new Promise(() => {});
	}
}

import postgres, { type Sql } from "postgres";
import * as assert from "./lib/assert.js";
import { waitFor } from "./lib/wait-for.js";
import type { Migration } from "./migration-store.js";
import {
	QueryBuilder,
	type OrchestratorHeartbeatArgs,
	type RegisterOrchestratorArgs,
	type RecoverStaleOrchestratorsArgs,
	type SweepOrchestratorsArgs,
	type OrchestratorShutdownArgs,
	type CountActiveOrchestratorsBelowArgs,
	type GetExecutionsArgs,
	type RemoveExecutionsArgs,
	type ClaimUnregisteredExecutionsArgs,
	type DispatchCustomEventsArgs,
	type RegisterWorkerArgs,
	type ScheduleCronExecutionArgs,
	type UnscheduleCronExecutionArgs,
	type ScheduleNextCronExecutionArgs,
	type GetExecutionArgs,
	type EventFilterArgs,
	type ListEventsArgs,
	type ListExecutionsArgs,
	type LoadStepArgs,
	type CountStepsArgs,
	type SaveStepArgs,
	type StartExecutionArgs,
	type ClearWaitingStateArgs,
	type EmitEventArgs,
	type RegisterEventWaitArgs,
	type AckSignalsArgs,
	type SignalExecutionArgs,
	type SignalWithStartArgs,
	type SignalWithStartStepArgs,
	type SetStateArgs,
	type SetListensArgs,
	type ListListenersArgs,
	type RegisterEventRaceArgs,
	type StartedExecution,
} from "./query-builder.js";
import { makeChildLogger, type Logger } from "./lib/logger.js";
import { uuidv7 } from "./lib/uuidv7.js";
import type { TraceContextCarrier } from "./telemetry.js";

export type JsonValue = string | number | boolean | null | Payload | JsonValue[];
export type Payload = { [key: string]: JsonValue };

/**
 * The fields a payload contains, as a jsonb containment (`@>`) matches them: any of an object's
 * fields, also of nested objects; arrays and scalars as they are.
 */
export type Contained<T> = T extends readonly unknown[]
	? T
	: T extends object
		? { [K in keyof T]?: Contained<T[K]> }
		: T;

export interface ExecutionSpec {
	task_key: string;
	queue: string;
	payload?: Payload | null;
	run_at?: Date | null;
	dedupe_key?: string | null;
	throttle?: { seconds: number } | null;
	debounce?: { seconds: number } | null;
	cron_expression?: string | null;
	priority?: number | null;
	group?: string | null;
	parent_execution_id?: string | null;
	parent_step_key?: string | null;
	parent_timeout_ms?: number | null;
	trace_context?: TraceContextCarrier | null;
	metadata?: Payload | null;
}

export interface TaskSpec {
	key: string;
	queue?: string | null;
	maxAttempts?: number | null;
	removeOnCompleteDays?: number | null;
	removeOnFailDays?: number | null;
	window?: [string, string] | null;
	concurrency?: number | null;
	groupConcurrency?: number | null;
	deadLetterQueue?: string | null;
	deadLetterTaskKey?: string | null;
}

export type DeadLetterMetadata = {
	sourceExecutionId: string;
	sourceQueue: string;
	sourceTaskKey: string;
	error: string | null;
	attempts: number;
	failedAt: Date;
};

export type ExecutionStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export type ExecutionInfo<TResult = unknown, TPayload = Payload, TState = Payload> = {
	id: string;
	taskKey: string;
	queue: string;
	status: ExecutionStatus;
	/** The key `signalWithStart()` addresses it by */
	key: string | null;
	/** The execution's input */
	payload: TPayload;
	/** What the execution last told others about itself with `ctx.setState()` */
	state: TState | null;
	/**
	 * Names of its listens (`ctx.listen()`, `signalWithStart({ listen })`) in the order they were
	 * created. Listens end with their execution, so an ended one has none.
	 */
	listens: string[];
	/** Return value of a completed execution */
	result: TResult | null;
	/** Error of the last failed attempt, or the cancellation reason */
	error: string | null;
	attempts: number;
	createdAt: Date;
	completedAt: Date | null;
	failedAt: Date | null;
};

/** An open execution an event would reach, as `conductor.listListeners()` returns it. */
export type ListenerInfo<TResult = unknown, TPayload = Payload, TState = Payload> = ExecutionInfo<
	TResult,
	TPayload,
	TState
> & {
	/** Names of the listens the event matches, in the order they were created */
	matched: string[];
};

/** An emitted event, while retention keeps it. */
export type EventInfo<TName extends string = string, TPayload = Payload> = {
	id: string;
	name: TName;
	payload: TPayload;
	createdAt: Date;
};

/** How an execution settled, as `ctx.waitForResult()` and `ctx.waitForAny()` return it. */
export type ExecutionOutcome<TResult = unknown> =
	| { status: "completed"; result: TResult }
	| { status: "failed"; error: string }
	| { status: "cancelled"; reason: string };

export interface Execution {
	id: string;
	task_key: string;
	queue: string;
	payload: Payload;
	waiting_on_execution_id: string | null;
	waiting_step_key: string | null;
	locked_by: string;
	claim_token: string;
	cancelled: boolean;
	last_error: string | null;
	dedupe_key?: string | null;
	cron_expression?: string | null;
	group?: string | null;
	subscription_id?: string | null;
	/** The event of an event delivery (`subscription_id` set), or the parent of a child */
	parent_execution_id?: string | null;
	dead_letter?: (Omit<DeadLetterMetadata, "failedAt"> & { failedAt: string }) | null;
	trace_context?: TraceContextCarrier | null;
	metadata?: Payload | null;
	/** The state as of the claim, set by `ctx.setState()` or `signalWithStart()` */
	state?: Payload | null;
	/** Undelivered signals in stored order, read when the execution was claimed */
	signals: StoredSignal[];
	/** Names of its listens in the order they were created, read when the execution was claimed */
	listens: string[];
	/** When the execution was created, as an ISO timestamp */
	created_at: string;
}

// todo: move all of this to query-builder too or create new types.ts file

export type ExecutionResult =
	| ExecutionCompleted
	| ExecutionFailed
	| ExecutionReleased
	| ExecutionIdle
	| ExecutionPermamentlyFailed
	| ExecutionInvokeChild;

export type ReturnExecutionsArgs = {
	orchestratorId: string;
	/** The queue of every settled execution */
	queueName: string;
	results: ExecutionResult[];
};

export type ExecutionCompleted = {
	execution_id: string;
	task_key: string;
	claim_token: string;
	status: "completed";
	result?: Payload;
	/** Signal names the attempt listened for; undelivered ones re-queue the execution */
	signal_names?: string[];
	/** Completed by `ctx.complete()`: undelivered signals do not re-queue it */
	final?: boolean;
};

export type ExecutionFailed = {
	execution_id: string;
	task_key: string;
	claim_token: string;
	status: "failed";
	error: string;
};

export type ExecutionReleased = {
	execution_id: string;
	task_key: string;
	claim_token: string;
	status: "released";
	reschedule_in_ms?: number | "infinity";
	step_key?: string;
};

/** An activation of an actor ended: it sleeps until a signal, or until `reschedule_in_ms` passed */
export type ExecutionIdle = {
	execution_id: string;
	task_key: string;
	claim_token: string;
	status: "idle";
	reschedule_in_ms: number | "infinity";
	/** Signal names the attempt listened for; undelivered ones start the next activation at once */
	signal_names?: string[];
};

export type ExecutionPermamentlyFailed = {
	execution_id: string;
	task_key: string;
	claim_token: string;
	status: "permanently_failed";
	error: string;
};

export type ExecutionInvokeChild = {
	group?: string | null;
	execution_id: string;
	task_key: string;
	claim_token: string;
	status: "invoke_child";
	timeout_ms: number | "infinity";
	step_key: string;
	child_task_name: string;
	child_task_queue: string;
	child_payload: Payload | null;
	trace_context?: TraceContextCarrier | null;
	metadata?: Payload | null;
};

export type EventFilterTerm = Record<string, JsonValue | undefined> & {
	field_name: string;
	operator: "exact" | "prefix" | "numeric_range" | "exists" | "anything_but";
	scalar_type?: "string" | "number" | "boolean" | "null";
	text_value?: string;
	number_value?: number;
	boolean_value?: boolean;
	lower_value?: number | null;
	upper_value?: number | null;
	lower_inclusive?: boolean;
	upper_inclusive?: boolean;
};

/** A signal stored for an execution and not yet delivered to a handler. */
export type StoredSignal = {
	seq: number;
	id: string;
	name: string;
	payload: JsonValue;
	/** Names of the execution's listens the event of this signal matched; empty for others */
	listens: string[];
	/** The sender's trace, or the event's for a listen; its handler continues it */
	trace_context?: TraceContextCarrier | null;
};

/** SQLSTATE `_private_put_listens()` raises for an exclusive listen another execution holds */
const LISTEN_HELD = "PGC02";

/**
 * An exclusive listen (`ctx.listen(name, { exclusive: true })`, `signalWithStart({ listen })`)
 * was refused: another open execution of the task holds the name exclusively. Nothing was written.
 */
export class ListenHeldError extends Error {
	readonly code = "PGCONDUCTOR_LISTEN_HELD";
	constructor(
		/** The name of the listen */
		public readonly listen: string,
		/** The execution that holds it */
		public readonly heldBy: string,
	) {
		super(`Listen "${listen}" is held by execution ${heldBy}`);
		this.name = "ListenHeldError";
	}
}

/** Reads the holder `_private_put_listens()` reports as JSON detail */
function listenHeldError(detail: string | undefined): ListenHeldError | null {
	if (!detail) return null;
	const held: unknown = JSON.parse(detail);
	if (
		typeof held === "object" &&
		held !== null &&
		"listen" in held &&
		typeof held.listen === "string" &&
		"execution_id" in held &&
		typeof held.execution_id === "string"
	) {
		return new ListenHeldError(held.listen, held.execution_id);
	}
	return null;
}

/** Executions claimed by a poll, and what changed for the running executions it was given. */
export type ClaimResult = {
	executions: Execution[];
	/** New signals of running executions that listen for any */
	signals: (StoredSignal & { execution_id: string })[];
	/** Running executions that were cancelled */
	cancelled: string[];
};

export type EventWaitResult =
	| { status: "resolved"; event: { name: string; payload: Payload } }
	| { status: "timed_out" }
	| { status: "closed" };

export type EventRaceBranch = {
	key: string;
	stepKey: string;
	executionId?: string;
	taskKey?: string;
	queue?: string;
	signal?: string;
};

export type EventRaceResult = {
	key: string;
	event?: { name: string; payload: Payload };
	outcome?: ExecutionOutcome;
	signal?: { id: string; name: string; payload: JsonValue };
};

export interface EventSubscriptionSpec {
	task_key: string;
	event_key: string;
	payload_fields: string[] | null;
	required_field_count: number;
	terms: EventFilterTerm[];
	/** Fires only for events no execution listens for */
	unlistened: boolean;
}

const RETRYABLE_SQLSTATE_CODES = new Set([
	"40001", // serialization_failure
	"40P01", // deadlock_detected
	"55P03", // lock_not_available
	"57P01", // admin_shutdown
	"57P02", // crash_shutdown
	"57P03", // cannot_connect_now
	"53300", // too_many_connections
	"08000", // connection_exception
	"08003", // connection_does_not_exist
	"08006", // connection_failure
	"08001", // sqlclient_unable_to_establish_sqlconnection
]);

const RETRYABLE_SYSTEM_ERROR_CODES = new Set([
	"ECONNRESET",
	"ECONNREFUSED",
	"EPIPE",
	"ETIMEDOUT",
	"EHOSTUNREACH",
	"ENETUNREACH",
	"CONNECTION_CLOSED",
	"CONNECTION_DESTROYED",
	"CONNECT_TIMEOUT",
	"ENOTFOUND",
	"EAI_AGAIN",
	"ECONNABORTED",
	"EPROTO",
	"ERR_TLS_HANDSHAKE_TIMEOUT",
	"ERR_TLS_CERT_ALTNAME_INVALID",
	"ERR_TLS_REQUIRED_SERVER_NAME",
	"ERR_TLS_CERT_SIGNATURE_ALGORITHM_UNSUPPORTED",
	"ERR_TLS_PSK_SET_IDENTIY_HINT_FAILED",
	"ERR_TLS_DH_PARAM_SIZE",
	"ERR_SSL_WRONG_VERSION_NUMBER",
]);

const MIN_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 60_000;
const BACKOFF_MULTIPLIER = 2;

type DatabaseClientOptions =
	| { connectionString: string; sql?: never; logger: Logger }
	| { sql: Sql; connectionString?: never; logger: Logger; ownsSql?: boolean };

type QueryOptions = {
	label?: string;
	expectError?: boolean;
	signal?: AbortSignal;
};

// A connection error is ambiguous: the statement may have committed. Only methods whose
// statement can run again without a second effect take a signal, which enables retries.
type QueryMethodOptions = Pick<QueryOptions, "signal">;

type ErrorWithOptionalCode = Error & { code?: string; detail?: string };

// postgres.js takes the options it parsed as they are, as it does for its own listen connection
function isParsedOptions(options: object): options is postgres.Options<{}> {
	return "shared" in options;
}

export type SetFakeTimeArgs = {
	date: Date;
};

export class DatabaseClient {
	private readonly sql: Sql;
	private readonly builder: QueryBuilder;
	private readonly ownsSql: boolean;
	private readonly logger: Logger;

	constructor(options: DatabaseClientOptions) {
		if ("sql" in options && options.sql) {
			this.sql = options.sql;
			this.ownsSql = options.ownsSql || false;
		} else if ("connectionString" in options && options.connectionString) {
			this.sql = postgres(options.connectionString);
			this.ownsSql = true;
		} else {
			throw new Error(
				"DatabaseClient requires either a connectionString or an existing postgres instance",
			);
		}

		this.logger = makeChildLogger(options.logger, {
			component: "DatabaseClient",
		});
		this.builder = new QueryBuilder(this.sql);
	}

	/**
	 * A client on a connection of its own, so its queries never wait for a busy pool; `close()` ends it.
	 * Like postgres.js's listen connection, it is never closed for being idle or old
	 */
	withOwnConnection(): DatabaseClient {
		const options: object = this.sql.options;
		assert.ok(isParsedOptions(options));
		return new DatabaseClient({
			// postgres.js disables a timer whose value is falsy; its types allow `null` only for max_lifetime
			sql: postgres({ ...options, max: 1, idle_timeout: undefined, max_lifetime: null }),
			logger: this.logger,
			ownsSql: true,
		});
	}

	async close(): Promise<void> {
		if (!this.ownsSql) return;
		await this.sql.end({ timeout: 0 });
	}

	private async query<T>(callback: (sql: Sql) => Promise<T>, options?: QueryOptions): Promise<T> {
		const logger = makeChildLogger(this.logger, { label: options?.label });

		const label = options?.label ? ` (${options.label})` : "";
		let attempt = 0;
		while (true) {
			try {
				return await callback(this.sql);
			} catch (error) {
				const err = error as ErrorWithOptionalCode;
				// A refusal, not a failure: thrown as is
				const held = err.code === LISTEN_HELD ? listenHeldError(err.detail) : null;
				if (held) throw held;
				if (!this.isRetryableError(err)) {
					if (!options?.expectError) {
						logger.error(`Non-retryable database error${label}: ${err.message}`);
					}
					throw err;
				}
				if (!options?.signal) {
					logger.warn(
						`Retryable database error, but no signal provided. Not retrying: ${err.message}`,
					);
					throw err;
				}

				logger.warn(`Retryable database error${label}: ${err.message}`);

				attempt += 1;
				const delay = Math.min(
					MAX_RETRY_DELAY_MS,
					MIN_RETRY_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, attempt - 1),
				);
				await waitFor(delay, {
					jitter: delay / 2,
					signal: options.signal,
				});

				if (options.signal.aborted) {
					logger.warn(`Aborting retries due to signal${label}`);
					throw err;
				}
			}
		}
	}

	private isRetryableError(err: ErrorWithOptionalCode): boolean {
		const code = err.code;
		if (!code) {
			return false;
		}
		return RETRYABLE_SQLSTATE_CODES.has(code) || RETRYABLE_SYSTEM_ERROR_CODES.has(code);
	}

	async registerOrchestrator(
		args: RegisterOrchestratorArgs,
		opts?: QueryMethodOptions,
	): Promise<{ shutdown_reason: string | null }> {
		const [row] = await this.query(() => this.builder.buildRegisterOrchestrator(args), {
			label: "registerOrchestrator",
			...opts,
		});
		return row;
	}

	/** Returns undefined once the orchestrator was recovered as stale */
	async orchestratorHeartbeat(
		args: OrchestratorHeartbeatArgs,
		opts?: QueryMethodOptions,
	): Promise<{ shutdown_reason: string | null } | undefined> {
		const [row] = await this.query(() => this.builder.buildOrchestratorHeartbeat(args), {
			label: "orchestratorHeartbeat",
			...opts,
		});
		return row;
	}

	async ackSignals(args: AckSignalsArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildAckSignals(args), {
			label: "ackSignals",
			...opts,
		});
	}

	async signalExecution(args: SignalExecutionArgs, opts?: QueryMethodOptions): Promise<boolean> {
		const [row] = await this.query(() => this.builder.buildSignalExecution(args), {
			label: "signalExecution",
			...opts,
		});
		return row.stored;
	}

	async signalWithStart(args: SignalWithStartArgs): Promise<StartedExecution> {
		const [row] = await this.query(() => this.builder.buildSignalWithStart(args), {
			label: "signalWithStart",
		});
		return { id: row.id, started: row.started };
	}

	/** Returns null when this worker no longer holds the signalling execution. */
	async signalWithStartStep(
		args: SignalWithStartStepArgs,
		opts?: QueryMethodOptions,
	): Promise<StartedExecution | null> {
		const rows = await this.query(() => this.builder.buildSignalWithStartStep(args), {
			label: "signalWithStartStep",
			...opts,
		});
		const row = rows[0];
		return row ? { id: row.id, started: row.started === true } : null;
	}

	async cancelExecution(
		executionId: string,
		options?: { reason?: string } & QueryMethodOptions,
	): Promise<boolean> {
		const result = await this.query(
			(sql) =>
				sql<{ cancel_execution: boolean }[]>`
				select pgconductor.cancel_execution(
					${executionId}::uuid,
					${options?.reason || "Cancelled by user"}::text
				) as cancel_execution
			`,
			{ label: "cancelExecution", ...options },
		);
		return result[0]?.cancel_execution || false;
	}

	async getDatabaseTime(options?: QueryMethodOptions): Promise<Date> {
		const result = await this.query(
			(sql) =>
				sql<{ now: Date }[]>`
				select pgconductor._private_current_time() as now
			`,
			{ label: "getDatabaseTime", ...options },
		);
		const row = result[0];
		if (!row) {
			throw new Error("getDatabaseTime returned no rows");
		}
		return row.now;
	}

	async recoverStaleOrchestrators(
		args: RecoverStaleOrchestratorsArgs,
		opts?: QueryMethodOptions,
	): Promise<void> {
		await this.query(() => this.builder.buildRecoverStaleOrchestrators(args), {
			label: "recoverStaleOrchestrators",
			...opts,
		});
	}

	async sweepOrchestrators(args: SweepOrchestratorsArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildSweepOrchestrators(args), {
			label: "sweepOrchestrators",
			...opts,
		});
	}

	/**
	 * Get the currently installed migration version
	 * Returns -1 if schema doesn't exist (not installed)
	 * Returns the highest migration version from schema_migrations table
	 */
	async getInstalledMigrationNumber(opts?: QueryMethodOptions): Promise<number> {
		try {
			const [{ version }] = await this.query(
				() => this.builder.buildGetInstalledMigrationNumber(),
				{
					label: "buildGetInstalledMigrationNumber",
					expectError: true,
					...opts,
				},
			);
			return version;
		} catch (err) {
			const pgErr = err as { code?: string };
			if (pgErr?.code === "42P01" || pgErr?.code === "3F000") {
				return -1;
			}
			throw err;
		}
	}

	// With `fence`, a breaking migration is admitted only while no older orchestrator is
	// registered. Locking the table makes an older orchestrator that registers concurrently wait
	// for the commit, so its registration sees the new version and it stops.
	async applyMigration(
		migration: Migration,
		opts?: { fence?: boolean } & QueryMethodOptions,
	): Promise<"applied" | "busy"> {
		return this.query(
			async (sql) => {
				return sql.begin(async (tx) => {
					const lockResult = await tx<{ locked: boolean }[]>`
						select pg_try_advisory_xact_lock(hashtext('pgconductor:migrations')) as locked
					`;

					if (!lockResult[0]?.locked) {
						return "busy" as const;
					}

					const applied =
						migration.version > 0
							? (
									await tx<{ exists: boolean }[]>`
						select exists (
							select 1
							from pgconductor.schema_migrations
							where version = ${migration.version}
						) as exists
					`
								).shift()?.exists
							: false;

					if (applied) {
						return "applied" as const;
					}

					if (opts?.fence) {
						await tx`lock table pgconductor._private_orchestrators in share mode`;
						const [{ registered }] = await tx<[{ registered: boolean }]>`
							select exists (
								select 1
								from pgconductor._private_orchestrators
								where migration_number < ${migration.version}::integer
							) as registered
						`;
						if (registered) {
							return "busy" as const;
						}
					}

					await tx.unsafe(migration.sql);
					// Migration 0 creates the ledger, so it is not checked above and may already be recorded
					await tx`
						insert into pgconductor.schema_migrations (version, name, breaking)
						values (${migration.version}, ${migration.name}, ${migration.breaking})
						on conflict (version) do nothing
					`;

					return "applied" as const;
				});
			},
			{ label: "applyMigration", ...opts },
		);
	}

	async countActiveOrchestratorsBelow(
		args: CountActiveOrchestratorsBelowArgs,
		opts?: QueryMethodOptions,
	): Promise<number> {
		try {
			const result = await this.query(() => this.builder.buildCountActiveOrchestratorsBelow(args), {
				label: "countActiveOrchestratorsBelow",
				...opts,
			});
			return Number(result[0]?.count || 0);
		} catch (err) {
			const pgErr = err as { code?: string };
			if (pgErr?.code === "42P01") {
				return 0;
			}
			throw err;
		}
	}

	async orchestratorShutdown(
		args: OrchestratorShutdownArgs,
		opts?: QueryMethodOptions,
	): Promise<void> {
		try {
			await this.query(() => this.builder.buildOrchestratorShutdown(args), {
				label: "orchestratorShutdown",
				expectError: true,
				...opts,
			});
		} catch (err) {
			const pgErr = err as { code?: string };
			// Not installed yet
			if (pgErr?.code === "3F000" || pgErr?.code === "42883") {
				return;
			}
			throw err;
		}
	}

	// Without a signal a claim is never retried: one whose response was lost may have committed.
	// The next poll claims again, and the heartbeat releases the orphaned claim.
	async getExecutions(args: GetExecutionsArgs): Promise<ClaimResult> {
		const [result] = await this.query(() => this.builder.buildGetExecutions(args), {
			label: "getExecutions",
		});
		return result;
	}

	/** Cancels the read when the signal aborts, so a read blocked on a lock does not outlive it */
	async getExecution(
		args: GetExecutionArgs,
		opts?: QueryMethodOptions,
	): Promise<ExecutionInfo | null> {
		const rows = await this.query(
			async () => {
				const query = this.builder.buildGetExecution(args);
				const cancel = () => query.cancel();
				opts?.signal?.addEventListener("abort", cancel);
				try {
					return await query;
				} finally {
					opts?.signal?.removeEventListener("abort", cancel);
				}
			},
			{ label: "getExecution", ...opts },
		);
		return rows[0] || null;
	}

	async listExecutions(
		args: ListExecutionsArgs,
		opts?: QueryMethodOptions,
	): Promise<ExecutionInfo[]> {
		return this.query(() => this.builder.buildListExecutions(args), {
			label: "listExecutions",
			...opts,
		});
	}

	async getEvent(eventId: string, opts?: QueryMethodOptions): Promise<EventInfo | null> {
		const rows = await this.query(() => this.builder.buildGetEvent(eventId), {
			label: "getEvent",
			...opts,
		});
		return rows[0] || null;
	}

	async listEvents(args: ListEventsArgs, opts?: QueryMethodOptions): Promise<EventInfo[]> {
		return this.query(() => this.builder.buildListEvents(args), {
			label: "listEvents",
			...opts,
		});
	}

	async countEvents(args: EventFilterArgs, opts?: QueryMethodOptions): Promise<number> {
		const [row] = await this.query(() => this.builder.buildCountEvents(args), {
			label: "countEvents",
			...opts,
		});
		return row?.count || 0;
	}

	async returnExecutions(args: ReturnExecutionsArgs, opts?: QueryMethodOptions): Promise<void> {
		// A query runs once, so every attempt builds a new one.
		await this.query(async () => this.builder.buildReturnExecutions(args), {
			label: "returnExecutions",
			...opts,
		});
	}

	async removeExecutions(args: RemoveExecutionsArgs, opts?: QueryMethodOptions): Promise<boolean> {
		const result = await this.query(() => this.builder.buildRemoveExecutions(args), {
			label: "removeExecutions",
			...opts,
		});

		const deletedCount = result[0]?.deleted_count ?? 0;
		return deletedCount >= args.batchSize;
	}

	// Fails the claimed executions through settlement, like a worker would, so they dead-letter,
	// resolve their result waiters, fail the executions waiting on them and end their listens
	async failUnregisteredExecutions(
		args: Omit<ClaimUnregisteredExecutionsArgs, "claimant">,
		opts?: QueryMethodOptions,
	): Promise<boolean> {
		const claimed = await this.query(
			(sql) =>
				sql.begin(async (tx) => {
					const builder = new QueryBuilder(tx);
					const claimant = crypto.randomUUID();
					const executions = await builder.buildClaimUnregisteredExecutions({ ...args, claimant });
					await builder.buildReturnExecutions({
						orchestratorId: claimant,
						queueName: args.queueName,
						results: executions.map((execution) => ({
							execution_id: execution.id,
							task_key: execution.task_key,
							claim_token: execution.claim_token,
							status: "permanently_failed",
							error: `task ${execution.task_key} is not registered on queue ${args.queueName}`,
						})),
					});
					return executions.length;
				}),
			{ label: "failUnregisteredExecutions", ...opts },
		);
		return claimed >= args.batchSize;
	}

	async registerWorker(args: RegisterWorkerArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildRegisterWorker(args), {
			label: "registerWorker",
			...opts,
		});
	}

	async scheduleCronExecution(args: ScheduleCronExecutionArgs): Promise<void> {
		await this.query(() => this.builder.buildScheduleCronExecution(args), {
			label: "scheduleCronExecution",
		});
	}

	async unscheduleCronExecution(
		args: UnscheduleCronExecutionArgs,
		opts?: QueryMethodOptions,
	): Promise<void> {
		await this.query(() => this.builder.buildUnscheduleCronExecution(args), {
			label: "unscheduleCronExecution",
			...opts,
		});
	}

	async scheduleNextCronExecution(
		args: ScheduleNextCronExecutionArgs,
		opts?: QueryMethodOptions,
	): Promise<void> {
		await this.query(() => this.builder.buildScheduleNextCronExecution(args), {
			label: "scheduleNextCronExecution",
			...opts,
		});
	}

	async invoke(spec: ExecutionSpec): Promise<string | null> {
		const result = await this.query(() => this.builder.buildInvoke(spec), { label: "invoke" });
		return result[0]?.id || null;
	}

	async invokeBatch(specs: ExecutionSpec[]): Promise<string[]> {
		const result = await this.query(() => this.builder.buildInvokeBatch(specs), {
			label: "invokeBatch",
		});
		return result.map((r) => r.id);
	}

	async loadStep(
		args: LoadStepArgs,
		opts?: QueryMethodOptions,
	): Promise<Payload | null | undefined> {
		const rows = await this.query(() => this.builder.buildLoadStep(args), {
			label: "loadStep",
			...opts,
		});

		if (!rows[0]) {
			return undefined;
		}
		return rows[0].result;
	}

	async countSteps(args: CountStepsArgs, opts?: QueryMethodOptions): Promise<number> {
		const rows = await this.query(() => this.builder.buildCountSteps(args), {
			label: "countSteps",
			...opts,
		});
		return rows[0].count;
	}

	/** Returns false when this worker no longer holds the execution. */
	async saveStep(args: SaveStepArgs, opts?: QueryMethodOptions): Promise<boolean> {
		const [row] = await this.query(() => this.builder.buildSaveStep(args), {
			label: "saveStep",
			...opts,
		});
		return row.claimed;
	}

	/** Returns false when this worker no longer holds the execution. */
	async setState(args: SetStateArgs, opts?: QueryMethodOptions): Promise<boolean> {
		const [row] = await this.query(() => this.builder.buildSetState(args), {
			label: "setState",
			...opts,
		});
		return row.claimed;
	}

	/** Returns null when this worker no longer holds the starting execution. */
	async startExecution(
		args: StartExecutionArgs,
		opts?: QueryMethodOptions,
	): Promise<StartedExecution | null> {
		const rows = await this.query(() => this.builder.buildStartExecution(args), {
			label: "startExecution",
			...opts,
		});
		const row = rows[0];
		return row ? { id: row.id, started: row.started === true } : null;
	}

	async checkMetadata(metadata: Payload, opts?: QueryMethodOptions): Promise<void> {
		await this.query(() => this.builder.buildCheckMetadata(metadata), {
			label: "checkMetadata",
			...opts,
		});
	}

	/** Returns false when the claim no longer holds the execution */
	async clearWaitingState(
		args: ClearWaitingStateArgs,
		opts?: QueryMethodOptions,
	): Promise<boolean> {
		const rows = await this.query(() => this.builder.buildClearWaitingState(args), {
			label: "clearWaitingState",
			...opts,
		});
		return rows.length > 0;
	}

	async registerEventWait(
		args: RegisterEventWaitArgs,
		opts?: QueryMethodOptions,
	): Promise<EventWaitResult | null> {
		const rows = await this.query(() => this.builder.buildRegisterEventWait(args), {
			label: "registerEventWait",
			...opts,
		});
		return rows[0]?.result || null;
	}

	async registerEventRace(
		args: RegisterEventRaceArgs,
		opts?: QueryMethodOptions,
	): Promise<EventRaceResult | null> {
		const rows = await this.query(() => this.builder.buildRegisterEventRace(args), {
			label: "registerEventRace",
			...opts,
		});
		return rows[0]?.result || null;
	}

	/**
	 * Dispatches claimed events and returns their ids. The events that matched listens are stored
	 * as signals by a second statement, which locks the listening executions apart from the
	 * waiting ones; a retried dispatch stores each once, by the event id.
	 */
	async dispatchCustomEvents(
		args: DispatchCustomEventsArgs,
		opts?: QueryMethodOptions,
	): Promise<string[]> {
		const sources = await this.query(() => this.builder.buildDispatchCustomEvents(args), {
			label: "dispatchCustomEvents",
			...opts,
		});
		const listens = sources.flatMap((source) => source.listens);
		if (listens.length > 0)
			await this.query(() => this.builder.buildDeliverListens(listens), {
				label: "deliverListens",
				...opts,
			});
		return sources.map((source) => source.event_id);
	}

	async listListeners(args: ListListenersArgs, opts?: QueryMethodOptions): Promise<ListenerInfo[]> {
		return this.query(() => this.builder.buildListListeners(args), {
			label: "listListeners",
			...opts,
		});
	}

	/**
	 * Returns the names of the execution's listens in the order they were created, or null when
	 * this worker no longer holds the execution.
	 */
	async setListens(args: SetListensArgs, opts?: QueryMethodOptions): Promise<string[] | null> {
		const [row] = await this.query(() => this.builder.buildSetListens(args), {
			label: "setListens",
			...opts,
		});
		return row.listens;
	}

	async emitEvent(args: EmitEventArgs, opts?: QueryMethodOptions): Promise<string> {
		const id = uuidv7();
		const result = await this.query(() => this.builder.buildEmitEvent({ ...args, id }), {
			label: "emitEvent",
			...opts,
		});
		return result[0]?.id || id;
	}

	/**
	 * Set fake time for testing purposes.
	 * pgconductor._private_current_time() returns this value and keeps running from it.
	 *
	 * IMPORTANT: Test database connection pool must have max: 1 for this to work.
	 */
	async setFakeTime({ date }: SetFakeTimeArgs, opts?: QueryMethodOptions): Promise<void> {
		await this.query(
			(sql) => sql`
				select set_config(
					'pgconductor.fake_offset',
					(${date.toISOString()}::timestamptz - clock_timestamp())::text,
					false
				)
			`,
			{ label: "setFakeTime", ...opts },
		);
	}

	/**
	 * Clear fake time, returning to real clock_timestamp().
	 */
	async clearFakeTime(opts?: QueryMethodOptions): Promise<void> {
		await this.query((sql) => sql.unsafe(`set pgconductor.fake_offset = ''`), {
			label: "clearFakeTime",
			...opts,
		});
	}
}

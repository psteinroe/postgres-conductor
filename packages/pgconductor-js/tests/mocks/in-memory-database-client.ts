import type {
	DatabaseClient,
	Execution,
	ExecutionResult,
	ExecutionSpec,
	TaskSpec,
	Payload,
	EventFilterTerm,
	SetFakeTimeArgs,
} from "../../src/database-client";
import { DatabaseClient as RealDatabaseClient } from "../../src/database-client";
import type {
	OrchestratorHeartbeatArgs,
	RecoverStaleOrchestratorsArgs,
	SweepOrchestratorsArgs,
	CountActiveOrchestratorsBelowArgs,
	GetExecutionsArgs,
	RemoveExecutionsArgs,
	RegisterWorkerArgs,
	ScheduleCronExecutionArgs,
	UnscheduleCronExecutionArgs,
	LoadStepArgs,
	SaveStepArgs,
	ClearWaitingStateArgs,
	OrchestratorShutdownArgs,
	EmitEventArgs,
	RegisterEventWaitArgs,
} from "../../src/query-builder";
import type { Migration } from "../../src/migration-store";
import type { Logger } from "../../src/lib/logger";
import CronExpressionParser from "cron-parser";
import { eventFilterTermsMatch } from "../../src/event-dispatch";

type PublicMethodsOf<T> = {
	[K in keyof T as T[K] extends Function ? K : never]: T[K];
};

type IDatabaseClient = PublicMethodsOf<DatabaseClient>;

const EVENT_DISPATCH_QUEUE = "pgconductor.internal";
const EVENT_DISPATCH_TASK = "pgconductor.event-dispatch";

interface StoredExecution {
	id: string;
	task_key: string;
	queue: string;
	group: string | null;
	payload: Payload;
	state: "pending" | "running" | "completed" | "failed";
	run_at: Date;
	attempts: number;
	max_attempts: number;
	last_error: string | null;
	result: Payload | null;
	cancelled: boolean;
	waiting_on_execution_id: string | null;
	waiting_step_key: string | null;
	waiting_timeout_at: Date | null;
	dedupe_key: string | null;
	singleton_on: Date | null;
	cron_expression: string | null;
	priority: number;
	orchestrator_id: string | null;
	parent_execution_id: string | null;
	parent_step_key: string | null;
	created_at: Date;
	updated_at: Date;
	failed_at: Date | null;
	subscription_id: string | null;
	dead_letter_source_execution_id: string | null;
	dead_letter_source_queue: string | null;
	dead_letter_source_task_key: string | null;
	dead_letter_error: string | null;
	dead_letter_attempts: number | null;
	dead_letter_failed_at: Date | null;
	trace_context: Execution["trace_context"];
}

interface StoredStep {
	execution_id: string;
	step_key: string;
	result: Payload;
	created_at: Date;
}

interface StoredTask {
	key: string;
	queue: string;
	max_attempts: number;
	remove_on_complete_days: number | null;
	remove_on_fail_days: number | null;
	window_start: string | null;
	window_end: string | null;
	concurrency: number | null;
	group_concurrency: number | null;
	dead_letter_queue: string | null;
	dead_letter_task_key: string | null;
}

interface StoredCronSchedule {
	task_key: string;
	queue: string;
	schedule_name: string;
	cron_expression: string;
	last_execution_id: string | null;
}

interface StoredOrchestrator {
	id: string;
	version: string;
	migration_number: number;
	last_heartbeat: Date;
}

interface StoredEventSubscription {
	id: string;
	task_key: string;
	queue: string;
	event_key: string;
	payload_fields: string[] | null;
	required_field_count: number;
	terms: EventFilterTerm[];
	kind: "task_trigger" | "execution_wait";
	execution_id: string | null;
	step_key: string | null;
	expires_at: Date | null;
	created_at: Date;
}

interface SignalData {
	signal_type: string;
}

/**
 * In-memory database client that simulates all PostgreSQL behavior without requiring a database.
 * Useful for fast, deterministic unit tests with full control over time and state.
 */
export class InMemoryDatabaseClient implements IDatabaseClient {
	private executions = new Map<string, StoredExecution>();
	private steps = new Map<string, Map<string, StoredStep>>();
	private tasks = new Map<string, StoredTask>();
	private cronSchedules = new Map<string, StoredCronSchedule>();
	private orchestrators = new Map<string, StoredOrchestrator>();
	private eventSubscriptions = new Map<string, StoredEventSubscription>();
	private currentTime: Date;
	private migrationNumber = -1;
	private idCounter = 0;

	constructor(initialTime: Date = new Date()) {
		this.currentTime = new Date(initialTime);
		this.registerInternalEventTask();
	}

	// ============================================================================
	// Time Control
	// ============================================================================

	async setFakeTime({ date }: SetFakeTimeArgs): Promise<void> {
		this.currentTime = new Date(date);
	}

	async clearFakeTime(): Promise<void> {
		this.currentTime = new Date();
	}

	advanceTime(ms: number): void {
		this.currentTime = new Date(this.currentTime.getTime() + ms);
	}

	// Internal synchronous method for time management
	private getInternalTime(): Date {
		return new Date(this.currentTime);
	}

	private createExecution(spec: ExecutionSpec, now: Date, singletonOn: Date | null = null): string {
		const task = this.tasks.get(this.taskId(spec.task_key, spec.queue));
		const id = this.generateId();
		this.executions.set(id, {
			id,
			task_key: spec.task_key,
			queue: spec.queue,
			group: spec.group || null,
			payload: spec.payload || {},
			state: "pending",
			run_at: spec.run_at || now,
			attempts: 0,
			max_attempts: task?.max_attempts || 3,
			last_error: null,
			result: null,
			cancelled: false,
			waiting_on_execution_id: null,
			waiting_step_key: null,
			waiting_timeout_at: null,
			dedupe_key: spec.dedupe_key || null,
			singleton_on: singletonOn,
			cron_expression: spec.cron_expression || null,
			priority: spec.priority || 0,
			orchestrator_id: null,
			parent_execution_id: spec.parent_execution_id || null,
			parent_step_key: spec.parent_step_key || null,
			created_at: now,
			updated_at: now,
			failed_at: null,
			subscription_id: null,
			dead_letter_source_execution_id: null,
			dead_letter_source_queue: null,
			dead_letter_source_task_key: null,
			dead_letter_error: null,
			dead_letter_attempts: null,
			dead_letter_failed_at: null,
			trace_context: spec.trace_context || null,
		});
		return id;
	}

	// Public synchronous method for test assertions
	getCurrentTimeSync(): Date {
		return this.getInternalTime();
	}

	// Async methods matching DatabaseClient interface
	async getCurrentTime(): Promise<Date> {
		return this.getInternalTime();
	}

	async getDatabaseTime(): Promise<Date> {
		return this.getInternalTime();
	}

	// ============================================================================
	// Orchestrator Management
	// ============================================================================

	async close(): Promise<void> {
		// No-op for in-memory client
	}

	async orchestratorHeartbeat(
		args: OrchestratorHeartbeatArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<
		{
			signal_type: string | null;
			signal_execution_id: string | null;
			signal_payload: Record<string, any> | null;
		}[]
	> {
		const orchestrator: StoredOrchestrator = {
			id: args.orchestratorId,
			version: args.version,
			migration_number: args.migrationNumber,
			last_heartbeat: this.getInternalTime(),
		};
		this.orchestrators.set(args.orchestratorId, orchestrator);
		return [];
	}

	async recoverStaleOrchestrators(
		_args: RecoverStaleOrchestratorsArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<void> {
		const staleThreshold = new Date(this.getInternalTime().getTime() - 30000);

		for (const orchestrator of this.orchestrators.values()) {
			if (orchestrator.last_heartbeat < staleThreshold) {
				// Release executions claimed by stale orchestrator
				for (const exec of this.executions.values()) {
					if (exec.orchestrator_id === orchestrator.id && exec.state === "running") {
						exec.state = exec.cancelled ? "failed" : "pending";
						exec.last_error = exec.cancelled
							? exec.last_error || "Task was cancelled"
							: exec.last_error;
						exec.orchestrator_id = null;
					}
				}
				this.orchestrators.delete(orchestrator.id);
			}
		}
	}

	async sweepOrchestrators(
		_args: SweepOrchestratorsArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<void> {
		const staleThreshold = new Date(this.getInternalTime().getTime() - 60000);

		for (const [id, orchestrator] of this.orchestrators.entries()) {
			if (orchestrator.last_heartbeat < staleThreshold) {
				this.orchestrators.delete(id);
			}
		}
	}

	async countActiveOrchestratorsBelow(
		args: CountActiveOrchestratorsBelowArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<number> {
		let count = 0;
		for (const orchestrator of this.orchestrators.values()) {
			if (orchestrator.migration_number < args.version) {
				count++;
			}
		}
		return count;
	}

	async orchestratorShutdown(
		args: OrchestratorShutdownArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<void> {
		this.orchestrators.delete(args.orchestratorId);
		for (const exec of this.executions.values()) {
			if (exec.orchestrator_id !== args.orchestratorId || exec.state !== "running") continue;
			exec.state = exec.cancelled ? "failed" : "pending";
			exec.last_error = exec.cancelled ? exec.last_error || "Task was cancelled" : exec.last_error;
			exec.orchestrator_id = null;
		}
	}

	// ============================================================================
	// Schema Management
	// ============================================================================

	async getInstalledMigrationNumber(_opts?: { signal?: AbortSignal }): Promise<number> {
		return this.migrationNumber;
	}

	async applyMigration(
		migration: Migration,
		_opts?: { signal?: AbortSignal },
	): Promise<"applied" | "busy"> {
		if (this.migrationNumber >= migration.version) {
			return "busy";
		}
		this.migrationNumber = migration.version;
		return "applied";
	}

	// ============================================================================
	// Worker Registration
	// ============================================================================

	async registerWorker(args: RegisterWorkerArgs, _opts?: { signal?: AbortSignal }): Promise<void> {
		// Register tasks
		for (const taskSpec of args.taskSpecs) {
			const task: StoredTask = {
				key: taskSpec.key,
				queue: taskSpec.queue || args.queueName,
				max_attempts: taskSpec.maxAttempts || 3,
				remove_on_complete_days: taskSpec.removeOnCompleteDays ?? null,
				remove_on_fail_days: taskSpec.removeOnFailDays ?? null,
				window_start: taskSpec.window?.[0] || null,
				window_end: taskSpec.window?.[1] || null,
				concurrency: taskSpec.concurrency || null,
				group_concurrency: taskSpec.groupConcurrency || null,
				dead_letter_queue: taskSpec.deadLetterQueue || null,
				dead_letter_task_key: taskSpec.deadLetterTaskKey || null,
			};
			this.tasks.set(this.taskId(taskSpec.key, task.queue), task);
		}

		// Registration is authoritative for this queue, just like PostgreSQL.
		for (const id of [...this.eventSubscriptions.keys()]) {
			const subscription = this.eventSubscriptions.get(id);
			if (subscription?.queue === args.queueName && subscription.kind === "task_trigger") {
				this.eventSubscriptions.delete(id);
			}
		}
		for (const spec of args.eventSubscriptions || []) {
			const id = this.generateId();
			this.eventSubscriptions.set(id, {
				id,
				task_key: spec.task_key,
				queue: args.queueName,
				event_key: spec.event_key,
				payload_fields: spec.payload_fields,
				required_field_count: spec.required_field_count,
				terms: structuredClone(spec.terms),
				kind: "task_trigger",
				execution_id: null,
				step_key: null,
				expires_at: null,
				created_at: this.getInternalTime(),
			});
		}

		// Register cron schedules (ExecutionSpec[])
		for (const cronSpec of args.cronSchedules || []) {
			if (cronSpec.cron_expression) {
				const key = `${cronSpec.task_key}:${cronSpec.cron_expression}`;
				this.cronSchedules.set(key, {
					task_key: cronSpec.task_key,
					queue: cronSpec.queue,
					schedule_name: cronSpec.cron_expression,
					cron_expression: cronSpec.cron_expression,
					last_execution_id: null,
				});
			}
		}
	}

	// ============================================================================
	// Execution Management
	// ============================================================================

	async getExecutions(
		args: GetExecutionsArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<Execution[]> {
		const results: Execution[] = [];
		const now = this.getInternalTime();
		const filterTaskKeys = new Set(args.filterTaskKeys || []);
		const concurrencyCount = new Map<string, number>();

		// Count running executions per task for concurrency limits.
		for (const exec of this.executions.values()) {
			const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
			if (exec.state === "running" && task?.concurrency != null) {
				concurrencyCount.set(
					this.taskId(exec.task_key, exec.queue),
					(concurrencyCount.get(this.taskId(exec.task_key, exec.queue)) || 0) + 1,
				);
			}
		}

		// Find eligible executions
		for (const exec of Array.from(this.executions.values()).sort(
			(a, b) =>
				a.priority - b.priority ||
				a.run_at.getTime() - b.run_at.getTime() ||
				a.created_at.getTime() - b.created_at.getTime() ||
				a.id.localeCompare(b.id),
		)) {
			// Skip if wrong queue
			if (exec.queue !== args.queueName) continue;

			// Skip if filtered out
			if (filterTaskKeys.has(exec.task_key)) continue;

			// Skip if not pending
			if (exec.state !== "pending") continue;

			// Skip if not ready to run
			if (exec.run_at > now) continue;

			// Skip if waiting on another execution
			if (exec.waiting_on_execution_id) {
				const parent = this.executions.get(exec.waiting_on_execution_id);
				if (parent && parent.state !== "completed") continue;
			}

			const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
			// Check concurrency limit.
			if (task?.concurrency != null) {
				const current = concurrencyCount.get(this.taskId(exec.task_key, exec.queue)) || 0;
				if (current >= task.concurrency) continue;
			}
			if (task?.group_concurrency && exec.group) {
				const activeGroup = Array.from(this.executions.values()).filter(
					(other) =>
						other.queue === exec.queue &&
						other.task_key === exec.task_key &&
						other.group === exec.group &&
						other.state === "running",
				).length;
				if (activeGroup >= task.group_concurrency) continue;
			}

			// Claim execution.
			exec.state = "running";
			exec.attempts += 1;
			exec.orchestrator_id = args.orchestratorId;

			// Update concurrency count
			if (task?.concurrency != null) {
				concurrencyCount.set(
					this.taskId(exec.task_key, exec.queue),
					(concurrencyCount.get(this.taskId(exec.task_key, exec.queue)) || 0) + 1,
				);
			}

			results.push({
				id: exec.id,
				task_key: exec.task_key,
				queue: exec.queue,
				payload: exec.payload,
				waiting_on_execution_id: exec.waiting_on_execution_id,
				waiting_step_key: exec.waiting_step_key,
				cancelled: exec.cancelled,
				last_error: exec.last_error,
				dedupe_key: exec.dedupe_key || undefined,
				cron_expression: exec.cron_expression || undefined,
				group: exec.group,
				subscription_id: exec.subscription_id,
				dead_letter_source_execution_id: exec.dead_letter_source_execution_id,
				dead_letter_source_queue: exec.dead_letter_source_queue,
				dead_letter_source_task_key: exec.dead_letter_source_task_key,
				dead_letter_error: exec.dead_letter_error,
				dead_letter_attempts: exec.dead_letter_attempts,
				dead_letter_failed_at: exec.dead_letter_failed_at,
				trace_context: exec.trace_context,
				locked_by: exec.orchestrator_id || "",
			});

			if (results.length >= args.batchSize) break;
		}

		return results;
	}

	private deleteExecutionWaits(executionId: string): void {
		for (const [id, subscription] of this.eventSubscriptions) {
			if (subscription.kind === "execution_wait" && subscription.execution_id === executionId) {
				this.eventSubscriptions.delete(id);
			}
		}
	}

	async returnExecutions(
		resultsOrGrouped:
			| ExecutionResult[]
			| import("../../src/database-client").GroupedExecutionResults,
		_opts?: { signal?: AbortSignal },
	): Promise<void> {
		// Handle both old array format (for testing) and new grouped format
		const results: ExecutionResult[] = Array.isArray(resultsOrGrouped)
			? resultsOrGrouped
			: [
					...resultsOrGrouped.completed,
					...resultsOrGrouped.failed,
					...resultsOrGrouped.released,
					...resultsOrGrouped.invokeChild,
					// ...resultsOrGrouped.waitForCustomEvent,
					// ...resultsOrGrouped.waitForDbEvent,
				];
		const now = this.getInternalTime();

		for (const result of results) {
			const exec = this.executions.get(result.execution_id);
			if (
				!exec ||
				!this.ownsClaim({
					executionId: result.execution_id,
					queue: result.queue,
					orchestratorId: result.orchestrator_id,
				})
			)
				continue;

			switch (result.status) {
				case "completed": {
					exec.state = "completed";
					exec.result = result.result || null;
					exec.orchestrator_id = null;
					this.deleteExecutionWaits(exec.id);

					// Event deliveries retain lineage without workflow-child behavior.
					if (exec.parent_execution_id && exec.subscription_id === null) {
						const parent = this.executions.get(exec.parent_execution_id);
						if (parent && parent.waiting_on_execution_id === exec.id) {
							parent.waiting_on_execution_id = null;
							parent.waiting_step_key = null;
							parent.waiting_timeout_at = null;
							parent.state = "pending";
							parent.run_at = now;
						}
					}

					// Schedule next cron execution if needed
					if (exec.cron_expression) {
						await this.scheduleNextCronExecution(exec);
					}

					// Zero-day retention removes immediately; positive retention is swept later.
					const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
					if (task?.remove_on_complete_days === 0) {
						this.executions.delete(exec.id);
						this.steps.delete(exec.id);
					}
					break;
				}

				case "failed": {
					exec.last_error = result.error;
					exec.orchestrator_id = null;

					const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
					const maxAttempts = task?.max_attempts || 3;

					if (exec.attempts >= maxAttempts) {
						// Permanently failed
						exec.state = "failed";
						exec.failed_at = now;
						this.deleteExecutionWaits(exec.id);
						if (!exec.cancelled) this.deliverToDeadLetterQueue(exec, task, result.error, now);

						// Fail a workflow parent only. Event delivery lineage is independent.
						if (exec.parent_execution_id && exec.subscription_id === null) {
							const parent = this.executions.get(exec.parent_execution_id);
							if (parent && parent.waiting_on_execution_id === exec.id) {
								parent.state = "failed";
								parent.last_error = `Child execution failed: ${result.error}`;
								parent.waiting_on_execution_id = null;
								parent.waiting_step_key = null;
								this.deleteExecutionWaits(parent.id);
								const parentTask = this.tasks.get(this.taskId(parent.task_key, parent.queue));
								if (!exec.cancelled) {
									this.deliverToDeadLetterQueue(parent, parentTask, parent.last_error, now);
								}
								if (parentTask?.remove_on_fail_days === 0) {
									this.executions.delete(parent.id);
									this.steps.delete(parent.id);
								}
							}
						}

						// Zero-day retention removes immediately; positive retention is swept later.
						if (task?.remove_on_fail_days === 0) {
							this.executions.delete(exec.id);
							this.steps.delete(exec.id);
						}
					} else {
						// Retry with backoff
						exec.state = "pending";
						const backoffSeconds = this.calculateBackoff(exec.attempts);
						exec.run_at = new Date(now.getTime() + backoffSeconds * 1000);

						// Don't reschedule cron on retry
						if (exec.cron_expression) {
							exec.cron_expression = null;
						}
					}
					break;
				}

				case "released": {
					exec.state = "pending";
					exec.orchestrator_id = null;
					exec.attempts = Math.max(exec.attempts - 1, 0);

					if (result.reschedule_in_ms === "infinity") {
						exec.run_at = new Date(8640000000000000); // Max date
					} else if (result.reschedule_in_ms) {
						exec.run_at = new Date(now.getTime() + result.reschedule_in_ms);
					} else {
						exec.run_at = now;
					}
					break;
				}

				case "permanently_failed": {
					exec.state = "failed";
					exec.failed_at = now;
					exec.last_error = result.error;
					this.deleteExecutionWaits(exec.id);

					const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
					if (!exec.cancelled) this.deliverToDeadLetterQueue(exec, task, result.error, now);
					exec.orchestrator_id = null;

					// Fail a workflow parent only. Event delivery lineage is independent.
					if (exec.parent_execution_id && exec.subscription_id === null) {
						const parent = this.executions.get(exec.parent_execution_id);
						if (parent && parent.waiting_on_execution_id === exec.id) {
							parent.state = "failed";
							parent.last_error = `Child execution failed: ${result.error}`;
							parent.waiting_on_execution_id = null;
							parent.waiting_step_key = null;
							parent.waiting_timeout_at = null;
							this.deleteExecutionWaits(parent.id);
							const parentTask = this.tasks.get(this.taskId(parent.task_key, parent.queue));
							if (!exec.cancelled) {
								this.deliverToDeadLetterQueue(parent, parentTask, parent.last_error, now);
							}
							if (parentTask?.remove_on_fail_days === 0) {
								this.executions.delete(parent.id);
								this.steps.delete(parent.id);
							}
						}
					}

					if (task?.remove_on_fail_days === 0) {
						this.executions.delete(exec.id);
						this.steps.delete(exec.id);
					}
					break;
				}

				case "invoke_child": {
					// Create child execution
					const childId = await this.invoke({
						task_key: result.child_task_name,
						queue: result.child_task_queue,
						payload: result.child_payload || {},
						group: result.group,
						parent_execution_id: exec.id,
						parent_step_key: result.step_key,
					});

					// Set parent to wait
					exec.state = "pending";
					exec.waiting_on_execution_id = childId;
					exec.waiting_step_key = result.step_key;
					exec.orchestrator_id = null;

					if (result.timeout_ms === "infinity") {
						exec.waiting_timeout_at = new Date(8640000000000000);
					} else {
						exec.waiting_timeout_at = new Date(now.getTime() + result.timeout_ms);
					}
					break;
				}

				// case "wait_for_custom_event": {
				// 	// Create subscription
				// 	const subscriptionId = this.generateId();
				// 	this.eventSubscriptions.set(subscriptionId, {
				// 		id: subscriptionId,
				// 		execution_id: exec.id,
				// 		step_key: result.step_key,
				// 		source: "event",
				// 		event_key: result.event_key,
				// 		timeout_at:
				// 			result.timeout_ms === "infinity"
				// 				? new Date(8640000000000000)
				// 				: new Date(now.getTime() + result.timeout_ms),
				// 	});

				// 	exec.state = "pending";
				// 	exec.run_at = new Date(8640000000000000); // Wait indefinitely
				// 	exec.orchestrator_id = null;
				// 	break;
				// }
			}

			exec.updated_at = now;
		}
	}

	async removeExecutions(
		args: RemoveExecutionsArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<boolean> {
		const now = this.getInternalTime();
		const candidates = Array.from(this.executions.values())
			.filter((exec) => {
				if (
					exec.queue !== args.queueName ||
					(exec.state !== "completed" && exec.state !== "failed")
				) {
					return false;
				}
				const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
				if (!task) return false;
				const retentionDays =
					exec.state === "completed" ? task.remove_on_complete_days : task.remove_on_fail_days;
				if (retentionDays == null || retentionDays <= 0) return false;
				const terminalAt = exec.state === "completed" ? exec.updated_at : exec.failed_at;
				return (
					terminalAt != null && terminalAt.getTime() < now.getTime() - retentionDays * 86400000
				);
			})
			.slice(0, args.batchSize);

		for (const exec of candidates) {
			this.executions.delete(exec.id);
			this.steps.delete(exec.id);
		}
		return candidates.length >= args.batchSize;
	}

	async invoke(spec: ExecutionSpec, _opts?: { signal?: AbortSignal }): Promise<string | null> {
		const now = this.getInternalTime();

		// Validation
		if (spec.throttle && spec.debounce) {
			throw new Error("Cannot use both throttle and debounce");
		}

		// Calculate singleton_on if throttle/debounce is specified
		let singletonOn: Date | null = null;
		let dedupeSeconds: number | null = null;
		let dedupeNextSlot = false;

		if (spec.throttle) {
			dedupeSeconds = spec.throttle.seconds;
			dedupeNextSlot = false;
		} else if (spec.debounce) {
			dedupeSeconds = spec.debounce.seconds;
			dedupeNextSlot = true;
		}

		if (dedupeSeconds) {
			// Calculate time slot (pg-boss formula)
			const epochSeconds = Math.floor(this.getInternalTime().getTime() / 1000);
			const slotNumber = Math.floor(epochSeconds / dedupeSeconds);
			singletonOn = new Date(slotNumber * dedupeSeconds * 1000);
		}

		const createExecution = (singletonOnValue: Date | null): string =>
			this.createExecution(spec, now, singletonOnValue);

		// Throttle/debounce logic
		if (singletonOn) {
			const normalizedDedupeKey = spec.dedupe_key || "";

			if (dedupeNextSlot) {
				// Debounce: ALWAYS create in next slot (not current slot)
				const nextSingletonOn = new Date(singletonOn.getTime() + dedupeSeconds! * 1000);

				// Delete existing job in next slot
				for (const [execId, exec] of this.executions.entries()) {
					if (
						exec.task_key === spec.task_key &&
						exec.queue === spec.queue &&
						(exec.dedupe_key || "") === normalizedDedupeKey &&
						exec.singleton_on &&
						exec.singleton_on.getTime() === nextSingletonOn.getTime() &&
						exec.state !== "completed" &&
						exec.state !== "failed" &&
						!exec.cancelled
					) {
						this.executions.delete(execId);
						this.steps.delete(execId);
					}
				}

				// Insert into next slot
				return createExecution(nextSingletonOn);
			} else {
				// Throttle: try current slot
				// Check singleton constraint on (task_key, singleton_on, COALESCE(dedupe_key, ''))
				const existingInCurrentSlot = Array.from(this.executions.values()).find(
					(exec) =>
						exec.task_key === spec.task_key &&
						exec.queue === spec.queue &&
						(exec.dedupe_key || "") === normalizedDedupeKey &&
						exec.singleton_on &&
						exec.singleton_on.getTime() === singletonOn.getTime() &&
						exec.state !== "completed" &&
						exec.state !== "failed" &&
						!exec.cancelled,
				);

				if (existingInCurrentSlot) {
					// Current slot is occupied - reject
					return null;
				} else {
					// Current slot is free - create execution
					return createExecution(singletonOn);
				}
			}
		}

		// Standard invocation (no throttle/debounce)
		// Handle dedupe_key logic
		if (spec.dedupe_key) {
			for (const exec of this.executions.values()) {
				if (
					exec.dedupe_key === spec.dedupe_key &&
					exec.task_key === spec.task_key &&
					exec.queue === spec.queue &&
					exec.state !== "failed" &&
					exec.state !== "completed"
				) {
					// Found existing execution with dedupe_key
					if (exec.state === "running") {
						// Locked execution - mark as superseded and clear dedupe_key
						exec.state = "failed";
						exec.last_error = "superseded by reinvoke";
						exec.dedupe_key = null;
						exec.orchestrator_id = null;
						// Will create new execution below
					} else {
						// Unlocked execution - update it with new values (replace behavior)
						exec.payload = spec.payload || {};
						exec.run_at = spec.run_at || now;
						exec.priority = spec.priority || 0;
						exec.cron_expression = spec.cron_expression || null;
						exec.updated_at = now;
						return exec.id;
					}
				}
			}
		}

		// Create new execution
		return createExecution(null);
	}

	async invokeBatch(specs: ExecutionSpec[], _opts?: { signal?: AbortSignal }): Promise<string[]> {
		// Validate: batch invoke doesn't support debounce
		for (const spec of specs) {
			if (spec.debounce) {
				throw new Error("Batch invoke only supports throttle, not debounce");
			}
		}

		const now = this.getInternalTime();
		const ids: string[] = [];

		// Process each spec with batch semantics (ON CONFLICT DO UPDATE)
		for (const spec of specs) {
			// Validation
			if (spec.throttle && spec.debounce) {
				throw new Error("Cannot use both throttle and debounce");
			}

			// Calculate singleton_on if throttle is specified
			let singletonOn: Date | null = null;
			if (spec.throttle) {
				const epochSeconds = Math.floor(now.getTime() / 1000);
				const slotNumber = Math.floor(epochSeconds / spec.throttle.seconds);
				singletonOn = new Date(slotNumber * spec.throttle.seconds * 1000);
			}

			// Check for existing execution with same dedupe_key (standard dedupe constraint)
			// This mimics ON CONFLICT (task_key, dedupe_key, queue) DO UPDATE
			let foundExisting = false;
			if (spec.dedupe_key) {
				for (const exec of this.executions.values()) {
					if (
						exec.dedupe_key === spec.dedupe_key &&
						exec.task_key === spec.task_key &&
						exec.queue === spec.queue &&
						exec.state !== "failed" &&
						exec.state !== "completed"
					) {
						// Found existing - update it (ON CONFLICT DO UPDATE)
						exec.payload = spec.payload || {};
						exec.run_at = spec.run_at || now;
						exec.priority = spec.priority || 0;
						exec.singleton_on = singletonOn;
						exec.cron_expression = spec.cron_expression || null;
						exec.updated_at = now;
						ids.push(exec.id);
						foundExisting = true;
						break;
					}
				}
			}

			if (!foundExisting) {
				// Create new execution
				const id = await this.invoke(spec);
				if (id) {
					ids.push(id);
				}
			}
		}

		return ids;
	}

	async cancelExecution(
		executionId: string,
		options: { reason?: string },
		_opts?: { signal?: AbortSignal },
	): Promise<boolean> {
		const exec = this.executions.get(executionId);
		if (!exec) return false;

		exec.cancelled = true;
		exec.last_error = options.reason || "Execution was cancelled";

		if (exec.state === "running") {
			exec.cancelled = true;
		}
		this.deleteExecutionWaits(executionId);

		return true;
	}

	// ============================================================================
	// Cron Scheduling
	// ============================================================================

	async scheduleCronExecution(
		args: ScheduleCronExecutionArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<string> {
		const spec = args.spec;
		const key = `${spec.task_key}:${args.scheduleName}`;

		// Store or update schedule
		this.cronSchedules.set(key, {
			task_key: spec.task_key,
			queue: spec.queue,
			schedule_name: args.scheduleName,
			cron_expression: spec.cron_expression || "",
			last_execution_id: null,
		});

		// Create first execution at next cron time (matches worker behavior)
		const nextRun = this.calculateNextCronRun(spec.cron_expression || "");
		const dedupeKey = `cron::${args.scheduleName}::${spec.task_key}::${spec.queue}`;

		const id = await this.invoke({
			...spec,
			run_at: nextRun,
			dedupe_key: dedupeKey,
		});

		if (!id) {
			throw new Error("Cron execution was throttled unexpectedly");
		}

		return id;
	}

	async unscheduleCronExecution(
		args: UnscheduleCronExecutionArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<void> {
		const key = `${args.taskKey}:${args.scheduleName}`;
		const schedule = this.cronSchedules.get(key);
		if (!schedule) return;

		// Remove schedule
		this.cronSchedules.delete(key);

		// Cancel pending cron executions with this schedule
		const dedupeKey = `cron::${args.scheduleName}::${args.taskKey}::${args.queue || "default"}`;
		for (const exec of this.executions.values()) {
			if (exec.dedupe_key === dedupeKey && exec.state === "pending") {
				await this.cancelExecution(exec.id, {
					reason: "Cron schedule was unscheduled",
				});
			}
		}
	}

	private async scheduleNextCronExecution(exec: StoredExecution): Promise<void> {
		if (!exec.cron_expression) return;

		// Find the schedule
		let schedule: StoredCronSchedule | undefined;
		for (const s of this.cronSchedules.values()) {
			if (s.task_key === exec.task_key && s.queue === exec.queue) {
				schedule = s;
				break;
			}
		}

		if (!schedule) return; // Schedule was removed

		// Calculate next run
		const nextRun = this.calculateNextCronRun(exec.cron_expression);
		const dedupeKey = `cron::${schedule.schedule_name}::${exec.task_key}::${exec.queue}`;

		// Create next execution
		await this.invoke({
			task_key: exec.task_key,
			queue: exec.queue,
			payload: {},
			run_at: nextRun,
			dedupe_key: dedupeKey,
			cron_expression: exec.cron_expression,
			group: exec.group,
		});
	}

	private calculateNextCronRun(cronExpression: string): Date {
		try {
			const interval = CronExpressionParser.parse(cronExpression, {
				currentDate: this.getInternalTime(),
			});
			return interval.next().toDate();
		} catch {
			// If parsing fails, default to 1 minute from now
			return new Date(this.getInternalTime().getTime() + 60000);
		}
	}

	// ============================================================================
	// Steps
	// ============================================================================

	async loadStep(
		args: LoadStepArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<Payload | null | undefined> {
		if (!this.ownsClaim(args)) return undefined;
		const execSteps = this.steps.get(args.executionId);
		if (!execSteps) return undefined;
		const step = execSteps.get(args.key);
		return step ? step.result : undefined;
	}

	async saveStep(args: SaveStepArgs, _opts?: { signal?: AbortSignal }): Promise<void> {
		if (!this.ownsClaim(args)) return;
		let execSteps = this.steps.get(args.executionId);
		if (!execSteps) {
			execSteps = new Map();
			this.steps.set(args.executionId, execSteps);
		}
		execSteps.set(args.key, {
			execution_id: args.executionId,
			step_key: args.key,
			result: args.result || {},
			created_at: this.getInternalTime(),
		});
	}

	async clearWaitingState(
		args: ClearWaitingStateArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<void> {
		const exec = this.executions.get(args.executionId);
		if (!exec || !this.ownsClaim(args)) return;

		exec.waiting_on_execution_id = null;
		exec.waiting_step_key = null;
		exec.waiting_timeout_at = null;
	}

	async registerEventWait(
		args: RegisterEventWaitArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<{ timedOut: boolean; timeoutMs: number | null }> {
		const execution = this.executions.get(args.executionId);
		if (
			!execution ||
			execution.queue !== args.queue ||
			execution.task_key !== args.taskKey ||
			!this.ownsClaim(args) ||
			execution.cancelled ||
			execution.state !== "running"
		) {
			return { timedOut: false, timeoutMs: null };
		}

		const existing = Array.from(this.eventSubscriptions.values()).find(
			(subscription) =>
				subscription.kind === "execution_wait" &&
				subscription.execution_id === args.executionId &&
				subscription.step_key === args.stepKey,
		);
		const now = this.getInternalTime();
		if (existing?.expires_at && existing.expires_at <= now) {
			this.eventSubscriptions.delete(existing.id);
			let executionSteps = this.steps.get(args.executionId);
			if (!executionSteps) {
				executionSteps = new Map();
				this.steps.set(args.executionId, executionSteps);
			}
			executionSteps.set(args.stepKey, {
				execution_id: args.executionId,
				step_key: args.stepKey,
				result: { status: "timed_out" },
				created_at: now,
			});
			return { timedOut: true, timeoutMs: 0 };
		}

		let expiresAt = existing?.expires_at ?? null;
		if (!existing) {
			const id = this.generateId();
			expiresAt = args.timeoutMs === null ? null : new Date(now.getTime() + args.timeoutMs);
			this.eventSubscriptions.set(id, {
				id,
				task_key: args.taskKey,
				queue: args.queue,
				event_key: args.eventKey,
				payload_fields: null,
				required_field_count: args.requiredFieldCount,
				terms: structuredClone(args.terms),
				kind: "execution_wait",
				execution_id: args.executionId,
				step_key: args.stepKey,
				expires_at: expiresAt,
				created_at: now,
			});
		}

		return {
			timedOut: false,
			timeoutMs: expiresAt ? Math.max(0, expiresAt.getTime() - now.getTime()) : null,
		};
	}

	// ============================================================================
	// Events
	// ============================================================================

	async invokeChild(): Promise<string> {
		return this.generateId();
	}

	async emitEvent(args: EmitEventArgs): Promise<string> {
		if (
			typeof args.eventKey !== "string" ||
			args.eventKey.trim().length === 0 ||
			new TextEncoder().encode(args.eventKey).length > 255
		) {
			throw new Error("Event name must contain between 1 and 255 UTF-8 bytes");
		}
		const payload = args.payload === undefined ? {} : args.payload;
		if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
			throw new Error("Event payload must be a JSON object");
		}
		const id = this.createExecution(
			{
				task_key: EVENT_DISPATCH_TASK,
				queue: EVENT_DISPATCH_QUEUE,
				payload: { eventKey: args.eventKey, payload },
			},
			this.getInternalTime(),
		);
		return id;
	}

	async dispatchCustomEvents(args: {
		eventIds: string[];
		orchestratorId: string;
	}): Promise<string[]> {
		const dispatched: string[] = [];
		const insertedIds: string[] = [];
		try {
			for (const eventId of args.eventIds) {
				const source = this.executions.get(eventId);
				if (
					!source ||
					source.queue !== EVENT_DISPATCH_QUEUE ||
					source.task_key !== EVENT_DISPATCH_TASK ||
					source.orchestrator_id !== args.orchestratorId ||
					source.state === "completed" ||
					source.state === "failed" ||
					source.cancelled
				) {
					continue;
				}
				const eventKey = String(source.payload.eventKey);
				const payload = source.payload.payload as Payload;
				const destinations: Array<{ spec: ExecutionSpec; subscriptionId: string }> = [];
				for (const subscription of this.eventSubscriptions.values()) {
					if (
						subscription.kind !== "task_trigger" ||
						subscription.event_key !== eventKey ||
						!this.tasks.has(this.taskId(subscription.task_key, subscription.queue))
					) {
						continue;
					}
					if (
						!eventFilterTermsMatch(payload, subscription.required_field_count, subscription.terms)
					)
						continue;
					if (
						Array.from(this.executions.values()).some(
							(execution) =>
								execution.parent_execution_id === eventId &&
								execution.subscription_id === subscription.id &&
								execution.queue === subscription.queue,
						)
					)
						continue;

					const destinationPayload = subscription.payload_fields
						? (Object.fromEntries(
								subscription.payload_fields
									.filter((field) => Object.prototype.hasOwnProperty.call(payload, field))
									.map((field) => [field, payload[field]]),
							) as Payload)
						: structuredClone(payload);
					destinations.push({
						spec: {
							task_key: subscription.task_key,
							queue: subscription.queue,
							payload: { event: eventKey, payload: destinationPayload },
							parent_execution_id: eventId,
						},
						subscriptionId: subscription.id,
					});
				}

				const now = this.getInternalTime();
				for (const destination of destinations) {
					const destinationId = this.createExecution(destination.spec, now);
					insertedIds.push(destinationId);
					this.executions.get(destinationId)!.subscription_id = destination.subscriptionId;
				}

				for (const subscription of [...this.eventSubscriptions.values()]) {
					if (
						subscription.kind !== "execution_wait" ||
						subscription.event_key !== eventKey ||
						source.created_at <= subscription.created_at ||
						(subscription.expires_at !== null && source.created_at > subscription.expires_at) ||
						!eventFilterTermsMatch(payload, subscription.required_field_count, subscription.terms)
					) {
						continue;
					}
					const execution = subscription.execution_id
						? this.executions.get(subscription.execution_id)
						: undefined;
					if (
						!execution ||
						execution.state !== "pending" ||
						execution.orchestrator_id !== null ||
						execution.cancelled ||
						!subscription.step_key
					) {
						continue;
					}
					let executionSteps = this.steps.get(execution.id);
					if (!executionSteps) {
						executionSteps = new Map();
						this.steps.set(execution.id, executionSteps);
					}
					if (!executionSteps.has(subscription.step_key)) {
						executionSteps.set(subscription.step_key, {
							execution_id: execution.id,
							step_key: subscription.step_key,
							result: {
								status: "resolved",
								event: { name: eventKey, payload: structuredClone(payload) },
							},
							created_at: now,
						});
					}
					this.eventSubscriptions.delete(subscription.id);
					execution.waiting_step_key = null;
					execution.waiting_timeout_at = null;
					execution.run_at = now;
				}
				dispatched.push(eventId);
			}
			return dispatched;
		} catch (error) {
			for (const destinationId of insertedIds) this.executions.delete(destinationId);
			throw error;
		}
	}

	// ============================================================================
	private deliverToDeadLetterQueue(
		exec: StoredExecution,
		task: StoredTask | undefined,
		error: string,
		now: Date,
	): void {
		if (!task?.dead_letter_queue || exec.cancelled) return;
		const destinationTaskKey = task.dead_letter_task_key || exec.task_key;
		const duplicate = Array.from(this.executions.values()).some(
			(destination) =>
				destination.dead_letter_source_execution_id === exec.id &&
				destination.queue === task.dead_letter_queue &&
				destination.task_key === destinationTaskKey,
		);
		if (duplicate) return;
		const id = this.generateId();
		this.executions.set(id, {
			id,
			task_key: destinationTaskKey,
			queue: task.dead_letter_queue,
			group: exec.group,
			payload: structuredClone(exec.payload),
			state: "pending",
			run_at: now,
			attempts: 0,
			max_attempts: 3,
			last_error: null,
			result: null,
			cancelled: false,
			waiting_on_execution_id: null,
			waiting_step_key: null,
			waiting_timeout_at: null,
			dedupe_key: null,
			singleton_on: null,
			cron_expression: null,
			priority: 0,
			orchestrator_id: null,
			parent_execution_id: null,
			parent_step_key: null,
			created_at: now,
			updated_at: now,
			failed_at: null,
			subscription_id: null,
			dead_letter_source_execution_id: exec.id,
			dead_letter_source_queue: exec.queue,
			dead_letter_source_task_key: exec.task_key,
			dead_letter_error: error,
			dead_letter_attempts: exec.attempts,
			dead_letter_failed_at: now,
			trace_context: null,
		});
	}

	// Helpers
	// ============================================================================

	private registerInternalEventTask(): void {
		this.tasks.set(this.taskId(EVENT_DISPATCH_TASK, EVENT_DISPATCH_QUEUE), {
			key: EVENT_DISPATCH_TASK,
			queue: EVENT_DISPATCH_QUEUE,
			max_attempts: 3,
			remove_on_complete_days: 0,
			remove_on_fail_days: null,
			window_start: null,
			window_end: null,
			concurrency: null,
			group_concurrency: null,
			dead_letter_queue: null,
			dead_letter_task_key: null,
		});
	}

	private taskId(key: string, queue: string): string {
		return `${queue}\u0000${key}`;
	}

	private ownsClaim(args: { executionId: string; queue: string; orchestratorId: string }): boolean {
		const exec = this.executions.get(args.executionId);
		return Boolean(
			exec && exec.queue === args.queue && exec.orchestrator_id === args.orchestratorId,
		);
	}

	private generateId(): string {
		this.idCounter++;
		return `in-memory-${this.idCounter.toString().padStart(8, "0")}`;
	}

	private calculateBackoff(attempts: number): number {
		// Backoff schedule: 15s, 30s, 60s, 120s, etc.
		if (attempts === 0) return 0;
		if (attempts === 1) return 15;
		if (attempts === 2) return 30;
		return Math.min(60 * Math.pow(2, attempts - 3), 3600); // Cap at 1 hour
	}

	// ============================================================================
	// Test Helpers
	// ============================================================================

	getExecution(id: string): StoredExecution | undefined {
		return this.executions.get(id);
	}

	getAllExecutions(): StoredExecution[] {
		return Array.from(this.executions.values());
	}

	getPendingExecutions(): StoredExecution[] {
		return Array.from(this.executions.values()).filter((e) => e.state === "pending");
	}

	getRunningExecutions(): StoredExecution[] {
		return Array.from(this.executions.values()).filter((e) => e.state === "running");
	}

	getCompletedExecutions(): StoredExecution[] {
		return Array.from(this.executions.values()).filter((e) => e.state === "completed");
	}

	getFailedExecutions(): StoredExecution[] {
		return Array.from(this.executions.values()).filter((e) => e.state === "failed");
	}

	getCronSchedules(taskKey?: string): StoredCronSchedule[] {
		const schedules = Array.from(this.cronSchedules.values());
		return taskKey ? schedules.filter((s) => s.task_key === taskKey) : schedules;
	}

	getSteps(executionId: string): StoredStep[] {
		const execSteps = this.steps.get(executionId);
		return execSteps ? Array.from(execSteps.values()) : [];
	}

	getEventSubscriptions(): StoredEventSubscription[] {
		return Array.from(this.eventSubscriptions.values());
	}

	clear(): void {
		this.executions.clear();
		this.steps.clear();
		this.tasks.clear();
		this.registerInternalEventTask();
		this.cronSchedules.clear();
		this.orchestrators.clear();
		this.eventSubscriptions.clear();
		this.idCounter = 0;
	}

	/**
	 * Creates a DatabaseClient wrapper that uses this in-memory implementation.
	 * This allows passing the in-memory client to Conductor/Worker.
	 */
	createDatabaseClient(logger: Logger): DatabaseClient {
		// Create a proxy that intercepts all method calls and delegates to in-memory implementation
		const proxy = new Proxy({} as DatabaseClient, {
			get: (target, prop: string) => {
				// If method exists on in-memory client, use it
				if (prop in this && typeof (this as any)[prop] === "function") {
					return (this as any)[prop].bind(this);
				}
				// Otherwise return a no-op
				return () => Promise.resolve();
			},
		});
		return proxy;
	}
}

import type {
	CronRegistration,
	DatabaseClient,
	DeadLetterDelivery,
	Execution,
	ExecutionResult,
	ExecutionSpec,
	TaskSpec,
	Payload,
	JsonValue,
	SetFakeTimeArgs,
	SettlementOutcome,
	// EventSubscriptionSpec,
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
	RegisterEventWaitArgs,
	OrchestratorShutdownArgs,
	EmitEventArgs,
} from "../../src/query-builder";
import type { Migration } from "../../src/migration-store";
import type { Logger } from "../../src/lib/logger";
import CronExpressionParser from "cron-parser";

type PublicMethodsOf<T> = {
	[K in keyof T as T[K] extends Function ? K : never]: T[K];
};

type IDatabaseClient = PublicMethodsOf<DatabaseClient>;

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
	dead_letter_source_execution_id: string | null;
	dead_letter_source_queue: string | null;
	dead_letter_source_task_key: string | null;
	dead_letter_error: string | null;
	dead_letter_attempts: number | null;
	dead_letter_failed_at: Date | null;
	trace_context: Execution["trace_context"];
	trace_link_context: Execution["trace_link_context"];
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
	event_key: string | null;
	filter: Record<string, JsonValue[]> | null;
	execution_id?: string;
	step_key?: string;
	source?: "event" | "db";
	timeout_at?: Date | null;
	wait_after_event_position?: number;
}

interface StoredCustomEvent {
	id: string;
	event_key: string;
	payload: JsonValue;
	trace_context: Execution["trace_context"];
	created_at: Date;
	processed_at: Date | null;
	event_position: number;
}

function isPayload(value: JsonValue): value is Payload {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function jsonEquals(left: JsonValue | undefined, right: JsonValue | undefined): boolean {
	if (left === right) return true;
	if (left === null || right === null || typeof left !== typeof right) return false;
	if (Array.isArray(left) || Array.isArray(right)) {
		if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
		return left.every((value, index) => jsonEquals(value, right[index]));
	}
	if (typeof left === "object" && typeof right === "object") {
		const leftKeys = Object.keys(left);
		const rightKeys = Object.keys(right);
		return (
			leftKeys.length === rightKeys.length &&
			leftKeys.every((key) => Object.hasOwn(right, key) && jsonEquals(left[key], right[key]))
		);
	}
	return false;
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
	private customEvents = new Map<string, StoredCustomEvent>();
	private eventDeliveries = new Set<string>();
	private eventPartitions = new Set<string>();
	private currentTime: Date;
	private migrationNumber = -1;
	private idCounter = 0;
	private lastEventPosition = 0;

	constructor(initialTime: Date = new Date()) {
		this.currentTime = new Date(initialTime);
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

	// Public synchronous method for test assertions
	getCurrentTimeSync(): Date {
		return this.getInternalTime();
	}

	// Async method matching DatabaseClient interface
	async getCurrentTime(): Promise<Date> {
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

	async registerWorker(
		args: RegisterWorkerArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<CronRegistration[]> {
		// Register tasks
		for (const taskSpec of args.taskSpecs) {
			const task: StoredTask = {
				key: taskSpec.key,
				queue: taskSpec.queue || args.queueName,
				max_attempts: taskSpec.maxAttempts || 3,
				remove_on_complete_days: taskSpec.removeOnCompleteDays || null,
				remove_on_fail_days: taskSpec.removeOnFailDays || null,
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
			if (subscription?.queue === args.queueName && !subscription.execution_id) {
				this.eventSubscriptions.delete(id);
			}
		}
		for (const spec of args.eventSubscriptions || []) {
			const id = this.generateId();
			this.eventSubscriptions.set(id, {
				id,
				task_key: spec.task_key,
				queue: spec.queue,
				event_key: spec.event_key,
				filter: spec.filter,
			});
		}

		const registrations: CronRegistration[] = [];
		for (const cronSpec of args.cronSchedules || []) {
			if (!cronSpec.cron_expression || !cronSpec.dedupe_key) continue;
			const key = `${cronSpec.task_key}:${cronSpec.cron_expression}`;
			this.cronSchedules.set(key, {
				task_key: cronSpec.task_key,
				queue: cronSpec.queue,
				schedule_name: cronSpec.cron_expression,
				cron_expression: cronSpec.cron_expression,
				last_execution_id: null,
			});
			const existing = [...this.executions.values()].find(
				(execution) =>
					execution.task_key === cronSpec.task_key &&
					execution.queue === cronSpec.queue &&
					execution.dedupe_key === cronSpec.dedupe_key,
			);
			const id = existing?.id ?? (await this.invoke(cronSpec));
			if (!id) continue;
			registrations.push({
				id,
				task_key: cronSpec.task_key,
				queue: cronSpec.queue,
				is_maintenance: cronSpec.task_key === "pgconductor.maintenance",
				inserted: !existing,
				authoritative: !existing,
			});
		}
		return registrations;
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

			// Claim execution and consume any one-shot event link.
			const traceLinkContext = exec.trace_link_context;
			exec.state = "running";
			exec.attempts += 1;
			exec.orchestrator_id = args.orchestratorId;
			exec.trace_link_context = null;

			// Update concurrency count
			if (task?.concurrency != null) {
				concurrencyCount.set(
					this.taskId(exec.task_key, exec.queue),
					(concurrencyCount.get(this.taskId(exec.task_key, exec.queue)) || 0) + 1,
				);
			}

			const parent = exec.parent_execution_id
				? this.executions.get(exec.parent_execution_id)
				: undefined;
			const parentTask = parent
				? this.tasks.get(this.taskId(parent.task_key, parent.queue))
				: undefined;
			results.push({
				id: exec.id,
				task_key: exec.task_key,
				queue: exec.queue,
				payload: exec.payload,
				waiting_on_execution_id: exec.waiting_on_execution_id,
				waiting_step_key: exec.waiting_step_key,
				attempts: exec.attempts,
				cancelled: exec.cancelled,
				last_error: exec.last_error,
				dedupe_key: exec.dedupe_key || undefined,
				cron_expression: exec.cron_expression || undefined,
				group: exec.group,
				dead_letter_source_execution_id: exec.dead_letter_source_execution_id,
				dead_letter_source_queue: exec.dead_letter_source_queue,
				dead_letter_source_task_key: exec.dead_letter_source_task_key,
				dead_letter_error: exec.dead_letter_error,
				dead_letter_attempts: exec.dead_letter_attempts,
				dead_letter_failed_at: exec.dead_letter_failed_at,
				trace_context: exec.trace_context,
				trace_link_context: traceLinkContext,
				parent_execution_id: exec.parent_execution_id,
				parent_queue: parent?.queue,
				parent_task_key: parent?.task_key,
				parent_dead_letter_queue: parentTask?.dead_letter_queue,
				parent_dead_letter_task_key: parentTask?.dead_letter_task_key,
				locked_by: exec.orchestrator_id || "",
			});

			if (results.length >= args.batchSize) break;
		}

		return results;
	}

	async returnExecutions(
		resultsOrGrouped:
			| ExecutionResult[]
			| import("../../src/database-client").GroupedExecutionResults,
		_opts?: { signal?: AbortSignal },
	): Promise<import("../../src/database-client").ReturnExecutionsResult> {
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
		const deliveries: DeadLetterDelivery[] = [];
		const outcomes: SettlementOutcome[] = [];
		const recordOutcome = (execution: StoredExecution, outcome: SettlementOutcome["outcome"]) =>
			outcomes.push({ queue: execution.queue, task_key: execution.task_key, outcome, count: 1 });

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

					// Wake up parent if waiting
					if (exec.parent_execution_id) {
						const parent = this.executions.get(exec.parent_execution_id);
						if (parent && parent.waiting_on_execution_id === exec.id) {
							if (exec.parent_step_key) {
								this.saveWaitResult(parent, exec.parent_step_key, result.result || {});
							}
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

					// Remove if cleanup is configured (only if task registered)
					const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
					if (task && task.remove_on_complete_days != null) {
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
						recordOutcome(exec, exec.cancelled ? "cancellation" : "permanent_failure");
						exec.state = "failed";
						exec.failed_at = now;
						if (!exec.cancelled) {
							const delivery = this.deliverToDeadLetterQueue(
								exec,
								task,
								result.error,
								now,
								result.dead_letter_trace_contexts?.[exec.id],
							);
							if (delivery) {
								deliveries.push(delivery);
								recordOutcome(exec, "dead_letter");
							}
						}

						// Fail parent if waiting. A force-failed parent is itself terminal and
						// follows its own DLQ and retention policy.
						if (exec.parent_execution_id) {
							const parent = this.executions.get(exec.parent_execution_id);
							if (parent && parent.waiting_on_execution_id === exec.id) {
								recordOutcome(parent, exec.cancelled ? "cancellation" : "permanent_failure");
								parent.state = "failed";
								parent.last_error = `Child execution failed: ${result.error}`;
								parent.waiting_on_execution_id = null;
								parent.waiting_step_key = null;
								const parentTask = this.tasks.get(this.taskId(parent.task_key, parent.queue));
								if (!exec.cancelled) {
									const delivery = this.deliverToDeadLetterQueue(
										parent,
										parentTask,
										parent.last_error,
										now,
										result.dead_letter_trace_contexts?.[parent.id],
									);
									if (delivery) {
										deliveries.push(delivery);
										recordOutcome(parent, "dead_letter");
									}
								}
								if (parentTask?.remove_on_fail_days != null) {
									this.executions.delete(parent.id);
									this.steps.delete(parent.id);
								}
							}
						}

						// Remove if cleanup is configured (only if task registered)
						if (task && task.remove_on_fail_days != null) {
							this.executions.delete(exec.id);
							this.steps.delete(exec.id);
						}
					} else {
						// Retry with backoff
						recordOutcome(exec, "retry");
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
					recordOutcome(exec, exec.cancelled ? "cancellation" : "permanent_failure");
					exec.state = "failed";
					exec.failed_at = now;
					exec.last_error = result.error;

					const task = this.tasks.get(this.taskId(exec.task_key, exec.queue));
					if (!exec.cancelled) {
						const delivery = this.deliverToDeadLetterQueue(
							exec,
							task,
							result.error,
							now,
							result.dead_letter_trace_contexts?.[exec.id],
						);
						if (delivery) {
							deliveries.push(delivery);
							recordOutcome(exec, "dead_letter");
						}
					}
					exec.orchestrator_id = null;

					// Fail parent if waiting. A force-failed parent is itself terminal and
					// follows its own DLQ and retention policy.
					if (exec.parent_execution_id) {
						const parent = this.executions.get(exec.parent_execution_id);
						if (parent && parent.waiting_on_execution_id === exec.id) {
							recordOutcome(parent, exec.cancelled ? "cancellation" : "permanent_failure");
							parent.state = "failed";
							parent.last_error = `Child execution failed: ${result.error}`;
							parent.waiting_on_execution_id = null;
							parent.waiting_step_key = null;
							parent.waiting_timeout_at = null;
							const parentTask = this.tasks.get(this.taskId(parent.task_key, parent.queue));
							if (!exec.cancelled) {
								const delivery = this.deliverToDeadLetterQueue(
									parent,
									parentTask,
									parent.last_error,
									now,
									result.dead_letter_trace_contexts?.[parent.id],
								);
								if (delivery) {
									deliveries.push(delivery);
									recordOutcome(parent, "dead_letter");
								}
							}
							if (parentTask?.remove_on_fail_days != null) {
								this.executions.delete(parent.id);
								this.steps.delete(parent.id);
							}
						}
					}

					if (task && task.remove_on_fail_days != null) {
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
						trace_context: result.trace_context,
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

				// case "wait_for_db_event": {
				// 	// Create subscription
				// 	const subscriptionId = this.generateId();
				// 	this.eventSubscriptions.set(subscriptionId, {
				// 		id: subscriptionId,
				// 		execution_id: exec.id,
				// 		step_key: result.step_key,
				// 		source: "db",
				// 		schema_name: result.schema_name,
				// 		table_name: result.table_name,
				// 		operation: result.operation,
				// 		columns: result.columns,
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
		return { outcomes, deliveries };
	}

	async removeExecutions(
		_args: RemoveExecutionsArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<boolean> {
		// In the real implementation, this removes old completed/failed executions
		// For the in-memory client, we'll just return true (could be enhanced later)
		return true;
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

		// Helper to create execution
		const createExecution = (singletonOnValue: Date | null): string => {
			const task = this.tasks.get(this.taskId(spec.task_key, spec.queue));
			const id = this.generateId();

			const execution: StoredExecution = {
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
				singleton_on: singletonOnValue,
				cron_expression: spec.cron_expression || null,
				priority: spec.priority || 0,
				orchestrator_id: null,
				parent_execution_id: spec.parent_execution_id || null,
				parent_step_key: spec.parent_step_key || null,
				created_at: now,
				updated_at: now,
				failed_at: null,
				dead_letter_source_execution_id: null,
				dead_letter_source_queue: null,
				dead_letter_source_task_key: null,
				dead_letter_error: null,
				dead_letter_attempts: null,
				dead_letter_failed_at: null,
				trace_context: spec.trace_context || null,
				trace_link_context: null,
			};

			this.executions.set(id, execution);
			return id;
		};

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
						exec.trace_context = spec.trace_context || null;
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
						exec.trace_context = spec.trace_context || null;
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

	async registerEventWait(
		args: RegisterEventWaitArgs,
		_opts?: { signal?: AbortSignal },
	): Promise<boolean> {
		const exec = this.executions.get(args.executionId);
		if (
			!exec ||
			exec.queue !== args.queue ||
			exec.task_key !== args.taskKey ||
			exec.orchestrator_id !== args.orchestratorId ||
			exec.state !== "running" ||
			exec.cancelled ||
			exec.failed_at
		)
			return false;
		const existing = [...this.eventSubscriptions.values()].find(
			(subscription) =>
				subscription.execution_id === args.executionId && subscription.step_key === args.stepKey,
		);
		if (existing) return false;
		const id = this.generateId();
		this.eventSubscriptions.set(id, {
			id,
			task_key: args.taskKey,
			queue: args.queue,
			event_key: args.eventKey,
			filter: args.filter,
			execution_id: args.executionId,
			step_key: args.stepKey,
			source: "event",
			wait_after_event_position: this.lastEventPosition,
			timeout_at:
				args.timeoutMs == null ? null : new Date(this.getInternalTime().getTime() + args.timeoutMs),
		});
		exec.state = "pending";
		exec.attempts = Math.max(0, exec.attempts - 1);
		exec.run_at = new Date(8640000000000000);
		exec.waiting_on_execution_id = null;
		exec.waiting_step_key = args.stepKey;
		exec.waiting_timeout_at =
			args.timeoutMs == null ? null : new Date(this.getInternalTime().getTime() + args.timeoutMs);
		exec.orchestrator_id = null;
		return true;
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

	// // ============================================================================
	// // Events
	// // ============================================================================
	//
	// async emitEvent(args: { eventKey: string; payload: unknown }, _opts?: { signal?: AbortSignal }): Promise<string> {
	// 	const eventId = this.generateId();
	// 	const now = this.getInternalTime();
	//
	// 	// Find subscriptions waiting for this event
	// 	for (const [subId, sub] of this.eventSubscriptions.entries()) {
	// 		if (sub.source === "event" && sub.event_key === args.eventKey) {
	// 			// Check timeout
	// 			if (sub.timeout_at && now > sub.timeout_at) {
	// 				this.eventSubscriptions.delete(subId);
	// 				continue;
	// 			}
	//
	// 			// Wake up execution
	// 			const exec = this.executions.get(sub.execution_id);
	// 			if (exec) {
	// 				exec.state = "pending";
	// 				exec.run_at = now;
	// 				this.eventSubscriptions.delete(subId);
	// 			}
	// 		}
	// 	}
	//
	// 	return eventId;
	// }
	//
	// // Methods referenced in mock but not in main interface
	// async subscribeEvent(): Promise<string> {
	// 	return this.generateId();
	// }
	//
	// async subscribeDbChange(): Promise<string> {
	// 	return this.generateId();
	// }

	async invokeChild(): Promise<string> {
		return this.generateId();
	}

	async emitEvent(args: EmitEventArgs): Promise<string> {
		const id = this.generateId();
		this.customEvents.set(id, {
			id,
			event_key: args.eventKey,
			payload: args.payload ?? {},
			trace_context: args.trace_context ?? null,
			created_at: this.getInternalTime(),
			processed_at: null,
			event_position: ++this.lastEventPosition,
		});
		return id;
	}

	async resolveEventWaits(args: { batchSize: number }): Promise<number> {
		let count = 0;
		const now = this.getInternalTime();
		const candidates = [...this.eventSubscriptions.values()]
			.filter((subscription) => subscription.execution_id)
			.filter((subscription) => {
				const exec = this.executions.get(subscription.execution_id!);
				if (!exec || exec.waiting_step_key !== subscription.step_key) return false;
				const hasEvent = [...this.customEvents.values()].some(
					(candidate) =>
						candidate.event_key === subscription.event_key &&
						candidate.event_position > (subscription.wait_after_event_position ?? 0) &&
						(!subscription.timeout_at || candidate.created_at <= subscription.timeout_at) &&
						Object.entries(subscription.filter || {}).every(([field, values]) =>
							values.some((value) =>
								jsonEquals(
									value,
									isPayload(candidate.payload) ? candidate.payload[field] : undefined,
								),
							),
						),
				);
				return hasEvent || Boolean(subscription.timeout_at && subscription.timeout_at <= now);
			})
			.sort((left, right) => left.id.localeCompare(right.id))
			.slice(0, Math.max(args.batchSize || 0, 0));
		for (const subscription of candidates) {
			const exec = this.executions.get(subscription.execution_id!);
			if (!exec || exec.waiting_step_key !== subscription.step_key) continue;
			const event = [...this.customEvents.values()]
				.filter((candidate) => candidate.event_key === subscription.event_key)
				.filter(
					(candidate) => candidate.event_position > (subscription.wait_after_event_position ?? 0),
				)
				.filter(
					(candidate) =>
						!subscription.timeout_at || candidate.created_at <= subscription.timeout_at,
				)
				.filter((candidate) =>
					Object.entries(subscription.filter || {}).every(([field, values]) =>
						values.some((value) =>
							jsonEquals(
								value,
								isPayload(candidate.payload) ? candidate.payload[field] : undefined,
							),
						),
					),
				)
				.sort((a, b) => a.event_position - b.event_position)[0];
			if (!event && (!subscription.timeout_at || subscription.timeout_at > now)) continue;
			this.saveWaitResult(
				exec,
				subscription.step_key!,
				event
					? {
							result: { name: event.event_key, payload: structuredClone(event.payload) as Payload },
						}
					: { __pgconductor_wait_for_event_timeout: true },
			);
			exec.run_at = now;
			exec.waiting_step_key = null;
			exec.waiting_timeout_at = null;
			exec.trace_link_context = event?.trace_context ?? null;
			this.eventSubscriptions.delete(subscription.id);
			count++;
		}
		return count;
	}

	async processEvents(_args: { batchSize: number }): Promise<number> {
		let count = 0;
		const now = this.getInternalTime();
		const events = [...this.customEvents.values()]
			.filter((event) => !event.processed_at)
			.sort((left, right) => left.event_position - right.event_position);
		for (const event of events) {
			const payload = isPayload(event.payload) ? event.payload : {};
			for (const subscription of [...this.eventSubscriptions.values()]) {
				if (subscription.execution_id || subscription.event_key !== event.event_key) continue;
				const matches = Object.entries(subscription.filter || {}).every(([field, values]) =>
					values.some((value) => jsonEquals(value, payload[field])),
				);
				if (!matches) continue;
				const deliveryKey = `${event.id}:${subscription.id}`;
				if (this.eventDeliveries.has(deliveryKey)) continue;
				this.eventDeliveries.add(deliveryKey);
				await this.invoke({
					task_key: subscription.task_key,
					queue: subscription.queue,
					payload: { event: event.event_key, payload: structuredClone(event.payload) as Payload },
					trace_context: event.trace_context,
				});
			}
			event.processed_at = now;
			count++;
		}
		return count;
	}

	private saveWaitResult(exec: StoredExecution, stepKey: string, result: Payload): void {
		let steps = this.steps.get(exec.id);
		if (!steps) {
			steps = new Map();
			this.steps.set(exec.id, steps);
		}
		steps.set(stepKey, {
			execution_id: exec.id,
			step_key: stepKey,
			result,
			created_at: this.getInternalTime(),
		});
	}

	async removeProcessedEvents(args: { before: Date; batchSize: number }): Promise<boolean> {
		const old = [...this.customEvents.values()]
			.filter((event) => event.processed_at && event.processed_at < args.before)
			.slice(0, args.batchSize);
		for (const event of old) this.customEvents.delete(event.id);
		return old.length >= args.batchSize;
	}

	// ============================================================================
	private deliverToDeadLetterQueue(
		exec: StoredExecution,
		task: StoredTask | undefined,
		error: string,
		now: Date,
		traceContext?: Execution["trace_context"],
	): DeadLetterDelivery | null {
		if (!task?.dead_letter_queue || exec.cancelled) return null;
		const destinationTaskKey = task.dead_letter_task_key || exec.task_key;
		const duplicate = Array.from(this.executions.values()).some(
			(destination) =>
				destination.dead_letter_source_execution_id === exec.id &&
				destination.queue === task.dead_letter_queue &&
				destination.task_key === destinationTaskKey,
		);
		if (duplicate) return null;
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
			dead_letter_source_execution_id: exec.id,
			dead_letter_source_queue: exec.queue,
			dead_letter_source_task_key: exec.task_key,
			dead_letter_error: error,
			dead_letter_attempts: exec.attempts,
			dead_letter_failed_at: now,
			trace_context: traceContext || null,
			trace_link_context: null,
		});
		return {
			sourceExecutionId: exec.id,
			destinationExecutionId: id,
			destinationQueue: task.dead_letter_queue,
			destinationTaskKey,
		};
	}

	// Helpers
	// ============================================================================

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

	getCustomEvents(): StoredCustomEvent[] {
		return Array.from(this.customEvents.values());
	}

	clear(): void {
		this.executions.clear();
		this.steps.clear();
		this.tasks.clear();
		this.cronSchedules.clear();
		this.orchestrators.clear();
		this.eventSubscriptions.clear();
		this.customEvents.clear();
		this.eventDeliveries.clear();
		this.eventPartitions.clear();
		this.idCounter = 0;
		this.lastEventPosition = 0;
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

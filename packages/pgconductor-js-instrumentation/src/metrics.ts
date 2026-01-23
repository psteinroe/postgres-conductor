import type { Meter, Counter, Histogram } from "@opentelemetry/api";
import * as SemanticConventions from "./semantic-conventions";

export interface TaskInvocationArgs {
	taskName: string;
	queue: string;
}

export interface TaskExecutionArgs {
	taskName: string;
	queue: string;
	status: string;
	durationMs: number;
}

export interface TaskRetryArgs {
	taskName: string;
	queue: string;
}

export interface StepExecutionArgs {
	cached: boolean;
	durationMs: number;
}

/**
 * Metrics for Postgres Conductor instrumentation.
 */
export class PgConductorMetrics {
	private taskInvocations: Counter;
	private taskExecutions: Counter;
	private taskRetries: Counter;
	private taskDuration: Histogram;
	private stepExecutions: Counter;
	private stepDuration: Histogram;

	constructor(meter: Meter) {
		this.taskInvocations = meter.createCounter(SemanticConventions.METRIC_TASK_INVOCATIONS, {
			description: "Number of tasks enqueued",
			unit: "{invocation}",
		});

		this.taskExecutions = meter.createCounter(SemanticConventions.METRIC_TASK_EXECUTIONS, {
			description: "Number of tasks executed",
			unit: "{execution}",
		});

		this.taskRetries = meter.createCounter(SemanticConventions.METRIC_TASK_RETRIES, {
			description: "Number of task retries",
			unit: "{retry}",
		});

		this.taskDuration = meter.createHistogram(SemanticConventions.METRIC_TASK_DURATION, {
			description: "Task execution duration in milliseconds",
			unit: "ms",
		});

		this.stepExecutions = meter.createCounter(SemanticConventions.METRIC_STEP_EXECUTIONS, {
			description: "Number of steps executed",
			unit: "{execution}",
		});

		this.stepDuration = meter.createHistogram(SemanticConventions.METRIC_STEP_DURATION, {
			description: "Step execution duration in milliseconds",
			unit: "ms",
		});
	}

	/**
	 * Record a task invocation (enqueue).
	 */
	recordTaskInvocation(args: TaskInvocationArgs): void {
		this.taskInvocations.add(1, {
			[SemanticConventions.PGCONDUCTOR_TASK_NAME]: args.taskName,
			[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: args.queue,
		});
	}

	/**
	 * Record a task execution completion.
	 */
	recordTaskExecution(args: TaskExecutionArgs): void {
		const attributes = {
			[SemanticConventions.PGCONDUCTOR_TASK_NAME]: args.taskName,
			[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: args.queue,
			[SemanticConventions.PGCONDUCTOR_EXECUTION_STATUS]: args.status,
		};

		this.taskExecutions.add(1, attributes);
		this.taskDuration.record(args.durationMs, attributes);
	}

	/**
	 * Record a task retry.
	 */
	recordTaskRetry(args: TaskRetryArgs): void {
		this.taskRetries.add(1, {
			[SemanticConventions.PGCONDUCTOR_TASK_NAME]: args.taskName,
			[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: args.queue,
		});
	}

	/**
	 * Record a step execution.
	 */
	recordStepExecution(args: StepExecutionArgs): void {
		const attributes = {
			[SemanticConventions.PGCONDUCTOR_STEP_CACHED]: args.cached,
		};

		this.stepExecutions.add(1, attributes);
		this.stepDuration.record(args.durationMs, attributes);
	}
}

/**
 * Semantic conventions for Postgres Conductor instrumentation.
 * Following OpenTelemetry messaging semantic conventions where applicable.
 */

// Standard OTel messaging attributes
export const MESSAGING_SYSTEM = "messaging.system";
export const MESSAGING_OPERATION = "messaging.operation";
export const MESSAGING_DESTINATION_NAME = "messaging.destination.name";

// Postgres Conductor specific attributes
export const PGCONDUCTOR_TASK_NAME = "pgconductor.task.name";
export const PGCONDUCTOR_TASK_QUEUE = "pgconductor.task.queue";

export const PGCONDUCTOR_EXECUTION_ID = "pgconductor.execution.id";
export const PGCONDUCTOR_EXECUTION_ATTEMPT = "pgconductor.execution.attempt";
export const PGCONDUCTOR_EXECUTION_STATUS = "pgconductor.execution.status";

export const PGCONDUCTOR_STEP_NAME = "pgconductor.step.name";
export const PGCONDUCTOR_STEP_CACHED = "pgconductor.step.cached";

export const PGCONDUCTOR_PARENT_EXECUTION_ID = "pgconductor.parent.execution_id";
export const PGCONDUCTOR_CHILD_EXECUTION_ID = "pgconductor.child.execution_id";

export const PGCONDUCTOR_BATCH_SIZE = "pgconductor.batch.size";

export const PGCONDUCTOR_ERROR_MESSAGE = "pgconductor.error.message";
export const PGCONDUCTOR_ERROR_RETRYABLE = "pgconductor.error.retryable";

// Messaging system value
export const MESSAGING_SYSTEM_VALUE = "pgconductor";

// Messaging operation values
export const MESSAGING_OPERATION_PUBLISH = "publish";
export const MESSAGING_OPERATION_PROCESS = "process";

// Execution status values
export const EXECUTION_STATUS_COMPLETED = "completed";
export const EXECUTION_STATUS_FAILED = "failed";
export const EXECUTION_STATUS_RELEASED = "released";
export const EXECUTION_STATUS_INVOKE_CHILD = "invoke_child";
export const EXECUTION_STATUS_CANCELLED = "cancelled";

// Meter and span names
export const INSTRUMENTATION_NAME = "@pgconductor/js-instrumentation";

// Metric names
export const METRIC_TASK_INVOCATIONS = "pgconductor.task.invocations";
export const METRIC_TASK_EXECUTIONS = "pgconductor.task.executions";
export const METRIC_TASK_RETRIES = "pgconductor.task.retries";
export const METRIC_TASK_DURATION = "pgconductor.task.duration";
export const METRIC_STEP_EXECUTIONS = "pgconductor.step.executions";
export const METRIC_STEP_DURATION = "pgconductor.step.duration";
export const METRIC_QUEUE_DEPTH = "pgconductor.queue.depth";

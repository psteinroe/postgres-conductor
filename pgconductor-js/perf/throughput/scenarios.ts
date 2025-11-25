/**
 * Performance test scenarios
 */

export type Scenario = {
	workers: number;
	tasks: number;
	taskType: "noop" | "cpu" | "io" | "steps";
	workerSettings: {
		pollIntervalMs: number;
		flushIntervalMs: number;
		concurrency: number;
		fetchBatchSize: number;
		flushBatchSize?: number;
	};
};

export const scenarios = {
	// Default configuration - balanced settings
	default: {
		workers: 4,
		tasks: 10000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 100,
		},
	},

	// Quick smoke test
	quick: {
		workers: 2,
		tasks: 100,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 100,
		},
	},

	// High throughput - aggressive batching
	"high-throughput": {
		workers: 8,
		tasks: 100000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 500,
			flushIntervalMs: 500,
			concurrency: 50,
			fetchBatchSize: 500,
			flushBatchSize: 100,
		},
	},

	// Graphile-worker equivalent - matches their perf test
	"graphile-equivalent": {
		workers: 4,
		tasks: 200000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 10,
			flushIntervalMs: 0, // ⭐ Immediate flush like graphile-worker
			concurrency: 24,
			fetchBatchSize: 500,
			flushBatchSize: 100,
		},
	},

	// Low latency - frequent polling and flushing
	"low-latency": {
		workers: 4,
		tasks: 10000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 10,
			flushIntervalMs: 10,
			concurrency: 5,
			fetchBatchSize: 10,
		},
	},

	// CPU-bound workload
	"cpu-bound": {
		workers: 4,
		tasks: 5000,
		taskType: "cpu",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 50,
		},
	},

	// I/O-bound workload
	"io-bound": {
		workers: 4,
		tasks: 5000,
		taskType: "io",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 100,
			fetchBatchSize: 100,
		},
	},

	// Step memoization testing
	"with-steps": {
		workers: 4,
		tasks: 1000,
		taskType: "steps",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 50,
		},
	},

	// Scalability test - single worker baseline
	"scale-1": {
		workers: 1,
		tasks: 10000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 100,
		},
	},

	// Scalability test - 2 workers
	"scale-2": {
		workers: 2,
		tasks: 10000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 100,
		},
	},

	// Scalability test - 4 workers
	"scale-4": {
		workers: 4,
		tasks: 10000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 100,
		},
	},

	// Scalability test - 8 workers
	"scale-8": {
		workers: 8,
		tasks: 10000,
		taskType: "noop",
		workerSettings: {
			pollIntervalMs: 100,
			flushIntervalMs: 100,
			concurrency: 10,
			fetchBatchSize: 100,
		},
	},
} as const satisfies Record<string, Scenario>;

export type ScenarioName = keyof typeof scenarios;

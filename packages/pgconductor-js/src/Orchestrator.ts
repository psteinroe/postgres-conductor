import type { Task } from "./Task";

export type OrchestratorOptions = {
	connectionString: string;
	workflows: Task[];
};

// Manages the worker pool and handles worker lifecycle
export class Orchestrator {
	private readonly workers: Worker[] = [];

	constructor(private options: OrchestratorOptions) {}

	async start() {
		// initialize workers and start processing
	}

	async gracefulShutdown() {
		// gracefully stop all workers
	}

	async forcefulShutdown() {}

	async runOnce() {
		// run a single iteration of work processing
	}
}

// expose start / stop but also run once for testing and serverless

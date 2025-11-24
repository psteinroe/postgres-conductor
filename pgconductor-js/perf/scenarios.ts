export type Scenario = {
	description: string;
	queues: string[];
	tasks: { key: string; queue: string; maxAttempts?: number }[];
	executions: {
		pending?: { queue: string; task_key: string; count: number; steps?: number }[];
		completed?: { queue: string; task_key: string; count: number; steps?: number }[];
		failed?: { queue: string; task_key: string; count: number; steps?: number }[];
	};
};

// Helper: Generate N queues
function queues(n: number): string[] {
	return Array.from({ length: n }, (_, i) => `q${i}`);
}

// Helper: Generate tasks for queues
function tasks(
	queues: string[],
	tasksPerQueue: number,
	maxAttempts = 3,
): { key: string; queue: string; maxAttempts: number }[] {
	return queues.flatMap((q) =>
		Array.from({ length: tasksPerQueue }, (_, i) => ({
			key: `${q}-t${i}`,
			queue: q,
			maxAttempts,
		})),
	);
}

// Helper: Distribute executions across tasks
function distribute(
	total: number,
	tasks: string[],
	queue: string,
	distribution: "uniform" | "hotspot" = "uniform",
): { queue: string; task_key: string; count: number }[] {
	if (distribution === "uniform") {
		const perTask = Math.floor(total / tasks.length);
		return tasks.map((t) => ({ queue, task_key: t, count: perTask }));
	}

	// 80% of executions on 20% of tasks
	const hotTasks = tasks.slice(0, Math.ceil(tasks.length * 0.2));
	const coldTasks = tasks.slice(hotTasks.length);
	const hotCount = Math.floor((total * 0.8) / hotTasks.length);
	const coldCount = Math.floor((total * 0.2) / coldTasks.length);

	return [
		...hotTasks.map((t) => ({ queue, task_key: t, count: hotCount })),
		...coldTasks.map((t) => ({ queue, task_key: t, count: coldCount })),
	];
}

export const scenarios: Record<string, Scenario> = {
	baseline: {
		description: "1 queue, 1 task, 1K pending",
		queues: ["default"],
		tasks: [{ key: "task-1", queue: "default", maxAttempts: 3 }],
		executions: {
			pending: [{ queue: "default", task_key: "task-1", count: 1_000 }],
		},
	},

	"heavy-single-queue": {
		description: "1 queue, 1 task, 1M pending",
		queues: ["default"],
		tasks: [{ key: "task-1", queue: "default", maxAttempts: 3 }],
		executions: {
			pending: [{ queue: "default", task_key: "task-1", count: 1_000_000 }],
		},
	},

	"many-queues-sparse": (() => {
		const qs = queues(100);
		const ts = tasks(qs, 5);
		return {
			description: "100 queues, 5 tasks each, 10K total (uniform)",
			queues: qs,
			tasks: ts,
			executions: {
				pending: distribute(
					10_000,
					ts.map((t) => t.key),
					"default",
					"uniform",
				),
			},
		};
	})(),

	"high-contention": (() => {
		const qs = ["default"];
		const ts = tasks(qs, 50);
		return {
			description: "1 queue, 50 tasks, 100K executions (80/20 hotspot)",
			queues: qs,
			tasks: ts,
			executions: {
				pending: distribute(
					100_000,
					ts.map((t) => t.key),
					"default",
					"hotspot",
				),
			},
		};
	})(),

	"mixed-states": {
		description: "10K pending, 50K completed, 1K failed",
		queues: ["default"],
		tasks: [{ key: "task-1", queue: "default", maxAttempts: 3 }],
		executions: {
			pending: [{ queue: "default", task_key: "task-1", count: 10_000 }],
			completed: [{ queue: "default", task_key: "task-1", count: 50_000 }],
			failed: [{ queue: "default", task_key: "task-1", count: 1_000 }],
		},
	},

	"with-steps": {
		description: "10K pending with 10 steps each",
		queues: ["default"],
		tasks: [{ key: "task-1", queue: "default", maxAttempts: 3 }],
		executions: {
			pending: [{ queue: "default", task_key: "task-1", count: 10_000, steps: 10 }],
		},
	},
};

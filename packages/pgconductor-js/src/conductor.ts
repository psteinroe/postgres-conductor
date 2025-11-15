import type { Sql } from "postgres";
import { DatabaseClient, type ExecutionSpec } from "./database-client";
import { Task, type TaskConfiguration, type AnyTask } from "./task";
import type { TaskContext } from "./task-context";
import {
	type FindTaskByName,
	type InferPayload,
	type InferReturns,
	type TaskDefinition,
	type TaskName,
} from "./task-definition";
import { Worker, type WorkerConfig } from "./worker";

type ConnectionOptions =
	| { connectionString: string; sql?: never }
	| { sql: Sql; connectionString?: never };

export type ConductorOptions<
	Tasks extends readonly TaskDefinition<string, any, any>[],
	ExtraContext extends object,
> = ConnectionOptions & {
	tasks: Tasks;

	context: ExtraContext;

	// logger

	// events
};

// similar to inngest client
// exposes the main createTask methods and handles types
export class Conductor<
	Tasks extends readonly TaskDefinition<string, any, any>[],
	ExtraContext extends object = {},
> {
	/**
	 * @internal
	 * Internal database client
	 */
	readonly db: DatabaseClient;

	constructor(public readonly options: ConductorOptions<Tasks, ExtraContext>) {
		if ("sql" in options && options.sql) {
			this.db = new DatabaseClient({ sql: options.sql });
		} else if ("connectionString" in options && options.connectionString) {
			this.db = new DatabaseClient({
				connectionString: options.connectionString,
			});
		} else {
			throw new Error(
				"Conductor requires either a connectionString or sql instance",
			);
		}
	}

	createTask<
		TName extends TaskName<Tasks>,
		TDef extends FindTaskByName<Tasks, TName>,
		TPayload extends object = InferPayload<TDef>,
		TReturns extends object | void = InferReturns<TDef>,
		TQueue extends string = TDef extends { queue: infer Q extends string }
			? Q
			: "default",
	>(
		definition: TaskConfiguration<TName, TQueue>,
		fn: (
			payload: TPayload,
			ctx: TaskContext & ExtraContext,
		) => Promise<TReturns>,
	): Task<TQueue, TName, TPayload, TReturns, TaskContext & ExtraContext> {
		return new Task(definition, fn);
	}

	createWorker(
		queueName: string,
		tasks: AnyTask[],
		config?: Partial<WorkerConfig>,
	): Worker {
		return new Worker(
			queueName,
			new Map(tasks.map((task) => [task.name, task])),
			this.db,
			config,
			this.options.context,
		);
	}

	async invoke<
		TName extends TaskName<Tasks>,
		TDef extends FindTaskByName<Tasks, TName>,
	>(
		task_key: TName,
		payload: InferPayload<TDef>,
		opts?: Omit<ExecutionSpec, "task_key" | "payload">,
	): Promise<void>;
	async invoke<
		TName extends TaskName<Tasks>,
		TDef extends FindTaskByName<Tasks, TName>,
	>(
		task_key: TName,
		items: Array<
			{ payload: InferPayload<TDef> } & Omit<
				ExecutionSpec,
				"task_key" | "payload"
			>
		>,
	): Promise<void>;
	async invoke<
		TName extends TaskName<Tasks>,
		TDef extends FindTaskByName<Tasks, TName>,
	>(
		task_key: TName,
		payloadOrItems:
			| InferPayload<TDef>
			| Array<
					{ payload: InferPayload<TDef> } & Omit<
						ExecutionSpec,
						"task_key" | "payload"
					>
			  >,
		opts?: Omit<ExecutionSpec, "task_key" | "payload">,
	): Promise<void | string[]> {
		if (Array.isArray(payloadOrItems)) {
			const specs = payloadOrItems.map((item) => ({
				task_key,
				payload: item.payload,
				run_at: item.run_at,
				key: item.key,
				priority: item.priority,
			}));
			return this.db.invokeBatch(specs);
		}

		return this.db.invoke({
			task_key,
			payload: payloadOrItems,
			...opts,
		});
	}

	// emit(name: string): Promise<void> {}
}

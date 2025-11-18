import type { Sql } from "postgres";
import { DatabaseClient, type ExecutionSpec } from "./database-client";
import {
	Task,
	type TaskConfiguration,
	type AnyTask,
	type TaskEventFromTriggers,
} from "./task";
import type { TaskContext } from "./task-context";
import {
	type FindTaskByName,
	type InferPayload,
	type InferReturns,
	type NonEmptyArray,
	type TaskDefinition,
	type TaskName,
	type Trigger,
	type ValidateTriggers,
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
		TName extends string,
		TQueue extends string = "default",
		TTriggers extends NonEmptyArray<Trigger> | Trigger = Trigger,
		TPayload extends object = TName extends TaskName<Tasks>
			? InferPayload<FindTaskByName<Tasks, TName>>
			: {},
	>(
		definition: TaskConfiguration<TName, TQueue>,
		triggers: ValidateTriggers<Tasks, TName, TTriggers>,
		fn: (
			event: TaskEventFromTriggers<TTriggers, TPayload>,
			ctx: TaskContext & ExtraContext,
		) => Promise<
			TName extends TaskName<Tasks>
				? InferReturns<FindTaskByName<Tasks, TName>>
				: void
		>,
	): Task<
		TQueue,
		TName,
		TPayload,
		TName extends TaskName<Tasks>
			? InferReturns<FindTaskByName<Tasks, TName>>
			: void,
		TaskContext & ExtraContext,
		TaskEventFromTriggers<TTriggers, TPayload>
	> {
		return new Task(definition, triggers as TTriggers, fn);
	}

	createWorker(
		queueName: string,
		tasks: AnyTask[],
		config?: Partial<WorkerConfig>,
	): Worker {
		return new Worker(queueName, tasks, this.db, config, this.options.context);
	}

	async invoke<
		TName extends TaskName<Tasks>,
		TDef extends FindTaskByName<Tasks, TName>,
	>(
		task_key: TName,
		payload: InferPayload<TDef>,
		opts?: Omit<ExecutionSpec, "task_key" | "payload">,
	): Promise<string>;
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
	): Promise<string[]>;
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
	): Promise<string | string[]> {
		if (Array.isArray(payloadOrItems)) {
			const specs = payloadOrItems.map((item) => ({
				task_key,
				payload: item.payload,
				run_at: item.run_at,
				dedupe_key: item.dedupe_key,
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

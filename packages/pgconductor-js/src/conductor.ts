import type { Sql } from "postgres";
import { DatabaseClient } from "./database-client";
import { Task } from "./task";
import type { TaskContext } from "./task-context";
import {
	type FindTaskByName,
	type InferPayload,
	type InferReturns,
	type TaskDefinition,
	type TaskName,
} from "./task-definition";

// should get db client!
// maybe migration store just on SchemaManager (Orchestrator only needs latest version?)

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
	public readonly db: DatabaseClient;

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
	>(
		name: TName,
		fn: (
			payload: InferPayload<TDef>,
			ctx: TaskContext & ExtraContext,
		) => Promise<InferReturns<TDef>>,
	): Task<
		TName,
		InferPayload<TDef>,
		InferReturns<TDef>,
		TaskContext & ExtraContext
	> {
		return new Task(name, fn);
	}

	async invokeTask<
		TName extends TaskName<Tasks>,
		TDef extends FindTaskByName<Tasks, TName>,
	>(name: TName, payload: InferPayload<TDef>): Promise<void> {
		// implement task invocation logic here
		// should be overloaded with batch version
		// no return value - we just invoke and dont wait for result
		// this.db.invoke();
	}

	// emitEvent(name: string): Promise<void> {}
}

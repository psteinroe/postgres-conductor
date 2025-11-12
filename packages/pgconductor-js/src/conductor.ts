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

export type ConductorOptions<
	Tasks extends readonly TaskDefinition<string, any, any>[],
	ExtraContext extends object,
> = {
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
	constructor(public readonly options: ConductorOptions<Tasks, ExtraContext>) {}

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

	// emitEvent(name: string): Promise<void> {}
}

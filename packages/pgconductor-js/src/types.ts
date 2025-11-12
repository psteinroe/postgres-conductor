import type { TaskContext } from "./task-context";

export type ExecuteFunction = (
	payload: any,
	context: TaskContext,
) => Promise<any>;

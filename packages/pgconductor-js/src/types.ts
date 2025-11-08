import type { TaskContext } from "./TaskContext";

export type HandlerFunction = (
	payload: any,
	context: TaskContext,
) => Promise<any>;

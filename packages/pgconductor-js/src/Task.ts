import type { HandlerFunction } from "./types";

// Represents a task definition that can be invoked or triggered by events
export class Task {
	public __type = "Task";

	constructor(private readonly handler: HandlerFunction) {}
}

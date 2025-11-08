// exposes the main createFunction methods and handles types

import { Task } from "./Task";
import type { HandlerFunction } from "./types";

// similar to inngest client
export class Conductor {
	// type-safe context
	// event and function schemas from standard schema

	createTask(
		{ queue, name }: { queue: string; name: string },
		fn: HandlerFunction,
	): Task {
		return new Task();
	}
}

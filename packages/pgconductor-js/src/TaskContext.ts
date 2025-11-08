// second argument for tasks
export class TaskContext {
	// ctx.step()
	// ctx.workflow()
	// ...dependencies

	// should receive AbortSignal for cancellation
	constructor({ signal }: { signal: AbortSignal }) {}
}

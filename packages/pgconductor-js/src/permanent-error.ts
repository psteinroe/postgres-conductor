// A global symbol, so an error of another copy of the package is recognized too
const permanent = Symbol.for("pgconductor.permanent-error");

/**
 * An error no retry can fix, e.g. invalid input found inside the handler. Thrown from a handler,
 * a `ctx.step()` callback, a `ctx.onSignal()` handler or middleware, it fails the execution
 * permanently at once, without the attempts it has left, and its message is the execution's
 * error. A batch handler returns it in place of a member's result to fail only that member.
 */
export class PermanentError extends Error {
	readonly code = "PGCONDUCTOR_PERMANENT";
	readonly [permanent] = true;

	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "PermanentError";
	}
}

/** @internal Whether `error` is a `PermanentError`, of this or another copy of the package */
export function isPermanentError(error: unknown): error is PermanentError {
	return typeof error === "object" && error !== null && permanent in error;
}

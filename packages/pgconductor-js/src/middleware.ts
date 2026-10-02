import type { Payload } from "./database-client";
import type { TaskContext } from "./task-context";

declare const middlewareContext: unique symbol;

/** Opaque result of `next`. Returning it lets the added context flow into handler types. */
export type MiddlewareResult<TContext extends object> = {
	readonly [middlewareContext]: TContext;
};

export type MiddlewareExecution<Metadata extends object = Payload> = {
	readonly id: string;
	readonly task_key: string;
	readonly queue: string;
	/** Retry attempt, starting at 1. */
	readonly attempt: number;
	readonly parent_execution_id: string | null;
	/**
	 * Whether earlier attempts left durable progress: a persisted step (including sleeps, received
	 * events and child results), a pending event wait, or a pending child invocation.
	 */
	readonly resumed: boolean;
	/** Same as `ctx.metadata`: `undefined` when the execution has none. */
	readonly metadata: Readonly<Metadata> | undefined;
};

/**
 * Runs once per handler attempt, including every resume. The object passed to `next` is merged
 * into the task context. `next` settles when the attempt ends: it resolves when the handler
 * returns, suspends, is cancelled or times out, and rejects when the handler throws.
 */
export type Middleware<
	TContext extends object = TaskContext,
	TAdded extends object = object,
	Metadata extends object = Payload,
> = (
	args: { execution: MiddlewareExecution<Metadata>; ctx: TContext },
	next: <TNext extends object>(ctx: TNext) => Promise<MiddlewareResult<TNext>>,
) => Promise<MiddlewareResult<TAdded>>;

type UnionToIntersection<T> = (T extends unknown ? (value: T) => void : never) extends (
	value: infer I,
) => void
	? I
	: never;

/** Intersection of the context each middleware adds. */
export type AddedContext<TAdded extends readonly object[]> = UnionToIntersection<TAdded[number]> &
	object;

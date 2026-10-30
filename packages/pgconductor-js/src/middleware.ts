import type { Payload } from "./database-client.js";
import type { TaskContext } from "./task-context.js";

declare const middlewareContext: unique symbol;

/** Opaque result of `next`. Returning it lets the added context flow into handler types. */
export type MiddlewareResult<TContext extends object> = {
	readonly [middlewareContext]: TContext;
};

export type MiddlewareExecution<Metadata extends object = Payload> = {
	readonly id: string;
	readonly task_key: string;
	readonly queue: string;
	/** Same as `ctx.metadata`: `undefined` when the execution has none. */
	readonly metadata: Readonly<Metadata> | undefined;
};

/**
 * Runs once per handler attempt, including every resume. The object passed to `next` is merged
 * into the task context; a context method it contains replaces the original for later middleware
 * and the handler, and must keep its signature. `next` settles when the attempt ends: it resolves
 * when the handler returns, suspends, is cancelled or times out, and rejects when the handler
 * throws.
 */
export type Middleware<
	TContext extends object = TaskContext,
	TAdded extends object = object,
	Metadata extends object = Payload,
> = (
	args: { execution: MiddlewareExecution<Metadata>; ctx: TContext },
	next: <TNext extends object>(ctx: TNext & Partial<TContext>) => Promise<MiddlewareResult<TNext>>,
) => Promise<MiddlewareResult<TAdded>>;

type UnionToIntersection<T> = (T extends unknown ? (value: T) => void : never) extends (
	value: infer I,
) => void
	? I
	: never;

/** Intersection of the context each middleware adds, without the keys it overrides in `TBase`. */
export type AddedContext<TAdded extends readonly object[], TBase extends object> = Omit<
	UnionToIntersection<TAdded[number]> & object,
	keyof TBase
>;

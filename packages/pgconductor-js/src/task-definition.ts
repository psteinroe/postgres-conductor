import type { StandardSchemaV1 } from "@standard-schema/spec";

type ObjectSchema = StandardSchemaV1<unknown, object>;

export type TaskDefinition<
	Name extends string,
	Payload = undefined,
	Returns = undefined,
	Queue extends string = "default",
> = {
	readonly name: Name;
	readonly queue: Queue;
	readonly payload: Payload;
	readonly returns: Returns;
};

/**
 * Type helper for defining tasks using pure TypeScript types (no runtime schema).
 *
 * @example
 * type SendEmail = DefineTask<{
 *   name: "send-email";
 *   queue: "notifications";
 *   payload: { to: string };
 *   returns: { sent: boolean };
 * }>;
 */
export type DefineTask<T extends {
	name: string;
	queue?: string;
	payload?: unknown;
	returns?: unknown;
}> = TaskDefinition<
	T["name"],
	T extends { payload: infer P } ? P : undefined,
	T extends { returns: infer R } ? R : undefined,
	T extends { queue: infer Q extends string } ? Q : "default"
>;

export function defineTask<Name extends string, Queue extends string>(def: {
	name: Name;
	queue: Queue;
}): TaskDefinition<Name, undefined, undefined, Queue>;
export function defineTask<Name extends string>(def: {
	name: Name;
	queue?: never;
}): TaskDefinition<Name, undefined, undefined, "default">;
export function defineTask<
	Name extends string,
	Queue extends string,
	Payload extends ObjectSchema,
>(def: {
	name: Name;
	queue: Queue;
	payload: Payload;
}): TaskDefinition<Name, Payload, undefined, Queue>;
export function defineTask<Name extends string, Payload extends ObjectSchema>(def: {
	name: Name;
	queue?: never;
	payload: Payload;
}): TaskDefinition<Name, Payload, undefined, "default">;
export function defineTask<
	Name extends string,
	Queue extends string,
	Payload extends ObjectSchema,
	Returns extends ObjectSchema,
>(def: {
	name: Name;
	queue: Queue;
	payload?: Payload;
	returns?: Returns;
}): TaskDefinition<Name, Payload, Returns, Queue>;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
	Returns extends ObjectSchema,
>(def: {
	name: Name;
	queue?: never;
	payload?: Payload;
	returns?: Returns;
}): TaskDefinition<Name, Payload, Returns, "default">;

export function defineTask(def: any) {
	return { ...def, queue: def.queue || "default" };
}

type EnsureObject<T> = T extends object ? T : {};

export type InferPayload<T> = T extends TaskDefinition<string, infer P, any, string>
	? P extends undefined
		? {}
		: P extends StandardSchemaV1<any, infer O>
			? EnsureObject<O>
			: EnsureObject<P> // Plain type (type-only definition)
	: never;

export type InferReturns<T> = T extends TaskDefinition<string, any, infer R, string>
	? R extends undefined
		? void
		: R extends StandardSchemaV1<any, infer O>
			? EnsureObject<O>
			: EnsureObject<R> // Plain type (type-only definition)
	: never;

export type TaskName<
	TTasks extends readonly TaskDefinition<string, any, any, string>[],
> = TTasks[number]["name"];

export type FindTaskByIdentifier<
	TTasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TQueue extends string = "default",
> = Extract<TTasks[number], { name: TName; queue: TQueue }>;

// Trigger types
export type InvocableTrigger = { invocable: true };
export type CronTrigger = { cron: string };
export type Trigger = InvocableTrigger | CronTrigger;

// Check if triggers include invocable
export type HasInvocable<TTriggers> = TTriggers extends readonly any[]
	? Extract<TTriggers[number], InvocableTrigger> extends never
		? false
		: true
	: TTriggers extends InvocableTrigger
		? true
		: false;

// Check if triggers include cron
export type HasCron<TTriggers> = TTriggers extends readonly any[]
	? Extract<TTriggers[number], CronTrigger> extends never
		? false
		: true
	: TTriggers extends CronTrigger
		? true
		: false;

// Non-empty array type
export type NonEmptyArray<T> = [T, ...T[]];

// Helper: Require at least one element in array matches the required type
// Uses Extract to handle readonly/mutable variants
type RequireAtLeastOneWith<TArray extends readonly any[], TRequired> = Extract<
	TArray[number],
	TRequired
> extends never
	? never
	: TArray;

// Helper: Check if all elements are NOT of a certain type
// Uses Extract to handle readonly/mutable variants
type NoneOf<TArray extends readonly any[], TForbidden> = Extract<
	TArray[number],
	TForbidden
> extends never
	? TArray
	: never;

// Check if task identifier exists in the conductor's task catalog
type TaskIdentifierIsDefined<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TQueue extends string,
> = Extract<Tasks[number], { name: TName; queue: TQueue }> extends never ? false : true;

// Validate triggers based on whether task is defined
export type ValidateTriggers<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TTriggers extends NonEmptyArray<Trigger> | Trigger,
	TQueue extends string = "default",
> = TTriggers extends readonly Trigger[]
	? // Array case - must be non-empty
		TTriggers extends NonEmptyArray<Trigger>
		? TaskIdentifierIsDefined<Tasks, TName, TQueue> extends true
			? RequireAtLeastOneWith<TTriggers, InvocableTrigger> extends never
				? `Task "${TName}" of queue "${TQueue}" is defined in the conductor catalog. You must include { invocable: true } in the triggers array to allow manual invocations.`
				: TTriggers
			: NoneOf<TTriggers, InvocableTrigger> extends never
				? `Task "${TName}" of queue "${TQueue}" is not defined in the conductor catalog. Either remove { invocable: true } from triggers (for cron-only tasks), or add a task definition to the conductor's tasks array.`
				: TTriggers
		: `Triggers array cannot be empty. Provide at least one trigger.`
	: // Single trigger case
		TaskIdentifierIsDefined<Tasks, TName, TQueue> extends true
		? TTriggers extends InvocableTrigger
			? TTriggers
			: `Task "${TName}" of queue "${TQueue}" is defined in the conductor catalog. You must include { invocable: true } in the triggers to allow manual invocations.`
		: TTriggers extends InvocableTrigger
			? `Task "${TName}" of queue "${TQueue}" is not defined in the conductor catalog. Remove { invocable: true } from triggers (for cron-only tasks), or add a task definition to the conductor's tasks array.`
			: TTriggers;

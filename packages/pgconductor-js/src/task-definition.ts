import type { StandardSchemaV1 } from "@standard-schema/spec";

type ObjectSchema = StandardSchemaV1<unknown, object>;

export type TaskDefinition<
	Name extends string,
	Payload extends ObjectSchema | undefined = undefined,
	Returns extends ObjectSchema | undefined = undefined,
	Queue extends string = "default",
> = {
	name: Name;
	queue: Queue;
	payload: Payload;
	returns: Returns;
};

export function defineTask<Name extends string>(def: {
	name: Name;
	queue?: string;
}): TaskDefinition<
	Name,
	undefined,
	undefined,
	typeof def.queue extends string ? typeof def.queue : "default"
>;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
>(def: {
	name: Name;
	queue?: string;
	payload: Payload;
}): TaskDefinition<
	Name,
	Payload,
	undefined,
	typeof def.queue extends string ? typeof def.queue : "default"
>;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
	Returns extends ObjectSchema,
>(def: {
	name: Name;
	queue?: string;
	payload?: Payload;
	returns?: Returns;
}): TaskDefinition<
	Name,
	Payload,
	Returns,
	typeof def.queue extends string ? typeof def.queue : "default"
>;
export function defineTask(def: any) {
	return { ...def, queue: def.queue || "default" };
}

type EnsureObject<T> = T extends object ? T : {};

export type InferPayload<T> = T extends TaskDefinition<string, infer P, any>
	? P extends undefined
		? {}
		: P extends StandardSchemaV1<any, infer O>
			? EnsureObject<O>
			: never
	: never;

export type InferReturns<T> = T extends TaskDefinition<string, any, infer R>
	? R extends undefined
		? void
		: R extends StandardSchemaV1<any, infer O>
			? EnsureObject<O>
			: never
	: never;

export type TaskName<
	TTasks extends readonly TaskDefinition<string, any, any>[],
> = TTasks[number]["name"];

export type FindTaskByName<
	TTasks extends readonly TaskDefinition<string, any, any>[],
	TName extends string,
> = Extract<TTasks[number], { name: TName }>;

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

// Check if task name exists in the conductor's task catalog
type TaskIsDefined<
	Tasks extends readonly TaskDefinition<string, any, any>[],
	TName extends string,
> = TName extends Tasks[number]["name"] ? true : false;

// Validate triggers based on whether task is defined
export type ValidateTriggers<
	Tasks extends readonly TaskDefinition<string, any, any>[],
	TName extends string,
	TTriggers extends NonEmptyArray<Trigger> | Trigger,
> = TTriggers extends readonly Trigger[]
	? // Array case - must be non-empty
		TTriggers extends NonEmptyArray<Trigger>
		? TaskIsDefined<Tasks, TName> extends true
			? RequireAtLeastOneWith<TTriggers, InvocableTrigger> extends never
				? `Task "${TName}" is defined in the conductor catalog. You must include { invocable: true } in the triggers array to allow manual invocations.`
				: TTriggers
			: NoneOf<TTriggers, InvocableTrigger> extends never
				? `Task "${TName}" is not defined in the conductor catalog. Either remove { invocable: true } from triggers (for cron-only tasks), or add a task definition to the conductor's tasks array.`
				: TTriggers
		: `Triggers array cannot be empty. Provide at least one trigger.`
	: // Single trigger case
		TaskIsDefined<Tasks, TName> extends true
		? TTriggers extends InvocableTrigger
			? TTriggers
			: `Task "${TName}" is defined in the conductor catalog. You must include { invocable: true } in the triggers to allow manual invocations.`
		: TTriggers extends InvocableTrigger
			? `Task "${TName}" is not defined in the conductor catalog. Remove { invocable: true } from triggers (for cron-only tasks), or add a task definition to the conductor's tasks array.`
			: TTriggers;

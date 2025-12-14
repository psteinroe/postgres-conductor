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
export type DefineTask<
	T extends {
		name: string;
		queue?: string;
		payload?: unknown;
		returns?: unknown;
	},
> = TaskDefinition<
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

export type InferPayload<T> =
	T extends TaskDefinition<string, infer P, any, string>
		? P extends undefined
			? {}
			: P extends StandardSchemaV1<any, infer O>
				? EnsureObject<O>
				: EnsureObject<P> // Plain type (type-only definition)
		: never;

export type InferReturns<T> =
	T extends TaskDefinition<string, any, infer R, string>
		? R extends undefined
			? void
			: R extends StandardSchemaV1<any, infer O>
				? EnsureObject<O>
				: EnsureObject<R> // Plain type (type-only definition)
		: never;

export type TaskName<TTasks extends readonly TaskDefinition<string, any, any, string>[]> =
	TTasks[number]["name"];

export type FindTaskByIdentifier<
	TTasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TQueue extends string = "default",
> = Extract<TTasks[number], { name: TName; queue: TQueue }>;

// Trigger types
export type InvocableTrigger = { invocable: true };
export type CronTrigger = { cron: string; name: string };

// Event trigger - triggers when a custom event is emitted
// export type CustomEventTrigger<TName extends string = string> = {
// 	event: TName;
// };

// Database event trigger - triggers on CDC events
// export type DatabaseEventTrigger<
// 	TSchema extends string = string,
// 	TTable extends string = string,
// 	TOp extends "insert" | "update" | "delete" = "insert" | "update" | "delete",
// 	TColumns extends string | undefined = undefined,
// > = {
// 	schema: TSchema;
// 	table: TTable;
// 	operation: TOp;
// 	columns?: TColumns;
// };

export type Trigger = InvocableTrigger | CronTrigger; // | CustomEventTrigger | DatabaseEventTrigger;

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

// Check if triggers include custom event
// export type HasCustomEvent<TTriggers> = TTriggers extends readonly any[]
// 	? Extract<TTriggers[number], CustomEventTrigger> extends never
// 		? false
// 		: true
// 	: TTriggers extends CustomEventTrigger
// 		? true
// 		: false;

// Check if triggers include database event
// export type HasDatabaseEvent<TTriggers> = TTriggers extends readonly any[]
// 	? {
// 			schema: string;
// 			table: string;
// 			operation: "insert" | "update" | "delete";
// 		} extends TTriggers[number]
// 		? false
// 		: Extract<
// 					TTriggers[number],
// 					{ schema: string; table: string; operation: "insert" | "update" | "delete" }
// 			  > extends never
// 			? false
// 			: true
// 	: TTriggers extends { schema: string; table: string; operation: "insert" | "update" | "delete" }
// 		? true
// 		: false;

// Non-empty array type
export type NonEmptyArray<T> = [T, ...T[]];
export type NonEmptyReadonlyArray<T> = readonly [T, ...T[]];

// Check if task identifier exists in the conductor's task catalog
type TaskIdentifierIsDefined<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TQueue extends string,
> = Extract<Tasks[number], { name: TName; queue: TQueue }> extends never ? false : true;

// Extract event names from triggers
// type ExtractEventNames<TTriggers> = TTriggers extends readonly any[]
// 	? TTriggers[number] extends { event: infer E extends string }
// 		? E
// 		: never
// 	: TTriggers extends { event: infer E extends string }
// 		? E
// 		: never;

// Check if all event names in triggers exist in the Events schema
// type AllEventsExist<Events extends readonly { name: string }[], TTriggers> =
// 	ExtractEventNames<TTriggers> extends infer EventNames
// 		? EventNames extends string
// 			? EventNames extends Events[number]["name"]
// 				? true
// 				: EventNames
// 			: true // No event triggers
// 		: true;

// Validate triggers based on whether task is defined
// Rules:
// - If has invocable: true → task must be in catalog (to know payload types for invoke)
// - If task is not in catalog → must not have invocable: true
// - If has custom event trigger → event must be defined in Events schema
// - Task in catalog can have any trigger type (invocable, cron, event)
export type ValidateTriggers<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	// Events extends readonly { name: string }[],
	TName extends string,
	TTriggers,
	TQueue extends string = "default",
> = TTriggers extends readonly Trigger[]
	? // Array case - must be non-empty
		TTriggers extends NonEmptyReadonlyArray<Trigger>
		? HasInvocable<TTriggers> extends true
			? TaskIdentifierIsDefined<Tasks, TName, TQueue> extends true
				? TTriggers
				: // ? AllEventsExist<Events, TTriggers> extends true
					// 	? TTriggers
					// 	: `Event "${AllEventsExist<Events, TTriggers> & string}" is not defined in the conductor's events. Add an event definition to the conductor's events array.`
					`Task "${TName}" of queue "${TQueue}" is not defined in the conductor catalog. Either remove { invocable: true } from triggers, or add a task definition to the conductor's tasks array.`
			: TTriggers
		: // : AllEventsExist<Events, TTriggers> extends true
			// 	? TTriggers
			// 	: `Event "${AllEventsExist<Events, TTriggers> & string}" is not defined in the conductor's events. Add an event definition to the conductor's events array.`
			`Triggers array cannot be empty. Provide at least one trigger.`
	: // Single trigger case
		TTriggers extends InvocableTrigger
		? TaskIdentifierIsDefined<Tasks, TName, TQueue> extends true
			? TTriggers
			: `Task "${TName}" of queue "${TQueue}" is not defined in the conductor catalog. Remove { invocable: true } from triggers, or add a task definition to the conductor's tasks array.`
		: TTriggers;
// : TTriggers extends CustomEventTrigger<infer TEventName>
// 	? TEventName extends Events[number]["name"]
// 		? TTriggers
// 		: `Event "${TEventName}" is not defined in the conductor's events. Add an event definition to the conductor's events array.`
// 	: TTriggers;

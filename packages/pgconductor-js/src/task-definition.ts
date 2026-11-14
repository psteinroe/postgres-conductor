import type { StandardSchemaV1 } from "@standard-schema/spec";
import type {
	EventDefinition,
	FilterForEvent,
	FindEventByIdentifier,
	InferEventPayload,
} from "./event-definition.js";
import type { ValidateColumns } from "./select-columns.js";
import type { JsonValue, Payload } from "./database-client.js";
import { parseSchema, validateSchema } from "./lib/validate-schema.js";

type ObjectSchema = StandardSchemaV1<unknown, object>;

/** Standard Schemas of a task's signals, by signal name */
type SignalSchemas = Record<string, StandardSchemaV1>;

export type TaskDefinition<
	Name extends string,
	Payload = undefined,
	Returns = undefined,
	Queue extends string = "default",
	State = unknown,
	Signals = unknown,
> = {
	readonly name: Name;
	readonly queue: Queue;
	readonly payload: Payload;
	readonly returns: Returns;
	/** Schema (or, in `DefineTask`, type) of the execution's state; untyped without one */
	readonly state?: State;
	/**
	 * Schemas (or, in `DefineTask`, types) of the payloads of the signals the task takes, by
	 * signal name; signals of other names are untyped
	 */
	readonly signals?: Signals;
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
 *   state: { attempts: number };
 *   signals: { cancel: { reason: string } };
 * }>;
 */
export type DefineTask<
	T extends {
		name: string;
		queue?: string;
		payload?: unknown;
		returns?: unknown;
		state?: unknown;
		signals?: Record<string, unknown>;
	},
> = TaskDefinition<
	T["name"],
	T extends { payload: infer P } ? P : undefined,
	T extends { returns: infer R } ? R : undefined,
	T extends { queue: infer Q extends string } ? Q : "default",
	T extends { state: infer S } ? S : undefined,
	T extends { signals: infer S } ? S : undefined
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
	// Unset when only state or signals route a definition here
	Returns extends ObjectSchema | undefined = undefined,
	State extends ObjectSchema | undefined = undefined,
	Signals extends SignalSchemas | undefined = undefined,
>(def: {
	name: Name;
	queue: Queue;
	payload?: Payload;
	returns?: Returns;
	state?: State;
	signals?: Signals;
}): TaskDefinition<Name, Payload, Returns, Queue, State, Signals>;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
	// Unset when only state or signals route a definition here
	Returns extends ObjectSchema | undefined = undefined,
	State extends ObjectSchema | undefined = undefined,
	Signals extends SignalSchemas | undefined = undefined,
>(def: {
	name: Name;
	queue?: never;
	payload?: Payload;
	returns?: Returns;
	state?: State;
	signals?: Signals;
}): TaskDefinition<Name, Payload, Returns, "default", State, Signals>;

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
		? unknown extends R
			? object | void // Any object or nothing, like the tasks of a catalog built at runtime
			: R extends undefined
				? void
				: R extends StandardSchemaV1<any, infer O>
					? EnsureObject<O>
					: EnsureObject<R> // Plain type (type-only definition)
		: never;

/**
 * The state of the task's executions: the output of its state schema, its `DefineTask` state type,
 * or any JSON object without either.
 */
export type InferState<T> =
	T extends TaskDefinition<string, any, any, string, infer S>
		? unknown extends S
			? Payload
			: [S] extends [undefined]
				? Payload
				: S extends StandardSchemaV1<any, infer O>
					? EnsureObject<O>
					: EnsureObject<S> // Plain type (type-only definition)
		: never;

/** The input or output types of the signal schemas `S`, by signal name, or their plain types. */
type SignalTypes<S, Side extends "input" | "output"> = {
	[K in keyof S]: S[K] extends StandardSchemaV1<infer I, infer O>
		? Side extends "input"
			? I
			: O
		: S[K]; // Plain type (type-only definition)
};

/** The signals `S` a task definition `T` declares, as `Side` types; `{}` for none. */
type DeclaredSignals<T, Side extends "input" | "output"> = [T] extends [never]
	? {}
	: T extends TaskDefinition<string, any, any, string, any, infer S>
		? unknown extends S
			? {}
			: [S] extends [undefined]
				? {}
				: SignalTypes<NonNullable<S>, Side>
		: never;

/**
 * The payload types of the signals a task declares, by signal name, as its handlers receive them:
 * the output of each signal schema, or its `DefineTask` type. `{}` for a task that declares none.
 */
export type InferSignals<T> = DeclaredSignals<T, "output">;

/**
 * The payload types of the signals a task declares, by signal name, as senders pass them: the
 * input of each signal schema, so a field with a default is optional, or its `DefineTask` type.
 */
export type InferSignalInputs<T> = DeclaredSignals<T, "input">;

/**
 * The payload of a signal named `TName` of a task with the signals `TSignals` (see
 * `InferSignals`): its declared type, or `TUndeclared` for a name the task does not declare.
 */
export type SignalPayload<
	TSignals,
	TName extends string,
	TUndeclared = JsonValue,
> = string extends TName
	? TUndeclared
	: TName extends keyof TSignals
		? TSignals[TName]
		: TUndeclared;

/** The signal schemas of `task` in the catalog `definitions`, if it declares any. */
export function signalsOf(
	definitions: readonly TaskDefinition<string, unknown, unknown, string>[],
	task: { name: string; queue: string },
): unknown {
	return definitions.find(
		(definition) => definition.name === task.name && definition.queue === task.queue,
	)?.signals;
}

/**
 * Parse the payload of a signal named `name` for an execution of `task` with its schema in
 * `signals`, a task definition's signal schemas: the schema's output, or the issues. A signal
 * without a schema takes any payload as it is.
 */
export async function parseSignalPayload(
	signals: unknown,
	task: string,
	name: string,
	payload: JsonValue | undefined,
): Promise<{ value: unknown } | { error: string }> {
	const schema =
		typeof signals === "object" && signals !== null && Object.hasOwn(signals, name)
			? (signals as Record<string, unknown>)[name]
			: undefined;
	return parseSchema(
		isStandardSchema(schema) ? schema : undefined,
		payload,
		`payload of signal "${name}" for task "${task}"`,
	);
}

/**
 * Validate a state written to an execution of `task` against the task's state schema, and return
 * the schema's output. Throws on issues; a task without a state schema takes any state.
 */
export async function validateState(
	definitions: readonly TaskDefinition<string, unknown, unknown, string>[],
	task: { name: string; queue: string },
	state: unknown,
): Promise<Payload> {
	const schema = definitions.find(
		(definition) => definition.name === task.name && definition.queue === task.queue,
	)?.state;
	return (await validateSchema(
		isStandardSchema(schema) ? schema : undefined,
		state,
		`state for task "${task.name}"`,
	)) as Payload;
}

function isStandardSchema(value: unknown): value is StandardSchemaV1 {
	return typeof value === "object" && value !== null && "~standard" in value;
}

export type TaskName<TTasks extends readonly TaskDefinition<string, any, any, string>[]> =
	TTasks[number]["name"];

export type FindTaskByIdentifier<
	TTasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TQueue extends string = "default",
> = FindTask<TTasks[number], TName, TQueue>;

/**
 * The tasks whose name and queue types include `TName` and `TQueue`: the task of that literal name,
 * or every task of a catalog typed by `string` names, as one made at runtime.
 */
type FindTask<TTask, TName extends string, TQueue extends string> = TTask extends {
	name: infer N;
	queue: infer Q;
}
	? TName extends N
		? TQueue extends Q
			? TTask
			: never
		: never
	: never;

// Trigger types
export type InvocableTrigger = { invocable: true };
export type CronTrigger = { cron: string; name: string; group?: string };

// Event trigger - triggers when a custom event is emitted
export type CustomEventTrigger<
	TName extends string = string,
	TFields extends string | undefined = string | undefined,
	TFilter extends Record<string, unknown> | undefined = Record<string, unknown> | undefined,
> = {
	event: TName;
	fields?: TFields;
	filter?: TFilter;
	/** Fire only for events that no execution listens for with `ctx.listen()` */
	unlistened?: boolean;
};

export type Trigger = InvocableTrigger | CronTrigger | CustomEventTrigger;

type ValidateEventFields<Event, T> = T extends { fields: infer Fields extends string }
	? ValidateColumns<Fields, InferEventPayload<Event>> extends Fields
		? T
		: ValidateColumns<Fields, InferEventPayload<Event>>
	: T;

type ValidateEventFilter<Event, Name extends string, T> = T extends { filter: infer Filter }
	? Filter extends FilterForEvent<Event>
		? Exclude<keyof Filter, keyof FilterForEvent<Event>> extends never
			? T
			: `Filter for event "${Name}" contains undeclared fields.`
		: `Filter for event "${Name}" contains undeclared or incorrectly typed fields.`
	: T;

type ValidateCustomEventTrigger<
	Events extends readonly EventDefinition<string, any, any>[],
	T,
> = T extends { event: infer Name extends string }
	? Events extends readonly []
		? T
		: FindEventByIdentifier<Events, Name> extends infer Event
			? [Event] extends [never]
				? `Event "${Name}" is not defined in the conductor event catalog.`
				: ValidateEventFields<Event, T> extends infer FieldsResult
					? FieldsResult extends T
						? ValidateEventFilter<Event, Name, T>
						: FieldsResult
					: never
			: never
	: T;

export type ValidateEventTriggers<
	Events extends readonly EventDefinition<string, any, any>[],
	TTriggers,
> = TTriggers extends readonly any[]
	? { [K in keyof TTriggers]: ValidateCustomEventTrigger<Events, TTriggers[K]> }
	: ValidateCustomEventTrigger<Events, TTriggers>;

type UnknownTriggerKey<T> = Exclude<
	keyof T,
	keyof InvocableTrigger | keyof CronTrigger | keyof CustomEventTrigger
>;

// Task options such as removeOnComplete belong in the task definition, not in a trigger
type ValidateTriggerKeys<TTriggers> = TTriggers extends readonly any[]
	? { [K in keyof TTriggers]: ValidateTriggerKeys<TTriggers[K]> }
	: [UnknownTriggerKey<TTriggers>] extends [never]
		? TTriggers
		: `Unknown trigger option "${UnknownTriggerKey<TTriggers> & string}". Task options belong in the first argument of createTask.`;

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

// Check if triggers include a custom event.
export type HasCustomEvent<TTriggers> = TTriggers extends readonly any[]
	? Extract<TTriggers[number], { event: string }> extends never
		? false
		: true
	: TTriggers extends { event: string }
		? true
		: false;

// Non-empty array type
export type NonEmptyArray<T> = [T, ...T[]];
export type NonEmptyReadonlyArray<T> = readonly [T, ...T[]];

// Check if task identifier exists in the conductor's task catalog
type TaskIdentifierIsDefined<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TQueue extends string,
> = [FindTaskByIdentifier<Tasks, TName, TQueue>] extends [never] ? false : true;

// Validate triggers based on whether task is defined
// Rules:
// - If has invocable: true → task must be in catalog (to know payload types for invoke)
// - If task is not in catalog → must not have invocable: true
// - If has custom event trigger → event must be defined in Events schema
// - Task in catalog can have any trigger type (invocable, cron, event)
export type ValidateTriggers<
	Tasks extends readonly TaskDefinition<string, any, any, string>[],
	TName extends string,
	TTriggers,
	TQueue extends string = "default",
> = TTriggers extends readonly Trigger[]
	? // Array case - must be non-empty
		TTriggers extends NonEmptyReadonlyArray<Trigger>
		? HasInvocable<TTriggers> extends true
			? TaskIdentifierIsDefined<Tasks, TName, TQueue> extends true
				? ValidateTriggerKeys<TTriggers>
				: `Task "${TName}" of queue "${TQueue}" is not defined in the conductor catalog. Either remove { invocable: true } from triggers, or add a task definition to the conductor's tasks array.`
			: ValidateTriggerKeys<TTriggers>
		: `Triggers array cannot be empty. Provide at least one trigger.`
	: // Single trigger case
		TTriggers extends Trigger
		? TTriggers extends InvocableTrigger
			? TaskIdentifierIsDefined<Tasks, TName, TQueue> extends true
				? ValidateTriggerKeys<TTriggers>
				: `Task "${TName}" of queue "${TQueue}" is not defined in the conductor catalog. Remove { invocable: true } from triggers, or add a task definition to the conductor's tasks array.`
			: ValidateTriggerKeys<TTriggers>
		: "Invalid trigger. Use an invocable, cron, or custom event trigger.";

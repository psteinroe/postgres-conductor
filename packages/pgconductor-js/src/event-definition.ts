import type { StandardSchemaV1 } from "@standard-schema/spec";

type ObjectSchema = StandardSchemaV1<unknown, object>;
type EnsureObject<T> = T extends object ? T : {};
type SchemaOutput<T> = T extends StandardSchemaV1<any, infer O> ? O : T;
type EventPayload<T> = T extends undefined ? {} : EnsureObject<SchemaOutput<T>>;

export type EventFilterScalar = string | number | boolean | null;
type ScalarFilterValue<T> = Extract<T, EventFilterScalar>;

type EventFilterPrefix = { readonly prefix: string };
type EventFilterNumeric = {
	readonly numeric:
		| readonly [">" | ">=" | "<" | "<=", number]
		| readonly [">" | ">=" | "<" | "<=", number, ">" | ">=" | "<" | "<=", number];
};
type EventFilterExists = { readonly exists: boolean };
type EventFilterAnythingBut<T> = { readonly "anything-but": ScalarFilterValue<T> };

type EventFilterPredicate<T> =
	| ScalarFilterValue<T>
	| (Extract<T, string> extends never ? never : EventFilterPrefix)
	| (Extract<T, number> extends never ? never : EventFilterNumeric)
	| EventFilterExists
	| EventFilterAnythingBut<T>;

export type FilterableKeys<T> = {
	[K in keyof EventPayload<T> & string]: Exclude<
		EventPayload<T>[K],
		undefined
	> extends EventFilterScalar
		? Exclude<EventPayload<T>[K], undefined> extends never
			? never
			: K
		: never;
}[keyof EventPayload<T> & string];

export type EventFilter<T> = {
	readonly [K in FilterableKeys<T>]?: readonly EventFilterPredicate<EventPayload<T>[K]>[];
};

export type FilterForEvent<T> =
	T extends EventDefinition<string, infer P, infer K>
		? {
				readonly [F in K & FilterableKeys<P>]?: readonly EventFilterPredicate<EventPayload<P>[F]>[];
			}
		: never;

export type EventDefinition<
	Name extends string,
	Payload = undefined,
	Filterable extends string = never,
> = {
	readonly name: Name;
	readonly payload: Payload;
	readonly filterable?: readonly Filterable[];
};

/**
 * Type helper for defining events using pure TypeScript types (no runtime schema).
 *
 * @example
 * type AppAccountCreated = DefineEvent<{
 *   name: "app/account.created";
 *   payload: { userId: string };
 * }>;
 */
export type DefineEvent<
	T extends {
		name: string;
		payload?: unknown;
		filterable?: readonly FilterableKeys<T["payload"]>[];
	},
> = EventDefinition<
	T["name"],
	T extends { payload: infer P } ? P : undefined,
	T extends { filterable: readonly (infer K extends string)[] } ? K : never
>;

export function defineEvent<Name extends string, Payload extends ObjectSchema>(def: {
	name: Name;
	payload?: Payload;
	filterable?: undefined;
}): EventDefinition<Name, Payload>;
export function defineEvent<
	Name extends string,
	Payload extends ObjectSchema,
	const Filterable extends readonly FilterableKeys<Payload>[],
>(def: {
	name: Name;
	payload: Payload;
	filterable: Filterable;
}): EventDefinition<Name, Payload, Filterable[number]>;
export function defineEvent(def: any) {
	return def;
}

export type EventName<TEvents extends readonly EventDefinition<string, any, any>[]> =
	TEvents[number]["name"];

export type FindEventByIdentifier<
	TEvents extends readonly EventDefinition<string, any, any>[],
	TName extends string,
> = Extract<TEvents[number], { name: TName }>;

export type InferEventPayload<T> =
	T extends EventDefinition<string, infer P, any> ? EventPayload<P> : never;

export type CustomEventConfig<TName extends string> = {
	event: TName;
};

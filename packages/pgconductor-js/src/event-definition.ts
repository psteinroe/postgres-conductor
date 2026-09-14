import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { ColumnSelectionError, SelectedRow, ValidateColumns } from "./select-columns";

type ObjectSchema = StandardSchemaV1<unknown, object>;
type EnsureObject<T> = T extends object ? T : {};
type SchemaOutput<T> = T extends StandardSchemaV1<any, infer O> ? O : T;
type EventPayload<T> = T extends undefined ? {} : EnsureObject<SchemaOutput<T>>;

export type FilterableKeys<T> = keyof EventPayload<T> & string;
export type EventFilter<T> = {
	readonly [K in FilterableKeys<T>]?: readonly EventPayload<T>[K][];
};

export type FilterForEvent<T> =
	T extends EventDefinition<string, infer P, infer K>
		? { readonly [F in K & keyof EventPayload<P>]?: readonly EventPayload<P>[F][] }
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
		filterable?: readonly (keyof EnsureObject<T["payload"]> & string)[];
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

export type GenericDatabase = Record<string, Record<string, unknown>>;
export type SchemaName<TDatabase extends GenericDatabase> = keyof TDatabase;
export type TableName<
	TDatabase extends GenericDatabase,
	TSchema extends SchemaName<TDatabase>,
> = keyof TDatabase[TSchema];
export type RowType<
	TDatabase extends GenericDatabase,
	TSchema extends SchemaName<TDatabase>,
	TTable extends TableName<TDatabase, TSchema>,
> = TDatabase[TSchema][TTable];
export type DatabaseEventPayload<
	TRow,
	TOp extends "insert" | "update" | "delete",
	TSelection extends string,
> =
	SelectedRow<TRow, TSelection> extends infer Selection
		? Selection extends ColumnSelectionError<any>
			? Selection
			: {
					old: TOp extends "delete" | "update" ? Selection : null;
					new: TOp extends "insert" | "update" ? Selection : null;
					tg_table: string;
					tg_op: Uppercase<TOp>;
				}
		: never;

export type CustomEventConfig<TName extends string> = {
	event: TName;
};
export type DatabaseEventConfig<
	TDatabase extends GenericDatabase,
	TSchema extends SchemaName<TDatabase>,
	TTable extends TableName<TDatabase, TSchema>,
	TOp extends "insert" | "update" | "delete",
	TSelection extends string = string,
> = {
	schema: TSchema;
	table: TTable;
	operation: TOp;
	columns: ValidateColumns<TSelection, RowType<TDatabase, TSchema, TTable>>;
};

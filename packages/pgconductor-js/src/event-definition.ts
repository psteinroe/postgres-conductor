import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { SelectedRow, SelectionInput } from "./select-columns";

type ObjectSchema = StandardSchemaV1<unknown, object>;

export type EventDefinition<
	Name extends string,
	Payload extends ObjectSchema | undefined = undefined,
> = {
	readonly name: Name;
	readonly payload: Payload;
};

export function defineEvent<
	Name extends string,
	Queue extends string,
	Payload extends ObjectSchema,
>(def: {
	name: Name;
	payload: Payload;
}): EventDefinition<Name, Payload>;
export function defineEvent<
	Name extends string,
	Payload extends ObjectSchema,
>(def: {
	name: Name;
	payload?: Payload;
}): EventDefinition<Name, Payload>;
export function defineEvent(def: any) {
	return def;
}

export type EventName<TEvents extends readonly EventDefinition<string, any>[]> =
	TEvents[number]["name"];

export type FindEventByIdentifier<
	TEvents extends readonly EventDefinition<string, any>[],
	TName extends string,
> = Extract<TEvents[number], { name: TName }>;

type EnsureObject<T> = T extends object ? T : {};

export type InferEventPayload<T> = T extends EventDefinition<string, infer P>
	? P extends undefined
		? {}
		: P extends StandardSchemaV1<any, infer O>
			? EnsureObject<O>
			: never
	: never;

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
	TSelection extends string | undefined,
> = {
	old: TOp extends "delete" | "update" ? SelectedRow<TRow, TSelection> : null;
	new: TOp extends "insert" | "update" ? SelectedRow<TRow, TSelection> : null;
	tg_table: string;
	tg_op: Uppercase<TOp>;
};

export type SharedEventConfig = { timeout?: number };
export type CustomEventConfig<TName extends string> = {
	event: TName;
};
export type DatabaseEventConfig<
	TDatabase extends GenericDatabase,
	TSchema extends SchemaName<TDatabase>,
	TTable extends TableName<TDatabase, TSchema>,
	TOp extends "insert" | "update" | "delete",
	TSelection extends
		| SelectionInput<RowType<TDatabase, TSchema, TTable>>
		| undefined = undefined,
> = {
	schema: TSchema;
	table: TTable;
	operation: TOp;
	columns?: TSelection;
} & SharedEventConfig;

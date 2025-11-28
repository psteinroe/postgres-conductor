import type { TaskDefinition } from "./task-definition";
import type { EventDefinition, GenericDatabase } from "./event-definition";

/**
 * Stackable adapter for task definitions.
 * Supports chaining fromSchema() and fromUnion() to combine different definition styles.
 *
 * @example
 * TaskSchemas
 *   .fromSchema([emailTask, orderTask])
 *   .fromUnion<SendNotification | ProcessPayment>()
 */
export class TaskSchemas<
	TSchemaTypes extends readonly TaskDefinition<string, any, any, string>[] = readonly [],
	TUnionTypes extends TaskDefinition<string, any, any, string> = never,
> {
	private constructor(readonly definitions: TSchemaTypes) {}

	/**
	 * Create TaskSchemas from standard-schema based task definitions.
	 */
	static fromSchema<const T extends readonly TaskDefinition<string, any, any, string>[]>(
		tasks: T,
	): TaskSchemas<T, never> {
		return new TaskSchemas(tasks);
	}

	/**
	 * Add type-only task definitions via union type.
	 */
	static fromUnion<TUnion extends TaskDefinition<string, any, any, string>>(): TaskSchemas<
		readonly [],
		TUnion
	> {
		return new TaskSchemas([]);
	}

	/**
	 * Chain: Add more standard-schema based task definitions.
	 */
	fromSchema<const T extends readonly TaskDefinition<string, any, any, string>[]>(
		tasks: T,
	): TaskSchemas<readonly [...TSchemaTypes, ...T], TUnionTypes> {
		return new TaskSchemas([...this.definitions, ...tasks]);
	}

	/**
	 * Chain: Add type-only task definitions via union type.
	 */
	fromUnion<TUnion extends TaskDefinition<string, any, any, string>>(): TaskSchemas<
		TSchemaTypes,
		TUnionTypes | TUnion
	> {
		return new TaskSchemas(this.definitions);
	}
}

/**
 * Stackable adapter for event definitions.
 * Supports chaining fromSchema() and fromUnion() to combine different definition styles.
 *
 * @example
 * EventSchemas
 *   .fromSchema([orderPlaced])
 *   .fromUnion<AppAccountCreated | UserDeleted>()
 */
export class EventSchemas<
	TSchemaTypes extends readonly EventDefinition<string, any>[] = readonly [],
	TUnionTypes extends EventDefinition<string, any> = never,
> {
	private constructor(readonly definitions: TSchemaTypes) {}

	/**
	 * Create EventSchemas from standard-schema based event definitions.
	 */
	static fromSchema<const T extends readonly EventDefinition<string, any>[]>(
		events: T,
	): EventSchemas<T, never> {
		return new EventSchemas(events);
	}

	/**
	 * Add type-only event definitions via union type.
	 */
	static fromUnion<TUnion extends EventDefinition<string, any>>(): EventSchemas<
		readonly [],
		TUnion
	> {
		return new EventSchemas([]);
	}

	/**
	 * Chain: Add more standard-schema based event definitions.
	 */
	fromSchema<const T extends readonly EventDefinition<string, any>[]>(
		events: T,
	): EventSchemas<readonly [...TSchemaTypes, ...T], TUnionTypes> {
		return new EventSchemas([...this.definitions, ...events]);
	}

	/**
	 * Chain: Add type-only event definitions via union type.
	 */
	fromUnion<TUnion extends EventDefinition<string, any>>(): EventSchemas<
		TSchemaTypes,
		TUnionTypes | TUnion
	> {
		return new EventSchemas(this.definitions);
	}
}

type SupabaseDatabase = {
	[schema_name: string]: {
		Tables: {
			[table_name: string]: {
				Row: unknown;
				Insert?: unknown;
				Update?: unknown;
			};
		};
	};
};

type ConvertSupabaseTables<T> = {
	[Schema in keyof T]: T[Schema] extends { Tables: infer Tables }
		? {
				[Table in keyof Tables]: Tables[Table] extends { Row: infer Row } ? Row : unknown;
			}
		: {};
};

/**
 * Adapter for database type definitions.
 * Wraps database types to allow different sources in the future (e.g., Supabase).
 */
export class DatabaseSchema<_TDatabase extends GenericDatabase> {
	private constructor() {}

	/**
	 * Create DatabaseSchema from generated types (e.g., from pgtyped, kysely, etc.).
	 */
	static fromGeneratedTypes<T extends GenericDatabase>(): DatabaseSchema<T> {
		return new DatabaseSchema();
	}

	/**
	 * Create DatabaseSchema from Supabase generated types.
	 */
	static fromSupabaseTypes<T extends SupabaseDatabase>(): DatabaseSchema<ConvertSupabaseTables<T>> {
		return new DatabaseSchema();
	}
}

// Type helpers to extract inner types from schema adapters
export type InferTasksFromSchema<T> =
	T extends TaskSchemas<infer TSchema, infer TUnion>
		? readonly [...TSchema, TUnion]
		: readonly TaskDefinition<string, any, any, string>[];

export type InferEventsFromSchema<T> =
	T extends EventSchemas<infer TSchema, infer TUnion>
		? readonly [...TSchema, TUnion]
		: readonly EventDefinition<string, any>[];

export type InferDatabaseFromSchema<T> = T extends DatabaseSchema<infer U> ? U : GenericDatabase;

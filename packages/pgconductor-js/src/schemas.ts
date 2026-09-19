import type { TaskDefinition } from "./task-definition";
import type { EventDefinition } from "./event-definition";

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
	TSchemaTypes extends readonly EventDefinition<string, any, any>[] = readonly [],
	TUnionTypes extends EventDefinition<string, any, any> = never,
> {
	private constructor(
		readonly definitions: TSchemaTypes,
		readonly hasTypeOnlyDefinitions: boolean,
	) {}

	/**
	 * Create EventSchemas from standard-schema based event definitions.
	 */
	static fromSchema<const T extends readonly EventDefinition<string, any, any>[]>(
		events: T,
	): EventSchemas<T, never> {
		return new EventSchemas(events, false);
	}

	/**
	 * Add type-only event definitions via union type.
	 */
	static fromUnion<TUnion extends EventDefinition<string, any, any>>(): EventSchemas<
		readonly [],
		TUnion
	> {
		return new EventSchemas([], true);
	}

	/**
	 * Chain: Add more standard-schema based event definitions.
	 */
	fromSchema<const T extends readonly EventDefinition<string, any, any>[]>(
		events: T,
	): EventSchemas<readonly [...TSchemaTypes, ...T], TUnionTypes> {
		return new EventSchemas([...this.definitions, ...events], this.hasTypeOnlyDefinitions);
	}

	/**
	 * Chain: Add type-only event definitions via union type.
	 */
	fromUnion<TUnion extends EventDefinition<string, any, any>>(): EventSchemas<
		TSchemaTypes,
		TUnionTypes | TUnion
	> {
		return new EventSchemas(this.definitions, true);
	}
}

// Type helpers to extract inner types from schema adapters
export type InferTasksFromSchema<T> =
	T extends TaskSchemas<infer TSchema, infer TUnion>
		? [TUnion] extends [never]
			? TSchema
			: readonly [...TSchema, TUnion]
		: readonly [];

export type InferEventsFromSchema<T> =
	T extends EventSchemas<infer TSchema, infer TUnion>
		? [TUnion] extends [never]
			? TSchema
			: readonly [...TSchema, TUnion]
		: readonly [];

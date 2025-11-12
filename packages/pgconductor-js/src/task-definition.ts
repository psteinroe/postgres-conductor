import type { StandardSchemaV1 } from "@standard-schema/spec";

type ObjectSchema = StandardSchemaV1<unknown, object>;

export type TaskDefinition<
	Name extends string,
	Payload extends ObjectSchema | undefined = undefined,
	Returns extends ObjectSchema | undefined = undefined,
> = {
	name: Name;
	payload: Payload;
	returns: Returns;
};

export function defineTask<Name extends string>(def: {
	name: Name;
}): TaskDefinition<Name>;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
>(def: {
	name: Name;
	payload: Payload;
}): TaskDefinition<Name, Payload>;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
	Returns extends ObjectSchema,
>(def: {
	name: Name;
	payload?: Payload;
	returns?: Returns;
}): TaskDefinition<Name, Payload, Returns>;
export function defineTask(def: any) {
	return def;
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
		? {}
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

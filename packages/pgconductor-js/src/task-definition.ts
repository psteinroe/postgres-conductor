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
}): TaskDefinition<Name, undefined, undefined, typeof def.queue extends string ? typeof def.queue : "default">;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
>(def: {
	name: Name;
	queue?: string;
	payload: Payload;
}): TaskDefinition<Name, Payload, undefined, typeof def.queue extends string ? typeof def.queue : "default">;
export function defineTask<
	Name extends string,
	Payload extends ObjectSchema,
	Returns extends ObjectSchema,
>(def: {
	name: Name;
	queue?: string;
	payload?: Payload;
	returns?: Returns;
}): TaskDefinition<Name, Payload, Returns, typeof def.queue extends string ? typeof def.queue : "default">;
export function defineTask(def: any) {
	return { ...def, queue: def.queue ?? "default" };
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

export type { TaskConfig } from "./task";

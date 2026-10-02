import type { EventFilterTerm } from "./database-client";
import type { EventDefinition, EventFilterScalar } from "./event-definition";
import type { Trigger } from "./task-definition";

const MAX_FILTER_ALTERNATIVES = 4;

type NumericOperator = ">" | ">=" | "<" | "<=";
type NumericFilter =
	| readonly [NumericOperator, number]
	| readonly [NumericOperator, number, NumericOperator, number];
type EventFilterPredicate =
	| EventFilterScalar
	| { readonly prefix: string }
	| { readonly numeric: NumericFilter }
	| { readonly exists: boolean }
	| { readonly "anything-but": EventFilterScalar };
export type EventFilter = Record<string, readonly EventFilterPredicate[]>;

export type CompiledEventFilter = {
	required_field_count: number;
	terms: EventFilterTerm[];
};

export type CompiledEventTrigger = CompiledEventFilter & {
	event_key: string;
	payload_fields: string[] | null;
};

function scalarTerm(
	field: string,
	operator: "exact" | "anything_but",
	value: EventFilterScalar,
): EventFilterTerm {
	if (value === null) return { field_name: field, operator, scalar_type: "null" };
	if (typeof value === "string") {
		return { field_name: field, operator, scalar_type: "string", text_value: value };
	}
	if (typeof value === "number") {
		return { field_name: field, operator, scalar_type: "number", number_value: value };
	}
	return { field_name: field, operator, scalar_type: "boolean", boolean_value: value };
}

function numericRangeTerm(context: string, field: string, numeric: NumericFilter): EventFilterTerm {
	let lower: number | null = null;
	let lowerInclusive = false;
	let upper: number | null = null;
	let upperInclusive = false;
	const [firstOperator, firstValue, secondOperator, secondValue] = numeric;
	for (const [operator, value] of [
		[firstOperator, firstValue],
		[secondOperator, secondValue],
	] as const) {
		if (operator === undefined || value === undefined) continue;
		if (operator === ">" || operator === ">=") {
			if (lower !== null) throw new Error(`${context} has more than one lower bound`);
			lower = value;
			lowerInclusive = operator === ">=";
		} else {
			if (upper !== null) throw new Error(`${context} has more than one upper bound`);
			upper = value;
			upperInclusive = operator === "<=";
		}
	}
	if (
		lower !== null &&
		upper !== null &&
		(lower > upper || (lower === upper && !(lowerInclusive && upperInclusive)))
	) {
		throw new Error(`${context} has an empty numeric range`);
	}
	return {
		field_name: field,
		operator: "numeric_range",
		scalar_type: "number",
		lower_value: lower,
		upper_value: upper,
		lower_inclusive: lowerInclusive,
		upper_inclusive: upperInclusive,
	};
}

function predicateTerm(
	context: string,
	field: string,
	predicate: EventFilterPredicate,
): EventFilterTerm {
	if (predicate === null || typeof predicate !== "object") {
		return scalarTerm(field, "exact", predicate);
	}
	if ("prefix" in predicate) {
		return {
			field_name: field,
			operator: "prefix",
			scalar_type: "string",
			text_value: predicate.prefix,
		};
	}
	if ("numeric" in predicate) return numericRangeTerm(context, field, predicate.numeric);
	if ("exists" in predicate) {
		return { field_name: field, operator: "exists", boolean_value: predicate.exists };
	}
	return scalarTerm(field, "anything_but", predicate["anything-but"]);
}

export function compileEventFilter(
	eventName: string,
	filter: unknown,
	eventDefinitions: readonly EventDefinition<string, any, any>[],
): CompiledEventFilter {
	const definition = eventDefinitions.find((event) => event.name === eventName);
	if (!definition) {
		throw new Error(`Event "${eventName}" is not defined in the conductor event catalog`);
	}

	const fields = Object.entries((filter || {}) as EventFilter);
	const terms = fields.flatMap(([field, predicates]) => {
		const context = `Filter for event "${eventName}" field "${field}"`;
		if (!definition.filterable?.includes(field)) {
			throw new Error(`Filter for event "${eventName}" contains undeclared field "${field}"`);
		}
		if (predicates.length === 0 || predicates.length > MAX_FILTER_ALTERNATIVES) {
			throw new Error(`${context} requires between 1 and ${MAX_FILTER_ALTERNATIVES} values`);
		}
		if (
			predicates.length > 1 &&
			predicates.some(
				(predicate) =>
					typeof predicate === "object" && predicate !== null && "anything-but" in predicate,
			)
		) {
			throw new Error(`${context} cannot combine anything-but with other values`);
		}
		return predicates.map((predicate) => predicateTerm(context, field, predicate));
	});
	return { required_field_count: fields.length, terms };
}

export function compileEventTriggers(
	triggers: readonly Trigger[],
	eventDefinitions: readonly EventDefinition<string, any, any>[],
): CompiledEventTrigger[] {
	return triggers.flatMap((trigger) => {
		if (!("event" in trigger)) return [];
		if ("when" in trigger) {
			throw new Error(`Custom event "${trigger.event}" does not support a when clause`);
		}
		return [
			{
				event_key: trigger.event,
				payload_fields: trigger.fields?.split(",").map((field) => field.trim()) || null,
				...compileEventFilter(trigger.event, trigger.filter, eventDefinitions),
			},
		];
	});
}

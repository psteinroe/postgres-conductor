import { z } from "zod";
import type { EventDefinition } from "./event-definition";
import type { EventFilterTerm, JsonValue } from "./database-client";

const EVENT_FIELD_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const MAX_EVENT_NAME_BYTES = 255;
const MAX_EVENT_FIELD_BYTES = 128;
const MAX_FILTER_VALUE_BYTES = 1024;
const MAX_FILTER_FIELDS = 8;
const MAX_FILTER_ALTERNATIVES = 4;
const MAX_PREFIX_CHARACTERS = 64;

const EVENT_TRIGGER_SCHEMA = z.looseObject({
	event: z
		.string({ error: "Custom event triggers require a non-empty event name" })
		.refine(
			(value) => value.trim().length > 0,
			"Custom event triggers require a non-empty event name",
		),
	fields: z.unknown().optional(),
	filter: z.unknown().optional(),
});
const FILTER_SCALAR_SCHEMA = z.union([z.string(), z.number().finite(), z.boolean(), z.null()], {
	error: "requires one scalar value",
});
const FILTER_SCHEMA = z.record(
	z.string(),
	z
		.array(z.unknown(), { error: "must be an array" })
		.min(1, "cannot be empty")
		.max(MAX_FILTER_ALTERNATIVES, "supports at most 4 values"),
);
const NUMERIC_OPERATOR_SCHEMA = z.enum([">", ">=", "<", "<="], {
	error: "has an unsupported operator",
});
const FINITE_NUMBER_SCHEMA = z
	.number({ error: "requires finite operands" })
	.finite("requires finite operands");
const PREFIX_SCHEMA = z
	.string({ error: "must be a non-empty string" })
	.min(1, "must be a non-empty string")
	.refine((value) => Array.from(value).length <= MAX_PREFIX_CHARACTERS, "exceeds 64 characters");
const EXISTS_SCHEMA = z.boolean({ error: "must be boolean" });

type FilterScalar = z.infer<typeof FILTER_SCALAR_SCHEMA>;
export type CanonicalEventPredicate = JsonValue;
export type CanonicalEventFilter = Record<string, CanonicalEventPredicate[]>;

function parse<T>(schema: z.ZodType<T>, value: unknown, context: string): T {
	const parsed = schema.safeParse(value);
	if (parsed.success) return parsed.data;
	const message = parsed.error.issues[0]?.message || "is invalid";
	throw new Error(`${context} ${message}`);
}

function utf8ByteLength(value: string): number {
	return new TextEncoder().encode(value).length;
}

function assertFieldName(field: string, eventName: string): void {
	if (!EVENT_FIELD_PATTERN.test(field)) {
		throw new Error(`Fields for event "${eventName}" contains invalid field "${field}"`);
	}
	if (utf8ByteLength(field) > MAX_EVENT_FIELD_BYTES) {
		throw new Error(`Field "${field}" for event "${eventName}" exceeds 128 UTF-8 bytes`);
	}
}

export function parseEventPayloadFields(fields: unknown, eventName: string): string[] | null {
	if (fields === undefined) return null;
	const value = parse(
		z.string({ error: "must be a comma-separated string" }),
		fields,
		`Fields for event "${eventName}"`,
	);
	const selected = value.split(",").map((field) => field.trim());
	if (selected.some((field) => field.length === 0)) {
		throw new Error(`Fields for event "${eventName}" cannot contain empty names`);
	}
	for (const field of selected) assertFieldName(field, eventName);
	if (new Set(selected).size !== selected.length) {
		throw new Error(`Fields for event "${eventName}" cannot contain duplicate names`);
	}
	return selected;
}

function scalarSortKey(value: FilterScalar): string {
	if (value === null) return "0:null";
	if (typeof value === "boolean") return `1:${value ? "true" : "false"}`;
	if (typeof value === "number") return `2:${JSON.stringify(value)}`;
	return `3:${JSON.stringify(value)}`;
}

function assertScalarSize(value: FilterScalar, eventName: string, field: string): void {
	if (utf8ByteLength(JSON.stringify(value)) > MAX_FILTER_VALUE_BYTES) {
		throw new Error(
			`Filter value for event "${eventName}" field "${field}" exceeds 1024 UTF-8 bytes`,
		);
	}
}

function canonicalNumericRange(
	value: unknown,
	eventName: string,
	field: string,
): CanonicalEventPredicate {
	const context = `Numeric filter for event "${eventName}" field "${field}"`;
	const values = parse(
		z.array(z.unknown()).refine((items) => items.length === 2 || items.length === 4, {
			message: "must contain one or two operator/value pairs",
		}),
		value,
		context,
	);

	let lower: number | null = null;
	let lowerInclusive = false;
	let upper: number | null = null;
	let upperInclusive = false;
	for (let index = 0; index < values.length; index += 2) {
		const operator = parse(NUMERIC_OPERATOR_SCHEMA, values[index], context);
		const operand = parse(FINITE_NUMBER_SCHEMA, values[index + 1], context);
		if (operator === ">" || operator === ">=") {
			if (lower !== null) throw new Error(`${context} contains duplicate lower bounds`);
			lower = operand;
			lowerInclusive = operator === ">=";
		} else {
			if (upper !== null) throw new Error(`${context} contains duplicate upper bounds`);
			upper = operand;
			upperInclusive = operator === "<=";
		}
	}
	if (
		lower !== null &&
		upper !== null &&
		(lower > upper || (lower === upper && (!lowerInclusive || !upperInclusive)))
	) {
		throw new Error(`${context} is empty`);
	}
	return { $operator: "numeric_range", lower, lowerInclusive, upper, upperInclusive };
}

function canonicalPredicate(
	value: unknown,
	eventName: string,
	field: string,
): CanonicalEventPredicate {
	const scalar = FILTER_SCALAR_SCHEMA.safeParse(value);
	if (scalar.success) {
		assertScalarSize(scalar.data, eventName, field);
		return scalar.data;
	}

	const context = `Filter for event "${eventName}" field "${field}"`;
	const operator = parse(z.record(z.string(), z.unknown()), value, context);
	const entries = Object.entries(operator);
	if (entries.length !== 1) {
		throw new Error(`${context} operator must contain exactly one key`);
	}
	const entry = entries[0];
	if (!entry) throw new Error(`${context} operator must contain exactly one key`);
	const [name, operand] = entry;

	switch (name) {
		case "prefix": {
			const prefix = parse(
				PREFIX_SCHEMA,
				operand,
				`Prefix filter for event "${eventName}" field "${field}"`,
			);
			assertScalarSize(prefix, eventName, field);
			return { $operator: "prefix", value: prefix };
		}
		case "numeric":
			return canonicalNumericRange(operand, eventName, field);
		case "exists":
			return {
				$operator: "exists",
				value: parse(
					EXISTS_SCHEMA,
					operand,
					`Exists filter for event "${eventName}" field "${field}"`,
				),
			};
		case "anything-but": {
			const scalarOperand = parse(
				FILTER_SCALAR_SCHEMA,
				operand,
				`Anything-but filter for event "${eventName}" field "${field}"`,
			);
			assertScalarSize(scalarOperand, eventName, field);
			return { $operator: "anything_but", value: scalarOperand };
		}
		default:
			throw new Error(`${context} uses unsupported operator "${name}"`);
	}
}

function predicateSortKey(value: CanonicalEventPredicate): string {
	const scalar = FILTER_SCALAR_SCHEMA.safeParse(value);
	return scalar.success ? `0:${scalarSortKey(scalar.data)}` : `1:${JSON.stringify(value)}`;
}

function canonicalFilter(
	filter: unknown,
	eventName: string,
	definition: EventDefinition<string, any, any> | undefined,
	allowUnknownEvents: boolean,
): CanonicalEventFilter | null {
	if (filter === undefined || filter === null) return null;
	parse(FILTER_SCHEMA, filter, `Filter for event "${eventName}"`);
	const entries = Object.entries(filter as Record<string, unknown[]>);
	if (entries.length > MAX_FILTER_FIELDS) {
		throw new Error(`Filter for event "${eventName}" supports at most 8 fields`);
	}
	if (!definition && !allowUnknownEvents) {
		throw new Error(`Filtered event "${eventName}" has no runtime event definition`);
	}

	const allowed = definition ? new Set(definition.filterable || []) : null;
	const canonical: CanonicalEventFilter = Object.create(null) as CanonicalEventFilter;
	for (const [field, values] of entries.sort(([left], [right]) => left.localeCompare(right))) {
		if (field.trim().length === 0) {
			throw new Error(`Filter for event "${eventName}" cannot contain empty field names`);
		}
		if (utf8ByteLength(field) > MAX_EVENT_FIELD_BYTES) {
			throw new Error(`Filter field "${field}" for event "${eventName}" exceeds 128 UTF-8 bytes`);
		}
		if (allowed && !allowed.has(field)) {
			throw new Error(`Filter for event "${eventName}" contains undeclared field "${field}"`);
		}

		const predicates = values.map((value) => canonicalPredicate(value, eventName, field));
		if (
			predicates.length > 1 &&
			predicates.some(
				(predicate) =>
					typeof predicate === "object" &&
					predicate !== null &&
					!Array.isArray(predicate) &&
					predicate.$operator === "anything_but",
			)
		) {
			throw new Error(
				`Anything-but filter for event "${eventName}" field "${field}" must be atomic`,
			);
		}
		const unique = new Map(predicates.map((predicate) => [predicateSortKey(predicate), predicate]));
		canonical[field] = [...unique.entries()]
			.sort(([left], [right]) => left.localeCompare(right))
			.map(([, predicate]) => predicate);
	}
	return canonical;
}

function scalarTerm(
	field: string,
	operator: "exact" | "anything_but",
	value: FilterScalar,
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

function termForPredicate(field: string, predicate: CanonicalEventPredicate): EventFilterTerm {
	const scalar = FILTER_SCALAR_SCHEMA.safeParse(predicate);
	if (scalar.success) return scalarTerm(field, "exact", scalar.data);

	const operator = predicate as Record<string, JsonValue>;
	switch (operator.$operator) {
		case "prefix":
			return {
				field_name: field,
				operator: "prefix",
				scalar_type: "string",
				text_value: operator.value as string,
			};
		case "numeric_range":
			return {
				field_name: field,
				operator: "numeric_range",
				scalar_type: "number",
				lower_value: operator.lower as number | null,
				upper_value: operator.upper as number | null,
				lower_inclusive: operator.lowerInclusive as boolean,
				upper_inclusive: operator.upperInclusive as boolean,
			};
		case "exists":
			return {
				field_name: field,
				operator: "exists",
				boolean_value: operator.value as boolean,
			};
		case "anything_but":
			return scalarTerm(field, "anything_but", operator.value as FilterScalar);
		default:
			throw new Error(`Unsupported canonical filter operator for field "${field}"`);
	}
}

export function compileEventFilterTerms(filter: CanonicalEventFilter | null): EventFilterTerm[] {
	if (!filter) return [];
	return Object.entries(filter).flatMap(([field, predicates]) =>
		predicates.map((predicate) => termForPredicate(field, predicate)),
	);
}

export type CompiledEventTrigger = {
	event_key: string;
	payload_fields: string[] | null;
	filter: CanonicalEventFilter | null;
	required_field_count: number;
	terms: EventFilterTerm[];
};

/** Validate and canonicalize one trigger. Non-event triggers return null. */
export function compileEventTrigger(
	trigger: object,
	eventDefinitions: readonly EventDefinition<string, any, any>[],
	allowUnknownEvents = false,
): CompiledEventTrigger | null {
	if (!("event" in trigger)) return null;
	const candidate = parse(EVENT_TRIGGER_SCHEMA, trigger, "Custom event trigger");
	const eventName = candidate.event;
	if (utf8ByteLength(eventName) > MAX_EVENT_NAME_BYTES) {
		throw new Error(`Event "${eventName}" exceeds 255 UTF-8 bytes`);
	}
	if ("when" in trigger) {
		throw new Error(`Custom event "${eventName}" does not support a when clause`);
	}

	const definition = eventDefinitions.find((event) => event.name === eventName);
	if (eventDefinitions.length > 0 && !definition && !allowUnknownEvents) {
		throw new Error(`Event "${eventName}" is not defined in the conductor event catalog`);
	}
	const filter = canonicalFilter(candidate.filter, eventName, definition, allowUnknownEvents);
	return {
		event_key: eventName,
		payload_fields: parseEventPayloadFields(candidate.fields, eventName),
		filter,
		required_field_count: filter ? Object.keys(filter).length : 0,
		terms: compileEventFilterTerms(filter),
	};
}

export function compileEventTriggers(
	triggers: object | readonly object[],
	eventDefinitions: readonly EventDefinition<string, any, any>[],
	allowUnknownEvents = false,
): CompiledEventTrigger[] {
	const list = Array.isArray(triggers) ? triggers : [triggers];
	return list.flatMap((trigger) => {
		const compiled = compileEventTrigger(trigger, eventDefinitions, allowUnknownEvents);
		return compiled ? [compiled] : [];
	});
}

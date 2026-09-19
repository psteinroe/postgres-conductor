import type { EventDefinition } from "./event-definition";
import type { EventSubscriptionSpec, JsonValue } from "./database-client";

const EVENT_FIELD_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const MAX_EVENT_NAME_BYTES = 255;
const MAX_EVENT_FIELD_BYTES = 128;
const MAX_FILTER_VALUE_BYTES = 1024;
const MAX_FILTER_FIELDS = 8;
const MAX_FILTER_ALTERNATIVES = 4;

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
	if (typeof fields !== "string") {
		throw new Error(`Fields for event "${eventName}" must be a comma-separated string`);
	}

	const selected = fields.split(",").map((field) => field.trim());
	if (selected.some((field) => field.length === 0)) {
		throw new Error(`Fields for event "${eventName}" cannot contain empty names`);
	}
	for (const field of selected) {
		assertFieldName(field, eventName);
	}
	if (new Set(selected).size !== selected.length) {
		throw new Error(`Fields for event "${eventName}" cannot contain duplicate names`);
	}
	return selected;
}

function isEventFilterScalar(value: unknown): value is string | number | boolean | null {
	return (
		value === null ||
		typeof value === "string" ||
		typeof value === "boolean" ||
		(typeof value === "number" && Number.isFinite(value))
	);
}

function scalarSortKey(value: string | number | boolean | null): string {
	if (value === null) return "0:null";
	if (typeof value === "boolean") return `1:${value ? "true" : "false"}`;
	if (typeof value === "number") return `2:${JSON.stringify(value)}`;
	return `3:${JSON.stringify(value)}`;
}

function scalarByteLength(value: string | number | boolean | null): number {
	return utf8ByteLength(JSON.stringify(value));
}

function canonicalFilter(
	filter: unknown,
	eventName: string,
	definition: EventDefinition<string, any, any> | undefined,
	allowUnknownEvents: boolean,
): Record<string, JsonValue[]> | null {
	if (filter === undefined || filter === null) return null;
	if (typeof filter !== "object" || Array.isArray(filter)) {
		throw new Error(`Filter for event "${eventName}" must be an object`);
	}

	const filterEntries = Object.entries(filter as Record<string, unknown>);
	if (filterEntries.length > MAX_FILTER_FIELDS) {
		throw new Error(`Filter for event "${eventName}" supports at most 8 fields`);
	}
	if (!definition && !allowUnknownEvents) {
		throw new Error(`Filtered event "${eventName}" has no runtime event definition`);
	}

	const allowed = definition ? new Set(definition.filterable ?? []) : undefined;
	const canonical: Record<string, JsonValue[]> = {};
	for (const [field, values] of filterEntries.sort(([left], [right]) =>
		left < right ? -1 : left > right ? 1 : 0,
	)) {
		if (utf8ByteLength(field) === 0) {
			throw new Error(`Filter for event "${eventName}" cannot contain empty field names`);
		}
		if (utf8ByteLength(field) > MAX_EVENT_FIELD_BYTES) {
			throw new Error(`Filter field "${field}" for event "${eventName}" exceeds 128 UTF-8 bytes`);
		}
		if (allowed && !allowed.has(field)) {
			throw new Error(`Filter for event "${eventName}" contains undeclared field "${field}"`);
		}
		if (!Array.isArray(values)) {
			throw new Error(`Filter value for event "${eventName}" field "${field}" must be an array`);
		}
		if (values.length === 0) {
			throw new Error(`Filter value for event "${eventName}" field "${field}" cannot be empty`);
		}
		if (values.length > MAX_FILTER_ALTERNATIVES) {
			throw new Error(
				`Filter value for event "${eventName}" field "${field}" supports at most 4 values`,
			);
		}

		const byKey = new Map<string, string | number | boolean | null>();
		for (const value of values) {
			if (!isEventFilterScalar(value)) {
				throw new Error(
					`Filter value for event "${eventName}" field "${field}" must contain only scalar values`,
				);
			}
			if (scalarByteLength(value) > MAX_FILTER_VALUE_BYTES) {
				throw new Error(
					`Filter value for event "${eventName}" field "${field}" exceeds 1024 UTF-8 bytes`,
				);
			}
			byKey.set(scalarSortKey(value), value);
		}
		canonical[field] = [...byKey.entries()]
			.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
			.map(([, value]) => value);
	}
	return canonical;
}

export type CompiledEventTrigger = Pick<
	EventSubscriptionSpec,
	"event_key" | "payload_fields" | "filter"
>;

/** Validate and canonicalize one trigger. Non-event triggers return null. */
export function compileEventTrigger(
	trigger: object,
	eventDefinitions: readonly EventDefinition<string, any, any>[],
	allowUnknownEvents = false,
): CompiledEventTrigger | null {
	if (!("event" in trigger)) return null;
	const candidate = trigger as Record<string, unknown>;
	if (typeof candidate.event !== "string" || candidate.event.trim().length === 0) {
		throw new Error("Custom event triggers require a non-empty event name");
	}

	const eventName = candidate.event;
	if (utf8ByteLength(eventName) > MAX_EVENT_NAME_BYTES) {
		throw new Error(`Event "${eventName}" exceeds 255 UTF-8 bytes`);
	}
	if ("when" in candidate) {
		throw new Error(`Custom event "${eventName}" does not support a when clause`);
	}

	const definition = eventDefinitions.find((event) => event.name === eventName);
	if (eventDefinitions.length > 0 && !definition && !allowUnknownEvents) {
		throw new Error(`Event "${eventName}" is not defined in the conductor event catalog`);
	}

	return {
		event_key: eventName,
		payload_fields: parseEventPayloadFields(candidate.fields, eventName),
		filter: canonicalFilter(candidate.filter, eventName, definition, allowUnknownEvents),
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

export function validateEventTriggers(
	triggers: object | readonly object[],
	eventDefinitions: readonly EventDefinition<string, any, any>[],
	allowUnknownEvents = false,
): void {
	compileEventTriggers(triggers, eventDefinitions, allowUnknownEvents);
}

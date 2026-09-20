import type { JsonValue, Payload } from "./database-client";

export type EventDispatchCandidate = {
	event_id: string;
	event_key: string;
	event_payload: Payload;
	completed: boolean;
	subscription_id: string | null;
	task_key: string | null;
	queue: string | null;
	payload_fields: string[] | null;
	filter: Record<string, JsonValue[]> | null;
};

export type EventDispatchDestination = {
	eventId: string;
	subscriptionId: string;
	taskKey: string;
	queue: string;
	payload: Payload;
};

function isScalar(value: unknown): value is string | number | boolean | null {
	return value === null || ["string", "number", "boolean"].includes(typeof value);
}

function exactMatch(left: unknown, right: unknown): boolean {
	return isScalar(left) && isScalar(right) && typeof left === typeof right && left === right;
}

function predicateMatches(present: boolean, value: unknown, predicate: JsonValue): boolean {
	if (isScalar(predicate)) return present && exactMatch(value, predicate);
	if (Array.isArray(predicate)) return false;

	switch (predicate.$operator) {
		case "prefix":
			return (
				present &&
				typeof value === "string" &&
				typeof predicate.value === "string" &&
				value.startsWith(predicate.value)
			);
		case "numeric_range": {
			if (!present || typeof value !== "number") return false;
			const lower = predicate.lower as number | null;
			const upper = predicate.upper as number | null;
			return (
				(lower === null || (predicate.lowerInclusive === true ? value >= lower : value > lower)) &&
				(upper === null || (predicate.upperInclusive === true ? value <= upper : value < upper))
			);
		}
		case "exists":
			return present === predicate.value;
		case "anything_but":
			return present && isScalar(value) && !exactMatch(value, predicate.value);
		default:
			return false;
	}
}

export function eventFilterMatches(
	payload: Payload,
	filter: Record<string, JsonValue[]> | null,
): boolean {
	if (!filter) return true;
	return Object.entries(filter).every(([field, predicates]) => {
		const present = Object.hasOwn(payload, field);
		const value = payload[field];
		return predicates.some((predicate) => predicateMatches(present, value, predicate));
	});
}

function projectPayload(payload: Payload, fields: string[] | null): Payload {
	if (!fields) return payload;
	return Object.fromEntries(
		fields.filter((field) => Object.hasOwn(payload, field)).map((field) => [field, payload[field]]),
	) as Payload;
}

export function buildEventDispatchDestinations(
	candidates: EventDispatchCandidate[],
): EventDispatchDestination[] {
	return candidates.flatMap((candidate) => {
		if (
			candidate.completed ||
			!candidate.subscription_id ||
			!candidate.task_key ||
			!candidate.queue ||
			!eventFilterMatches(candidate.event_payload, candidate.filter)
		) {
			return [];
		}
		return [
			{
				eventId: candidate.event_id,
				subscriptionId: candidate.subscription_id,
				taskKey: candidate.task_key,
				queue: candidate.queue,
				payload: {
					event: candidate.event_key,
					payload: projectPayload(candidate.event_payload, candidate.payload_fields),
				},
			},
		];
	});
}

import type { Payload } from "./database-client";

export type EventDispatchSource = {
	event_id: string;
	event_key: string;
	event_payload: Payload;
};

export type EventDispatchCandidate = EventDispatchSource & {
	subscription_id: string;
	task_key: string;
	queue: string;
	payload_fields: string[] | null;
	group_number: number;
	clause_number: number | null;
	field_name: string | null;
	operator: "exact" | "prefix" | "numeric_range" | "exists" | "anything_but" | null;
	scalar_type: "string" | "number" | "boolean" | "null" | null;
	text_value: string | null;
	number_value: number | null;
	boolean_value: boolean | null;
	lower_value: number | null;
	upper_value: number | null;
	lower_inclusive: boolean | null;
	upper_inclusive: boolean | null;
};

export type EventDispatchDestination = {
	eventId: string;
	subscriptionId: string;
	taskKey: string;
	queue: string;
	payload: Payload;
};

type ScalarType = "string" | "number" | "boolean" | "null";

function scalarType(value: unknown): ScalarType | null {
	if (value === null) return "null";
	switch (typeof value) {
		case "string":
			return "string";
		case "number":
			return "number";
		case "boolean":
			return "boolean";
		default:
			return null;
	}
}

function exactMatch(candidate: EventDispatchCandidate, value: unknown): boolean {
	if (scalarType(value) !== candidate.scalar_type) return false;
	switch (candidate.scalar_type) {
		case "string":
			return value === candidate.text_value;
		case "number":
			return value === candidate.number_value;
		case "boolean":
			return value === candidate.boolean_value;
		case "null":
			return true;
		default:
			return false;
	}
}

function predicateMatches(candidate: EventDispatchCandidate): boolean {
	if (!candidate.field_name || !candidate.operator) return false;
	const present = Object.hasOwn(candidate.event_payload, candidate.field_name);
	const value = candidate.event_payload[candidate.field_name];

	switch (candidate.operator) {
		case "exact":
			return present && exactMatch(candidate, value);
		case "prefix":
			return present && typeof value === "string" && value.startsWith(candidate.text_value || "");
		case "numeric_range":
			return (
				present &&
				typeof value === "number" &&
				(candidate.lower_value === null ||
					(candidate.lower_inclusive
						? value >= candidate.lower_value
						: value > candidate.lower_value)) &&
				(candidate.upper_value === null ||
					(candidate.upper_inclusive
						? value <= candidate.upper_value
						: value < candidate.upper_value))
			);
		case "exists":
			return present === candidate.boolean_value;
		case "anything_but":
			return present && scalarType(value) !== null && !exactMatch(candidate, value);
	}
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
	const subscriptions = new Map<
		string,
		{
			candidate: EventDispatchCandidate;
			groups: Map<number, Map<number, EventDispatchCandidate[]>>;
		}
	>();

	for (const candidate of candidates) {
		const key = `${candidate.event_id}:${candidate.subscription_id}`;
		let subscription = subscriptions.get(key);
		if (!subscription) {
			subscription = { candidate, groups: new Map() };
			subscriptions.set(key, subscription);
		}
		let clauses = subscription.groups.get(candidate.group_number);
		if (!clauses) {
			clauses = new Map();
			subscription.groups.set(candidate.group_number, clauses);
		}
		if (candidate.clause_number === null) continue;
		const predicates = clauses.get(candidate.clause_number) || [];
		predicates.push(candidate);
		clauses.set(candidate.clause_number, predicates);
	}

	const destinations: EventDispatchDestination[] = [];
	for (const { candidate, groups } of subscriptions.values()) {
		const matches = [...groups.values()].some((clauses) =>
			[...clauses.values()].every((predicates) => predicates.some(predicateMatches)),
		);
		if (!matches) continue;

		destinations.push({
			eventId: candidate.event_id,
			subscriptionId: candidate.subscription_id,
			taskKey: candidate.task_key,
			queue: candidate.queue,
			payload: {
				event: candidate.event_key,
				payload: projectPayload(candidate.event_payload, candidate.payload_fields),
			},
		});
	}
	return destinations;
}

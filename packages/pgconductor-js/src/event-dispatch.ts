import type { EventFilterTerm, Payload } from "./database-client";

function termScalarValue(term: EventFilterTerm): string | number | boolean | null | undefined {
	switch (term.scalar_type) {
		case "string":
			return term.text_value;
		case "number":
			return term.number_value;
		case "boolean":
			return term.boolean_value;
		case "null":
			return null;
		default:
			return undefined;
	}
}

function exactMatch(value: unknown, term: EventFilterTerm): boolean {
	const expected = termScalarValue(term);
	return expected !== undefined && typeof value === typeof expected && value === expected;
}

function termMatches(payload: Payload, term: EventFilterTerm): boolean {
	const present = Object.hasOwn(payload, term.field_name);
	const value = payload[term.field_name];

	switch (term.operator) {
		case "exact":
			return present && exactMatch(value, term);
		case "prefix":
			return (
				present &&
				typeof value === "string" &&
				typeof term.text_value === "string" &&
				value.startsWith(term.text_value)
			);
		case "numeric_range":
			return (
				present &&
				typeof value === "number" &&
				(term.lower_value === null ||
					term.lower_value === undefined ||
					(term.lower_inclusive ? value >= term.lower_value : value > term.lower_value)) &&
				(term.upper_value === null ||
					term.upper_value === undefined ||
					(term.upper_inclusive ? value <= term.upper_value : value < term.upper_value))
			);
		case "exists":
			return present === term.boolean_value;
		case "anything_but":
			return (
				present &&
				(value === null || ["string", "number", "boolean"].includes(typeof value)) &&
				!exactMatch(value, term)
			);
	}
}

/** In-memory parity helper. Production matching is performed set-wise in SQL. */
export function eventFilterTermsMatch(
	payload: Payload,
	requiredFieldCount: number,
	terms: EventFilterTerm[],
): boolean {
	if (requiredFieldCount === 0) return true;
	const matchedFields = new Set<string>();
	for (const term of terms) {
		if (termMatches(payload, term)) matchedFields.add(term.field_name);
	}
	return matchedFields.size === requiredFieldCount;
}

export type DurationUnit = "ms" | "s" | "m" | "h" | "d";
export type DurationInput = number | `${number}${DurationUnit}`;

const DURATION_UNITS: Record<DurationUnit, number> = {
	ms: 1,
	s: 1_000,
	m: 60_000,
	h: 3_600_000,
	d: 86_400_000,
};

function isDurationUnit(value: string): value is DurationUnit {
	return value in DURATION_UNITS;
}

/** Parse a non-negative duration into integer milliseconds. */
export function parseDuration(value: DurationInput): number {
	if (typeof value === "number") {
		if (!Number.isFinite(value) || value < 0) {
			throw new Error("duration must be a non-negative finite number");
		}
		const milliseconds = Math.trunc(value);
		if (!Number.isSafeInteger(milliseconds)) {
			throw new Error("duration is too large");
		}
		return milliseconds;
	}

	const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/.exec(value);
	if (!match) {
		throw new Error(`invalid duration: ${value}`);
	}

	const unit = match[2];
	if (!unit || !isDurationUnit(unit)) {
		throw new Error(`invalid duration: ${value}`);
	}
	const milliseconds = Number(match[1]) * DURATION_UNITS[unit];
	if (!Number.isSafeInteger(milliseconds)) {
		throw new Error(`duration is too large: ${value}`);
	}
	return milliseconds;
}

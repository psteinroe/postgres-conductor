export function assertOk<T = unknown>(
	value: T,
	error?: string,
): NonNullable<T> {
	if (!value) {
		throw new Error(error || "Value is not defined");
	}
	return value as NonNullable<T>;
}

function assertEqual<T>(actual: T, expected: T, message?: string): void {
	if (actual !== expected) {
		throw new Error(
			message || `Assertion failed: expected ${expected}, got ${actual}`,
		);
	}
}

export default {
	ok: assertOk,
	eq: assertEqual,
};

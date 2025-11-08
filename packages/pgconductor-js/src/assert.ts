export function assertOk(value: unknown | undefined | null, error?: string) {
	if (value) {
		return;
	}

	throw new Error(error || "Value is not defined");
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

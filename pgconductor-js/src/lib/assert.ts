export function ok(condition: any, msg?: string): asserts condition {
	if (!condition) {
		throw new Error(msg || "Assertion failed");
	}
}

export function equal<T>(actual: T, expected: T, message?: string): void {
	if (actual !== expected) {
		throw new Error(
			message || `Assertion failed: expected ${expected}, got ${actual}`,
		);
	}
}

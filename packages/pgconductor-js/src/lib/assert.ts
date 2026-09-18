export function ok(condition: any, msg?: string): asserts condition {
	if (!condition) {
		throw new Error(msg || "Assertion failed");
	}
}

export function equal<T>(actual: T, expected: T, message?: string): void {
	if (actual !== expected) {
		throw new Error(message || `Assertion failed: expected ${expected}, got ${actual}`);
	}
}

export function never(x: never): never {
	throw new Error(`Unhandled case: ${x}`);
}

export function positiveInteger(value: number | undefined, name: string): number | undefined {
	if (value !== undefined) {
		ok(Number.isInteger(value) && value > 0, `${name} must be a positive integer`);
	}
	return value;
}

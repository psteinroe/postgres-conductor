/**
 * All kinds of arguments can come through
 *
 * Examples seen are
 * - string
 * - object / hash
 * - values used for string interpolation, basically anything
 */
export type LogArg = unknown;

/**
 * Based on https://datatracker.ietf.org/doc/html/rfc5424#autoid-11
 * it's pretty reasonable to expect a logger to have the following interfaces
 * available.
 */
export interface Logger {
	info(...args: LogArg[]): void;
	warn(...args: LogArg[]): void;
	error(...args: LogArg[]): void;
	debug(...args: LogArg[]): void;
}

export class DefaultLogger implements Logger {
	info(...args: LogArg[]) {
		console.info(...args);
	}

	warn(...args: LogArg[]) {
		console.warn(...args);
	}

	error(...args: LogArg[]) {
		console.error(...args);
	}

	debug(...args: LogArg[]) {
		console.debug(...args);
	}
}

export function makeChildLogger(parent: Logger, metadata: Record<string, unknown>): Logger {
	let child = parent;
	try {
		if ("child" in parent && typeof parent.child === "function") {
			child = parent.child(metadata);
		}
	} catch {
		// ignore
	}

	return child;
}

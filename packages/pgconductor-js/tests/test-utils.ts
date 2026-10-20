import { setTimeout as sleep } from "node:timers/promises";
import type { Sql } from "postgres";

export async function waitForCondition(
	condition: () => boolean | Promise<boolean>,
	timeoutMs = 20_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!(await condition())) {
		if (Date.now() >= deadline) {
			throw new Error(`condition was not met within ${timeoutMs}ms`);
		}
		await sleep(25);
	}
}

// Runs the first statement whose text or parameters contain `marker`, waits for `beforeLoss`,
// then fails it as if the connection dropped after the commit but before the response arrived.
// Another `code` fails it with an error the client does not retry.
export function loseFirstResponse(
	sql: Sql,
	marker: string,
	{ beforeLoss, code = "ECONNRESET" }: { beforeLoss?: () => Promise<void>; code?: string } = {},
): Sql {
	let lost = false;
	return new Proxy(sql, {
		apply(target, thisArg, args) {
			const query = Reflect.apply(target, thisArg, args);
			if (lost || !args.map(String).join().includes(marker)) return query;
			lost = true;
			return query.then(async () => {
				await beforeLoss?.();
				throw Object.assign(new Error(`lost response (${code})`), { code });
			});
		},
	});
}

// Fails statements containing `marker` before they run while `fail` returns an error
export function failStatements(sql: Sql, marker: string, fail: () => Error | null): Sql {
	return new Proxy(sql, {
		apply(target, thisArg, args) {
			const query = Reflect.apply(target, thisArg, args);
			if (!String(args[0]).includes(marker)) return query;
			const error = fail();
			return error ? Promise.reject(error) : query;
		},
	});
}

// Error shaped like a dropped connection, which DatabaseClient retries when given a signal
export function connectionError(code = "ECONNRESET"): Error {
	return Object.assign(new Error(`injected ${code}`), { code });
}

// Resolves "timeout" if the promise does not settle within ms
export function within(promise: Promise<unknown>, ms: number): Promise<"done" | "timeout"> {
	return Promise.race([
		promise.then(
			() => "done" as const,
			() => "done" as const,
		),
		sleep(ms).then(() => "timeout" as const),
	]);
}

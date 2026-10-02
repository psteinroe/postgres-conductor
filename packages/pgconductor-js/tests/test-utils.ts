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
		await Bun.sleep(25);
	}
}

// Runs the first statement containing `marker`, then fails it as if the
// connection dropped after the commit but before the response arrived.
export function loseFirstResponse(sql: Sql, marker: string): Sql {
	let lost = false;
	return new Proxy(sql, {
		apply(target, thisArg, args) {
			const query = Reflect.apply(target, thisArg, args);
			if (lost || !String(args[0]).includes(marker)) return query;
			lost = true;
			return query.then(() => {
				throw Object.assign(new Error("connection reset"), { code: "ECONNRESET" });
			});
		},
	});
}

import { describe, expect, test } from "vitest";
import { AsyncQueue } from "../../src/lib/async-queue";
import { Deferred } from "../../src/lib/deferred";
import { mapConcurrent } from "../../src/lib/map-concurrent";

async function collect<R>(results: AsyncGenerator<R>): Promise<R[]> {
	const out: R[] = [];
	for await (const result of results) out.push(result);
	return out;
}

describe("mapConcurrent", () => {
	test("runs at most `limit` mappers and yields results as they settle", async () => {
		const queue = new AsyncQueue<number>(10);
		let running = 0;
		let peak = 0;
		const gates = [0, 1, 2, 3].map(() => new Deferred<void>());
		const results = collect(
			mapConcurrent(queue, 2, async (n) => {
				peak = Math.max(peak, ++running);
				await gates[n]?.promise;
				running--;
				return n;
			}),
		);
		for (const n of [0, 1, 2, 3]) await queue.push(n);
		queue.close();
		gates[1]?.resolve();
		gates[0]?.resolve();
		gates[3]?.resolve();
		gates[2]?.resolve();

		const yielded = await results;
		// 1 settled first; 2 and 3 started once slots freed
		expect(yielded[0]).toBe(1);
		expect([...yielded].sort()).toEqual([0, 1, 2, 3]);
		expect(peak).toBe(2);
	});

	test("starts items pushed while a long mapper runs", async () => {
		const queue = new AsyncQueue<string>(10);
		const long = new Deferred<void>();
		const seen: string[] = [];
		const results = collect(
			mapConcurrent(queue, 4, async (item) => {
				if (item === "long") await long.promise;
				seen.push(item);
				return item;
			}),
		);
		await queue.push("long");
		for (let i = 0; i < 100; i++) await queue.push(`short-${i}`);
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(seen).toHaveLength(100);
		long.resolve();
		queue.close();
		expect(await results).toHaveLength(101);
	});

	test("a rejected mapper throws once every other mapper settled", async () => {
		const queue = new AsyncQueue<number>(10);
		const slow = new Deferred<void>();
		let slowDone = false;
		const results = collect(
			mapConcurrent(queue, 2, async (n) => {
				if (n === 0) throw new Error("boom");
				await slow.promise;
				slowDone = true;
				return n;
			}),
		);
		await queue.push(1);
		await queue.push(0);
		queue.close();
		setTimeout(() => slow.resolve(), 10);

		await expect(results).rejects.toThrow("boom");
		expect(slowDone).toBe(true);
	});
});

/**
 * Concurrently maps over an async iterable with a concurrency limit.
 *
 * @param source - The async iterable source
 * @param limit - Maximum number of concurrent operations
 * @param mapper - Function to apply to each item
 * @returns AsyncGenerator that yields results as they complete
 */
export async function* mapConcurrent<T, R>(
	source: AsyncIterable<T>,
	limit: number,
	mapper: (item: T) => Promise<R>,
): AsyncGenerator<R> {
	const it = source[Symbol.asyncIterator]();
	let sourceDone = false;
	const active = new Set<Promise<R>>();

	const nextItem = async (): Promise<T | null> => {
		if (sourceDone) return null;
		const { value, done } = await it.next();
		if (done) {
			sourceDone = true;
			return null;
		}
		return value;
	};

	const fillSlots = async () => {
		while (!sourceDone && active.size < limit) {
			const item = await nextItem();
			if (item === null) break;
			const p = mapper(item).finally(() => {
				active.delete(p);
			});
			active.add(p);
		}
	};

	await fillSlots();

	while (active.size > 0 || !sourceDone) {
		if (active.size === 0) break;
		const result = await Promise.race(active);
		yield result;
		await fillSlots();
	}

	if (typeof it.return === "function") {
		try {
			await it.return();
		} catch {
			// ignore
		}
	}
}

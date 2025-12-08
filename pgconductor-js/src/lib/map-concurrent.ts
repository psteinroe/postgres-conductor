import type { PollableAsyncIterable } from "./async-queue";

/**
 * Concurrently maps over an async iterable with a concurrency limit.
 */
export async function* mapConcurrent<T, R>(
	source: PollableAsyncIterable<T>,
	limit: number,
	mapper: (item: T) => Promise<R>,
): AsyncGenerator<R> {
	const it = source[Symbol.asyncIterator]();
	let sourceDone = false;
	let nextId = 0;

	// Track promises with unique IDs
	type Task = { id: number; promise: Promise<R> };
	const active = new Map<number, Task>();

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
			let item: T | null;

			if (active.size === 0) {
				// No active tasks - MUST block to get at least one
				item = await nextItem();
			} else {
				// Try non-blocking poll
				const polled = source.tryNext();
				if (polled === undefined) {
					// Queue empty, stop filling
					break;
				}
				item = polled;
			}

			if (item === null) break;

			const id = nextId++;
			active.set(id, { id, promise: mapper(item) });
		}
	};

	await fillSlots();

	while (active.size > 0) {
		// Wrap each promise to include its ID
		const wrappedPromises = Array.from(active.values()).map(async (task) => ({
			id: task.id,
			result: await task.promise,
		}));

		// Race to get first completed task
		const { id, result } = await Promise.race(wrappedPromises);

		// Remove the completed task
		active.delete(id);

		yield result;

		// Refill slots
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

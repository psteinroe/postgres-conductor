import type { PollableAsyncIterable } from "./async-queue";

/**
 * Concurrently maps over an async iterable with a concurrency limit.
 */
export async function* mapConcurrent<T, R>(
	source: PollableAsyncIterable<T>,
	limit: number,
	mapper: (item: T) => Promise<R>,
): AsyncGenerator<R> {
	type Task = { id: number; promise: Promise<R> };
	type RaceResult = { id: number; result: R } | { id: null };
	type ItemWait = { promise: Promise<RaceResult>; cancel(): void };

	const it = source[Symbol.asyncIterator]();
	const onNextItemAvailable = source.onNextItemAvailable?.bind(source);
	const active = new Map<number, Task>();
	let sourceDone = false;
	let itemWait: ItemWait | null = null;
	let nextId = 0;

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

	try {
		while (active.size > 0) {
			if (!sourceDone && active.size < limit && !itemWait && onNextItemAvailable) {
				let cancel = () => {};
				const promise = new Promise<void>((resolve) => {
					cancel = onNextItemAvailable(resolve);
				}).then(() => ({ id: null }) as const);
				itemWait = { promise, cancel: () => cancel() };
			}

			const wrappedPromises: Promise<RaceResult>[] = Array.from(active.values()).map(
				async (task) => ({ id: task.id, result: await task.promise }),
			);
			if (itemWait) wrappedPromises.push(itemWait.promise);

			// Race to get completed work or newly available input
			const event = await Promise.race(wrappedPromises);

			if (event.id === null) {
				itemWait = null;
				await fillSlots();
				continue;
			}

			// Remove the completed task
			active.delete(event.id);

			yield event.result;

			// Refill slots
			await fillSlots();
		}
	} finally {
		itemWait?.cancel();
	}
}

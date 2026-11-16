import type { PollableAsyncIterable } from "./async-queue.js";

/**
 * Concurrently maps over an async iterable with a concurrency limit.
 */
export async function* mapConcurrent<T, R>(
	source: PollableAsyncIterable<T>,
	limit: number,
	mapper: (item: T) => Promise<R>,
): AsyncGenerator<R> {
	const it = source[Symbol.asyncIterator]();
	const onNextItemAvailable = source.onNextItemAvailable?.bind(source);
	const active = new Map<number, Promise<R>>();
	// Each mapper reports here once when it settles, so a long one gathers no reactions per loop
	const settled: { id: number; outcome: PromiseSettledResult<R> }[] = [];
	let wake: (() => void) | null = null;
	let cancelItemWait = () => {};
	let itemWaiting = false;
	let itemReady = false;
	let sourceDone = false;
	let nextId = 0;

	const notify = () => {
		const resolve = wake;
		wake = null;
		resolve?.();
	};

	const nextItem = async (): Promise<T | null> => {
		if (sourceDone) return null;
		const { value, done } = await it.next();
		if (done) {
			sourceDone = true;
			return null;
		}
		return value;
	};

	const start = (item: T) => {
		const id = nextId++;
		const promise = mapper(item);
		active.set(id, promise);
		promise.then(
			(value) => {
				settled.push({ id, outcome: { status: "fulfilled", value } });
				notify();
			},
			(reason: unknown) => {
				settled.push({ id, outcome: { status: "rejected", reason } });
				notify();
			},
		);
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
			start(item);
		}
	};

	await fillSlots();

	try {
		while (active.size > 0) {
			if (!sourceDone && active.size < limit && !itemWaiting && onNextItemAvailable) {
				itemWaiting = true;
				// The source may call the listener at once
				cancelItemWait = onNextItemAvailable(() => {
					itemWaiting = false;
					itemReady = true;
					notify();
				});
			}

			// Wait for completed work or newly available input
			if (settled.length === 0 && !itemReady) {
				await new Promise<void>((resolve) => {
					wake = resolve;
				});
			}

			const next = settled.shift();
			if (!next) {
				itemReady = false;
				await fillSlots();
				continue;
			}

			active.delete(next.id);
			if (next.outcome.status === "rejected") throw next.outcome.reason;

			yield next.outcome.value;

			// Refill slots
			await fillSlots();
		}
	} finally {
		cancelItemWait();
		// Also on an error or an early exit, the consumer continues once every mapper settled
		await Promise.allSettled(active.values());
	}
}

import type { PollableAsyncIterable } from "./async-queue";

/**
 * Concurrently maps over an async iterable with a concurrency limit.
 */
export async function* mapConcurrent<T, R>(
	source: PollableAsyncIterable<T>,
	limit: number,
	mapper: (item: T) => Promise<R>,
): AsyncGenerator<R> {
	const iterator = source[Symbol.asyncIterator]();
	let sourceDone = false;
	let pendingRead: Promise<IteratorResult<T>> | null = null;
	let nextId = 0;

	type ActiveTask = { id: number; promise: Promise<R> };
	const active = new Map<number, ActiveTask>();

	const startRead = () => {
		if (!sourceDone && !pendingRead && active.size < limit) {
			pendingRead = iterator.next();
		}
	};

	try {
		startRead();

		const getPendingRead = (): Promise<IteratorResult<T>> | null => pendingRead;

		while (active.size > 0 || pendingRead) {
			const read = getPendingRead();
			const reads = read ? [read.then((result) => ({ kind: "read" as const, result }))] : [];
			const tasks = [...active.values()].map((task) =>
				task.promise.then((result) => ({ kind: "result" as const, id: task.id, result })),
			);

			const event = await Promise.race([...reads, ...tasks]);
			if (event.kind === "read") {
				pendingRead = null;
				if (event.result.done) {
					sourceDone = true;
				} else {
					const id = nextId++;
					active.set(id, { id, promise: mapper(event.result.value) });
				}
				startRead();
			} else {
				active.delete(event.id);
				yield event.result;
				startRead();
			}
		}
	} finally {
		if (typeof iterator.return === "function") {
			try {
				await iterator.return();
			} catch {
				// Ignore cleanup errors.
			}
		}
	}
}

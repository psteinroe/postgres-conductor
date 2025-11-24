export class AsyncQueue<T> implements AsyncIterable<T> {
	private queue: T[] = [];
	private resolvers: ((value: IteratorResult<T>) => void)[] = [];
	private closed = false;

	constructor(private readonly capacity: number) {}

	async push(item: T) {
		if (this.closed) return;
		// Wait while full
		while (this.queue.length >= this.capacity && !this.closed) {
			await new Promise((r) => setTimeout(r, 5));
		}
		if (this.closed) return;
		// If a consumer is waiting, deliver immediately
		const resolver = this.resolvers.shift();
		if (resolver) {
			resolver({ value: item, done: false });
		} else {
			// No consumer waiting, add to queue for later
			this.queue.push(item);
		}
	}

	async next(): Promise<IteratorResult<T>> {
		if (this.queue.length > 0) {
			const value = this.queue.shift()!;
			return { value, done: false };
		}
		if (this.closed) return { value: undefined as any, done: true };
		return new Promise((resolve) => this.resolvers.push(resolve));
	}

	close() {
		this.closed = true;
		for (const r of this.resolvers) r({ value: undefined as any, done: true });
		this.resolvers = [];
	}

	[Symbol.asyncIterator]() {
		return this;
	}
}

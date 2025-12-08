export interface PollableAsyncIterable<T> extends AsyncIterable<T> {
	tryNext(): T | undefined;
}

export class AsyncQueue<T> implements PollableAsyncIterable<T> {
	private queue: T[] = [];
	private resolvers: ((value: IteratorResult<T>) => void)[] = [];
	private pushResolvers: (() => void)[] = [];
	private closed = false;

	constructor(private readonly capacity: number) {}

	async push(item: T) {
		if (this.closed) return;

		// Wait if full
		if (this.queue.length >= this.capacity) {
			await new Promise<void>((resolve) => {
				this.pushResolvers.push(resolve);
			});
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

	tryNext(): T | undefined {
		if (this.queue.length > 0) {
			const item = this.queue.shift()!;
			this.notifyPusher();
			return item;
		}
		return undefined;
	}

	async next(): Promise<IteratorResult<T>> {
		if (this.queue.length > 0) {
			const value = this.queue.shift()!;
			this.notifyPusher();
			return { value, done: false };
		}
		if (this.closed) return { value: undefined as any, done: true };
		return new Promise((resolve) => this.resolvers.push(resolve));
	}

	private notifyPusher() {
		// If there's space and someone waiting to push, wake them
		if (this.queue.length < this.capacity && this.pushResolvers.length > 0) {
			const pusher = this.pushResolvers.shift();
			if (pusher) pusher();
		}
	}

	close() {
		this.closed = true;
		for (const r of this.resolvers) r({ value: undefined as any, done: true });
		this.resolvers = [];
		for (const pusher of this.pushResolvers) pusher();
		this.pushResolvers = [];
	}

	[Symbol.asyncIterator]() {
		return this;
	}
}

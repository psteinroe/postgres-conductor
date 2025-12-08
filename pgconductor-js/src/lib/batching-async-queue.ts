type BatchConfig = { size: number; timeoutMs: number };

/**
 * AsyncQueue that automatically batches items based on configuration.
 *
 * Follows AsyncQueue implementation exactly, but emits T[] instead of T.
 * - Items with batch config are accumulated and emitted as arrays when size/timeout reached
 * - Items without batch config are emitted immediately as single-item arrays
 * - Always emits T[] (arrays) to downstream consumers
 */
export class BatchingAsyncQueue<T extends { task_key: string }> implements AsyncIterable<T[]> {
	private queue: T[][] = [];
	private resolvers: ((value: IteratorResult<T[]>) => void)[] = [];
	private closed = false;

	// Track batches per task
	private batches = new Map<
		string,
		{
			items: T[];
			timer: Timer | null;
			config: BatchConfig;
		}
	>();

	constructor(
		private readonly capacity: number,
		private readonly getBatchConfig: (taskKey: string) => BatchConfig | null,
	) {}

	async push(item: T): Promise<void> {
		if (this.closed) return;

		// Wait while full (backpressure at push level, not emit level)
		while (this.queue.length >= this.capacity && !this.closed) {
			await new Promise((r) => setTimeout(r, 5));
		}
		if (this.closed) return;

		const batchConfig = this.getBatchConfig(item.task_key);

		if (batchConfig) {
			// Batched task: accumulate
			this.addToBatch(item, batchConfig);
		} else {
			// Non-batched: emit immediately as single-item array
			this.emitGroup([item]);
		}
	}

	private addToBatch(item: T, config: BatchConfig): void {
		let batch = this.batches.get(item.task_key);

		if (!batch) {
			batch = {
				items: [],
				timer: null,
				config,
			};
			this.batches.set(item.task_key, batch);
		}

		batch.items.push(item);

		// Start timeout on first item
		if (batch.items.length === 1) {
			batch.timer = setTimeout(() => {
				void this.flushBatch(item.task_key);
			}, config.timeoutMs);
		}

		// Flush when size reached
		if (batch.items.length >= config.size) {
			void this.flushBatch(item.task_key);
		}
	}

	private flushBatch(taskKey: string): void {
		const batch = this.batches.get(taskKey);
		if (!batch || batch.items.length === 0) return;

		if (batch.timer) {
			clearTimeout(batch.timer);
			batch.timer = null;
		}

		// Emit batch as array
		const items = [...batch.items];
		batch.items = [];
		this.emitGroup(items);
	}

	private emitGroup(group: T[]): void {
		if (this.closed) return;

		// If a consumer is waiting, deliver immediately (same as AsyncQueue)
		const resolver = this.resolvers.shift();
		if (resolver) {
			resolver({ value: group, done: false });
		} else {
			// No consumer waiting, add to queue (may temporarily exceed capacity)
			// Backpressure is managed at push() level, not here
			this.queue.push(group);
		}
	}

	async next(): Promise<IteratorResult<T[]>> {
		// Same logic as AsyncQueue
		if (this.queue.length > 0) {
			const value = this.queue.shift()!;
			return { value, done: false };
		}
		if (this.closed) return { value: undefined as any, done: true };
		return new Promise((resolve) => this.resolvers.push(resolve));
	}

	close(): void {
		if (this.closed) return;

		// Flush all pending batches to queue BEFORE marking as closed
		for (const taskKey of this.batches.keys()) {
			const batch = this.batches.get(taskKey);
			if (batch && batch.items.length > 0) {
				if (batch.timer) {
					clearTimeout(batch.timer);
					batch.timer = null;
				}
				// Add to queue for consumers to process
				this.queue.push([...batch.items]);
				batch.items = [];
			}
		}

		// Deliver queued items to waiting resolvers before marking as done
		while (this.queue.length > 0 && this.resolvers.length > 0) {
			const resolver = this.resolvers.shift()!;
			const value = this.queue.shift()!;
			resolver({ value, done: false });
		}

		// Now mark as closed
		this.closed = true;

		// Signal remaining waiting consumers as done
		for (const r of this.resolvers) r({ value: undefined as any, done: true });
		this.resolvers = [];
	}

	[Symbol.asyncIterator]() {
		return this;
	}
}

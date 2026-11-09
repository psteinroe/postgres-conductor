import type { JsonValue, StoredSignal } from "./database-client.js";
import { AsyncQueue } from "./lib/async-queue.js";
import { coerceError } from "./lib/coerce-error.js";

export type SignalInfo = { id: string; name: string };

export type SignalHandler<T extends JsonValue = JsonValue> = (
	payload: T,
	info: SignalInfo,
) => unknown;

export type SignalInboxOptions = {
	/** Mark a signal delivered */
	ack: (id: string) => Promise<void>;
	/** A handler threw, or a signal could not be acknowledged, while the inbox was open */
	onError: (error: Error) => void;
};

/**
 * Delivers the signals of one execution attempt to its `ctx.onSignal` handlers, one at a time in
 * the order they were stored, and acknowledges each once its handler returned. A signal without a
 * handler when it is reached stays undelivered: if a handler for it registers later, settlement
 * re-queues the execution and the next attempt receives it.
 */
export class SignalInbox {
	private readonly handlers = new Map<string, SignalHandler>();
	private readonly queue = new AsyncQueue<StoredSignal>(Infinity);
	private lastSeq = 0;
	private delivering: SignalInfo | null = null;
	private holds = 0;
	private resume: (() => void) | null = null;
	private consuming: Promise<void> | null = null;
	private closed = false;
	private stopped = false;
	private failure: Error | null = null;
	/** Signals received, and ones whose handler returned or that had no handler */
	private received = 0;
	private handled = 0;
	/** Handlers started */
	private _started = 0;
	private quietWaiters: (() => void)[] = [];

	constructor(private readonly opts: SignalInboxOptions) {}

	/** Signal names this attempt listened for */
	get names(): string[] {
		return [...this.handlers.keys()];
	}

	/** Handlers started so far */
	get started(): number {
		return this._started;
	}

	/** Sequence number of the last signal received, or null while nothing is read for this inbox */
	get afterSeq(): number | null {
		return this.closed || this.handlers.size === 0 ? null : this.lastSeq;
	}

	listen(name: string, handler: SignalHandler): void {
		if (this.closed) {
			throw new Error(`Cannot listen for signal "${name}" after signal delivery stopped`);
		}
		if (this.handlers.has(name)) {
			throw new Error(`A handler for signal "${name}" is already registered`);
		}
		this.handlers.set(name, handler);
		this.consuming ||= this.consume();
	}

	/** Add an undelivered signal, in stored order; one received before is skipped */
	push(signal: StoredSignal): void {
		if (signal.seq <= this.lastSeq || this.closed) return;
		this.lastSeq = signal.seq;
		this.received++;
		void this.queue.push(signal);
	}

	/**
	 * Stop delivering if no handler started since `started` was `since`, and every signal received
	 * was handled; signals received later stay undelivered. Returns whether it stopped.
	 */
	stopIfQuiet(since: number): boolean {
		if (this.consuming && (this._started !== since || !this.quiet)) return false;
		this.closed = true;
		this.stopped = true;
		this.queue.close();
		this.resume?.();
		this.notifyQuiet();
		return true;
	}

	/** Resolves once every signal received was handled, or delivery stopped */
	whenQuiet(): Promise<void> {
		if (this.quiet || this.closed) return Promise.resolve();
		return new Promise((resolve) => this.quietWaiters.push(resolve));
	}

	private get quiet(): boolean {
		return this.handled === this.received;
	}

	private notifyQuiet(): void {
		for (const resolve of this.quietWaiters.splice(0)) resolve();
	}

	/**
	 * Run `fn` without starting a handler, passing the signal whose handler is running and not yet
	 * acknowledged. Every other signal read so far stays undelivered until `fn` settles.
	 */
	async hold<T>(fn: (delivering: SignalInfo | null) => Promise<T>): Promise<T> {
		this.holds++;
		try {
			return await fn(this.delivering);
		} finally {
			this.holds--;
			if (this.holds === 0) this.resume?.();
		}
	}

	/**
	 * Stop accepting handlers and signals. With `drain`, deliver the signals already received
	 * first; otherwise stop after the running handler. Returns the error that failed delivery.
	 */
	async close({ drain }: { drain: boolean }): Promise<Error | null> {
		this.closed = true;
		this.stopped = !drain;
		this.queue.close();
		this.resume?.();
		this.notifyQuiet();
		await this.consuming;
		return this.failure;
	}

	private async consume(): Promise<void> {
		for await (const signal of this.queue) {
			while (this.holds > 0 && !this.closed) {
				await new Promise<void>((resolve) => (this.resume = resolve));
			}
			if (this.stopped) return;
			const handler = this.handlers.get(signal.name);
			if (!handler) {
				this.markHandled();
				continue;
			}

			this._started++;
			this.delivering = { id: signal.id, name: signal.name };
			try {
				await handler(signal.payload, { id: signal.id, name: signal.name });
			} catch (err) {
				this.fail(
					new Error(`Signal handler for "${signal.name}" failed: ${coerceError(err).message}`),
				);
				return;
			}

			try {
				await this.opts.ack(signal.id);
			} catch (err) {
				this.fail(coerceError(err));
				return;
			}
			this.delivering = null;
			this.markHandled();
		}
	}

	private markHandled(): void {
		this.handled++;
		if (this.quiet) this.notifyQuiet();
	}

	private fail(error: Error): void {
		this.failure = error;
		// once closed, the error is returned by close() instead
		if (!this.closed) this.opts.onError(error);
		this.closed = true;
		this.notifyQuiet();
	}
}

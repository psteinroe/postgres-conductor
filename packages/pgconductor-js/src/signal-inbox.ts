import type { JsonValue, StoredSignal } from "./database-client";
import { coerceError } from "./lib/coerce-error";

export type SignalInfo = { id: string; name: string };

export type SignalHandler<T extends JsonValue = JsonValue> = (
	payload: T,
	info: SignalInfo,
) => unknown;

export type SignalInboxOptions = {
	/** Mark signals delivered */
	ack: (ids: string[]) => Promise<void>;
	/** A handler threw, or a signal could not be acknowledged, while the inbox was open */
	onError: (error: Error) => void;
};

/**
 * Delivers the signals of one execution attempt to its `ctx.onSignal` handlers, one at a time in
 * the order they were stored. The worker pushes undelivered signals read on each poll. A signal is
 * acknowledged once its handler returned. Signals without a handler are kept until one registers.
 */
export class SignalInbox {
	private readonly handlers = new Map<string, SignalHandler>();
	private readonly buffered: StoredSignal[] = [];
	private lastSeq = 0;
	private pumping: Promise<void> | null = null;
	private closed = false;
	private failure: Error | null = null;

	constructor(private readonly opts: SignalInboxOptions) {}

	/** Signal names this attempt listened for */
	get names(): string[] {
		return [...this.handlers.keys()];
	}

	/** Sequence number of the last signal received, or null while nothing is read for this inbox */
	get afterSeq(): number | null {
		return this.closed || this.handlers.size === 0 ? null : this.lastSeq;
	}

	listen(name: string, handler: SignalHandler): void {
		if (this.closed) {
			throw new Error(`Cannot listen for signal "${name}" after the handler returned`);
		}
		if (this.handlers.has(name)) {
			throw new Error(`A handler for signal "${name}" is already registered`);
		}
		this.handlers.set(name, handler);
		this.pump();
	}

	/** Add undelivered signals in stored order; ones received before are skipped */
	push(signals: StoredSignal[]): void {
		if (this.closed) return;
		for (const signal of signals) {
			if (signal.seq <= this.lastSeq) continue;
			this.buffered.push(signal);
			this.lastSeq = signal.seq;
		}
		this.pump();
	}

	/**
	 * Stop delivering. With `wait`, wait for the running handler and its acknowledgement.
	 * Returns the error that failed delivery, if any.
	 */
	async close({ wait }: { wait: boolean }): Promise<Error | null> {
		this.closed = true;
		if (wait) await this.pumping;
		return this.failure;
	}

	private pump(): void {
		if (this.pumping || this.closed || !this.hasDeliverable()) return;
		this.pumping = this.pumpLoop().finally(() => {
			this.pumping = null;
			// a signal may have been buffered after the loop's last check
			this.pump();
		});
	}

	private async pumpLoop(): Promise<void> {
		while (!this.closed) {
			const index = this.buffered.findIndex((signal) => this.handlers.has(signal.name));
			const [signal] = index === -1 ? [] : this.buffered.splice(index, 1);
			const handler = signal && this.handlers.get(signal.name);
			if (!signal || !handler) return;

			try {
				await handler(signal.payload, { id: signal.id, name: signal.name });
			} catch (err) {
				this.fail(err, `Signal handler for "${signal.name}" failed: `);
				return;
			}

			try {
				await this.opts.ack([signal.id]);
			} catch (err) {
				this.fail(err);
				return;
			}
		}
	}

	private hasDeliverable(): boolean {
		return this.buffered.some((signal) => this.handlers.has(signal.name));
	}

	private fail(err: unknown, prefix = ""): void {
		if (this.failure) return;
		const error = new Error(`${prefix}${coerceError(err).message}`);
		this.failure = error;
		const wasClosed = this.closed;
		this.closed = true;
		if (!wasClosed) this.opts.onError(error);
	}
}

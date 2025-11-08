export class Signal {
	private promise: Promise<void> | null = null;
	private resolve: (() => void) | null = null;
	private timeout: Timer | null = null;

	/**
	 * Wait for the next signal. Can be called multiple times concurrently.
	 */
	wait(): Promise<void> {
		if (!this.promise) {
			this.promise = new Promise<void>((r) => {
				this.resolve = r;
			});
		}
		return this.promise;
	}

	/**
	 * Wait for signal or timeout after ms milliseconds.
	 * Automatically signals after the timeout.
	 */
	waitFor(ms: number): Promise<void> {
		this.clearTimeout();
		this.timeout = setTimeout(() => this.signal(), ms);
		return this.wait();
	}

	/**
	 * Signal all waiters. Resets automatically for next wait.
	 */
	signal() {
		if (this.resolve) {
			this.resolve();
			this.clearTimeout();
			this.promise = null;
			this.resolve = null;
		}
	}

	/**
	 * Abort any pending timeout and signal immediately.
	 */
	abort() {
		this.clearTimeout();
		this.signal();
	}

	/**
	 * Check if anyone is waiting
	 */
	hasWaiters(): boolean {
		return this.promise !== null;
	}

	/**
	 * Clear any pending waiters and timeout without resolving
	 */
	reset() {
		this.clearTimeout();
		this.promise = null;
		this.resolve = null;
	}

	private clearTimeout() {
		if (this.timeout) {
			clearTimeout(this.timeout);
			this.timeout = null;
		}
	}
}

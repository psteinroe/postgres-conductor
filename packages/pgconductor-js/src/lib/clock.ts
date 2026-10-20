import type { Logger } from "./logger.js";

const DATABASE_CLOCK_REFRESH_INTERVAL_MS = 10 * 60 * 1000;

// Running clocks by the `sql` instance they sample
const runningClocks = new WeakMap<object, Set<Clock>>();

/**
 * Re-sample every running clock that reads time through `source`.
 * `pgconductor-js/testing` calls this after changing fake time, so running workers follow it.
 */
export async function refreshClocks(source: object): Promise<void> {
	await Promise.all(Array.from(runningClocks.get(source) || [], (clock) => clock.refresh()));
}

export type DatabaseTime = {
	now: Date;
	// Fake time is set, so database time stands still
	frozen: boolean;
};

export type ClockOptions = {
	sampleDatabaseTime: (signal?: AbortSignal) => Promise<DatabaseTime>;
	logger: Logger;
	// Key under which `refreshClocks` finds this clock while it runs
	source: object;
	localClock?: () => Date;
	refreshIntervalMs?: number;
};

/**
 * A worker-local clock corrected to the database server's clock.
 * The offset is database time minus the midpoint of the local request times.
 * While the database uses fake time, the clock stands still at the sampled time.
 */
export class Clock {
	private readonly sampleDatabaseTime: (signal?: AbortSignal) => Promise<DatabaseTime>;
	private readonly logger: Logger;
	private readonly source: object;
	private readonly localClock: () => Date;
	private readonly refreshIntervalMs: number;
	private offsetMs = 0;
	private frozenAt: Date | null = null;
	private refreshTimer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	constructor({
		sampleDatabaseTime,
		logger,
		source,
		localClock = () => new Date(),
		refreshIntervalMs = DATABASE_CLOCK_REFRESH_INTERVAL_MS,
	}: ClockOptions) {
		this.sampleDatabaseTime = sampleDatabaseTime;
		this.logger = logger;
		this.source = source;
		this.localClock = localClock;
		this.refreshIntervalMs = refreshIntervalMs;
	}

	now(): Date {
		if (this.frozenAt) return new Date(this.frozenAt);
		return new Date(this.localClock().getTime() + this.offsetMs);
	}

	async start(signal?: AbortSignal): Promise<void> {
		this.stop();
		this.running = true;
		const clocks = runningClocks.get(this.source) || new Set();
		runningClocks.set(this.source, clocks.add(this));
		await this.refresh(signal);
		if (this.running) this.scheduleNextRefresh();
	}

	stop(): void {
		this.running = false;
		runningClocks.get(this.source)?.delete(this);
		if (this.refreshTimer !== null) {
			clearTimeout(this.refreshTimer);
			this.refreshTimer = null;
		}
	}

	async refresh(signal?: AbortSignal): Promise<void> {
		try {
			const requestStart = this.localClock();
			const databaseTime = await this.sampleDatabaseTime(signal);
			const responseEnd = this.localClock();
			const midpoint = (requestStart.getTime() + responseEnd.getTime()) / 2;
			this.offsetMs = databaseTime.now.getTime() - midpoint;
			this.frozenAt = databaseTime.frozen ? databaseTime.now : null;
		} catch (error) {
			this.logger.warn("Database clock offset refresh failed; retaining previous offset", error);
		}
	}

	private scheduleNextRefresh(): void {
		this.refreshTimer = setTimeout(async () => {
			this.refreshTimer = null;
			await this.refresh();
			if (this.running) this.scheduleNextRefresh();
		}, this.refreshIntervalMs);
	}
}

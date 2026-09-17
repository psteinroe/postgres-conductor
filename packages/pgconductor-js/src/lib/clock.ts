import type { Logger } from "./logger";

const DATABASE_CLOCK_REFRESH_INTERVAL_MS = 10 * 60 * 1000;

export type ClockOptions = {
	sampleDatabaseTime: (signal?: AbortSignal) => Promise<Date>;
	logger: Logger;
	localClock?: () => Date;
	refreshIntervalMs?: number;
};

/**
 * A worker-local clock corrected to the database server's clock.
 * The offset is database time minus the midpoint of the local request times.
 */
export class Clock {
	private offsetMs = 0;
	private refreshTimer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	constructor({
		sampleDatabaseTime,
		logger,
		localClock = () => new Date(),
		refreshIntervalMs = DATABASE_CLOCK_REFRESH_INTERVAL_MS,
	}: ClockOptions) {
		this.sampleDatabaseTime = sampleDatabaseTime;
		this.logger = logger;
		this.localClock = localClock;
		this.refreshIntervalMs = refreshIntervalMs;
	}

	private readonly sampleDatabaseTime: (signal?: AbortSignal) => Promise<Date>;
	private readonly logger: Logger;
	private readonly localClock: () => Date;
	private readonly refreshIntervalMs: number;

	now(): Date {
		return new Date(this.localClock().getTime() + this.offsetMs);
	}

	get offset(): number {
		return this.offsetMs;
	}

	async start(signal?: AbortSignal): Promise<void> {
		this.stop();
		this.running = true;
		await this.refresh(signal);
		if (this.running) this.scheduleNextRefresh();
	}

	stop(): void {
		this.running = false;
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
			this.offsetMs = databaseTime.getTime() - midpoint;
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

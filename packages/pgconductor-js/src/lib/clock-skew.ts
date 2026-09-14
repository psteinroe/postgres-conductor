import type { Logger } from "./logger";

export type LocalClock = () => Date;
export type DatabaseTimeSampler = (signal?: AbortSignal) => Promise<Date>;

export type WorkerClock = {
	now(): Date;
	start(signal?: AbortSignal): Promise<void>;
	stop(): void;
};

export const DATABASE_CLOCK_REFRESH_INTERVAL_MS = 10 * 60 * 1000;

/**
 * A worker-local clock corrected to the database server's clock.
 * The offset is database time minus the midpoint of the local request times.
 */
export class DatabaseClockOffset implements WorkerClock {
	private offsetMs = 0;
	private refreshTimer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	constructor(
		private readonly sampleDatabaseTime: DatabaseTimeSampler,
		private readonly logger: Logger,
		private readonly localClock: LocalClock = () => new Date(),
		private readonly refreshIntervalMs = DATABASE_CLOCK_REFRESH_INTERVAL_MS,
	) {}

	now(): Date {
		return new Date(this.localClock().getTime() + this.offsetMs);
	}

	get offset(): number {
		return this.offsetMs;
	}

	async start(signal?: AbortSignal): Promise<void> {
		this.stop();
		this.running = true;
		try {
			await this.refreshOffset(signal, true);
			if (this.running) this.scheduleRefresh();
		} catch (error) {
			this.stop();
			throw error;
		}
	}

	stop(): void {
		this.running = false;
		if (this.refreshTimer !== null) {
			clearTimeout(this.refreshTimer);
			this.refreshTimer = null;
		}
	}

	async refresh(signal?: AbortSignal): Promise<void> {
		await this.refreshOffset(signal, false);
	}

	private async refreshOffset(signal: AbortSignal | undefined, initial: boolean): Promise<void> {
		try {
			const requestStart = this.localClock();
			const databaseTime = await this.sampleDatabaseTime(signal);
			const responseEnd = this.localClock();
			const midpoint = (requestStart.getTime() + responseEnd.getTime()) / 2;
			this.offsetMs = databaseTime.getTime() - midpoint;
		} catch (error) {
			if (initial) throw error;
			this.logger.warn("Database clock offset refresh failed; retaining previous offset", error);
		}
	}

	private scheduleRefresh(): void {
		this.refreshTimer = setTimeout(async () => {
			this.refreshTimer = null;
			await this.refresh();
			if (this.running) this.scheduleRefresh();
		}, this.refreshIntervalMs);
	}
}

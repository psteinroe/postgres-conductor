import * as assert from "./assert";

export class WindowChecker {
	private startMinutes: number;
	private endMinutes: number;

	constructor(private window: [string, string]) {
		this.startMinutes = this.timeToMinutes(window[0]);
		this.endMinutes = this.timeToMinutes(window[1]);
	}

	private parseTime(time: string): { hours: number; minutes: number } {
		const [h, m] = time.split(":").map(Number);

		assert.ok(
			typeof h === "number" && !Number.isNaN(h) && h >= 0 && h <= 24,
			`Invalid time format: ${time}`,
		);

		const hours = h;
		const minutes = typeof m === "undefined" || Number.isNaN(m) ? 0 : m;

		return { hours, minutes };
	}

	private timeToMinutes(time: string): number {
		const { hours, minutes } = this.parseTime(time);
		return hours * 60 + minutes;
	}

	isWithinWindow(now: Date): boolean {
		const currentMinutes = now.getUTCHours() * 60 + now.getUTCMinutes();
		return currentMinutes >= this.startMinutes && currentMinutes < this.endMinutes;
	}

	getNextValidRunAt(now: Date): Date {
		const { hours, minutes } = this.parseTime(this.window[0]);

		const nextRun = new Date(now);
		nextRun.setUTCHours(hours, minutes, 0, 0);

		if (nextRun <= now) {
			nextRun.setUTCDate(nextRun.getUTCDate() + 1);
		}

		return nextRun;
	}
}

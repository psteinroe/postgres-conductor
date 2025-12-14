import { test, expect, describe, mock } from "bun:test";
import { DatabaseClient } from "../../src/database-client";
import type { Sql } from "postgres";

// Helper to create a mock Sql instance with proper postgres.js interface
function createMockSql(handler: () => Promise<any[]> | any[]): Sql & { json: (val: any) => any } {
	const wrappedHandler = () => {
		try {
			const result = handler();
			// Ensure result is a promise
			return result instanceof Promise ? result : Promise.resolve(result);
		} catch (err) {
			// Convert synchronous errors to promise rejections
			return Promise.reject(err);
		}
	};

	const sqlFn = function (strings: TemplateStringsArray, ...values: any[]) {
		return wrappedHandler();
	} as any;

	sqlFn.unsafe = wrappedHandler;
	sqlFn.json = (val: any) => val; // Pass through for mock

	return sqlFn as Sql & { json: (val: any) => any };
}

describe("DatabaseClient retry behavior", () => {
	test("retries on retryable errors when signal provided", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			if (attemptCount < 3) {
				const err = new Error("Connection reset") as Error & { code?: string };
				err.code = "ECONNRESET";
				throw err;
			}
			return Promise.resolve([{ id: "test-id" }]);
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		const signal = new AbortController().signal;
		const result = await db.invoke(
			{
				task_key: "test-task",
				queue: "default",
				payload: {},
			},
			{ signal },
		);

		expect(result).toBe("test-id");
		expect(attemptCount).toBe(3);
	}, 10000);

	test("fails fast on retryable errors when no signal provided", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			const err = new Error("Connection reset") as Error & { code?: string };
			err.code = "ECONNRESET";
			throw err;
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		try {
			await db.invoke({
				task_key: "test-task",
				queue: "default",
				payload: {},
			});
			expect.unreachable("Should have thrown");
		} catch (err) {
			expect((err as Error).message).toBe("Connection reset");
			expect(attemptCount).toBe(1);
		}
	});

	test("respects abort signal during retries", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			const err = new Error("Deadlock detected") as Error & { code?: string };
			err.code = "40P01";
			throw err;
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		const controller = new AbortController();

		// Abort after first retry
		setTimeout(() => controller.abort(), 150);

		try {
			await db.invoke(
				{
					task_key: "test-task",
					queue: "default",
					payload: {},
				},
				{ signal: controller.signal },
			);
			expect.unreachable("Should have thrown");
		} catch (err) {
			expect((err as Error).message).toBe("Deadlock detected");
			// Should have tried at least once but stopped due to abort
			expect(attemptCount).toBeGreaterThanOrEqual(1);
			expect(attemptCount).toBeLessThan(5); // Should not retry many times
		}
	});

	test("fails immediately on non-retryable errors with signal", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			const err = new Error("Syntax error") as Error & { code?: string };
			err.code = "42601";
			throw err;
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		const signal = new AbortController().signal;

		try {
			await db.invoke(
				{
					task_key: "test-task",
					queue: "default",
					payload: {},
				},
				{ signal },
			);
			expect.unreachable("Should have thrown");
		} catch (err) {
			expect((err as Error).message).toBe("Syntax error");
			expect(attemptCount).toBe(1);
		}
	});

	test("fails immediately on non-retryable errors without signal", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			const err = new Error("Syntax error") as Error & { code?: string };
			err.code = "42601";
			throw err;
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		try {
			await db.invoke({
				task_key: "test-task",
				queue: "default",
				payload: {},
			});
			expect.unreachable("Should have thrown");
		} catch (err) {
			expect((err as Error).message).toBe("Syntax error");
			expect(attemptCount).toBe(1);
		}
	});

	test("succeeds immediately when no errors", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			return Promise.resolve([{ id: "success-id" }]);
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		// Test with signal
		const resultWithSignal = await db.invoke(
			{
				task_key: "test-task",
				queue: "default",
				payload: {},
			},
			{ signal: new AbortController().signal },
		);
		expect(resultWithSignal).toBe("success-id");
		expect(attemptCount).toBe(1);

		// Reset counter
		attemptCount = 0;

		// Test without signal
		const resultWithoutSignal = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: {},
		});
		expect(resultWithoutSignal).toBe("success-id");
		expect(attemptCount).toBe(1);
	});

	test("retries on serialization failure", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			if (attemptCount < 2) {
				const err = new Error("Serialization failure") as Error & {
					code?: string;
				};
				err.code = "40001";
				throw err;
			}
			return Promise.resolve([{ id: "success-after-retry" }]);
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		const signal = new AbortController().signal;
		const result = await db.invoke(
			{
				task_key: "test-task",
				queue: "default",
				payload: {},
			},
			{ signal },
		);

		expect(result).toBe("success-after-retry");
		expect(attemptCount).toBe(2);
	}, 10000);

	test("retries on lock not available", async () => {
		let attemptCount = 0;
		const mockSql = createMockSql(() => {
			attemptCount++;
			if (attemptCount < 2) {
				const err = new Error("Lock not available") as Error & { code?: string };
				err.code = "55P03";
				throw err;
			}
			return Promise.resolve([{ id: "success-after-lock" }]);
		});

		const db = new DatabaseClient({
			sql: mockSql,
			logger: {
				info: mock(),
				warn: mock(),
				error: mock(),
				debug: mock(),
			},
		});

		const signal = new AbortController().signal;
		const result = await db.invoke(
			{
				task_key: "test-task",
				queue: "default",
				payload: {},
			},
			{ signal },
		);

		expect(result).toBe("success-after-lock");
		expect(attemptCount).toBe(2);
	}, 10000);
});

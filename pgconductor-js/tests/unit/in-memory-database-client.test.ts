import { test, expect, describe } from "bun:test";
import { InMemoryDatabaseClient } from "../mocks/in-memory-database-client";

describe("InMemoryDatabaseClient", () => {
	describe("Basic Execution Lifecycle", () => {
		test("invoke creates pending execution", async () => {
			const db = new InMemoryDatabaseClient();

			const id = await db.invoke({
				task_key: "test-task",
				queue: "default",
				payload: { foo: "bar" },
			});

			expect(id).toMatch(/^in-memory-/);

			const exec = db.getExecution(id!);
			expect(exec).toBeDefined();
			expect(exec?.task_key).toBe("test-task");
			expect(exec?.state).toBe("pending");
			expect(exec?.payload).toEqual({ foo: "bar" });
		});

		test("getExecutions returns ready executions", async () => {
			const db = new InMemoryDatabaseClient();

			await db.invoke({
				task_key: "task-a",
				queue: "default",
				payload: {},
			});

			await db.invoke({
				task_key: "task-b",
				queue: "default",
				payload: {},
			});

			const executions = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			expect(executions.length).toBe(2);
			expect(executions[0]?.task_key).toBe("task-a");
			expect(executions[1]?.task_key).toBe("task-b");

			// Verify they were claimed
			expect(db.getExecution(executions[0]!.id)?.state).toBe("running");
			expect(db.getExecution(executions[1]!.id)?.state).toBe("running");
		});

		test("returnExecutions marks as completed", async () => {
			const db = new InMemoryDatabaseClient();

			const id = await db.invoke({
				task_key: "test-task",
				queue: "default",
				payload: {},
			});

			const executions = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "test-task",
					status: "completed",
					result: { output: "success" },
				},
			]);

			const exec = db.getExecution(id!);
			expect(exec?.state).toBe("completed");
			expect(exec?.result).toEqual({ output: "success" });
		});
	});

	describe("Retry and Backoff", () => {
		test("failed execution retries with backoff", async () => {
			const db = new InMemoryDatabaseClient();

			await db.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "test-task",
						maxAttempts: 3,
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			});

			const id = await db.invoke({
				task_key: "test-task",
				queue: "default",
				payload: {},
			});

			const executions = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			const startTime = db.getCurrentTime();

			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "test-task",
					status: "failed",
					error: "Task failed",
				},
			]);

			const exec = db.getExecution(id!);
			expect(exec?.state).toBe("pending");
			expect(exec?.attempts).toBe(1);
			expect(exec?.last_error).toBe("Task failed");

			// Should be rescheduled with 15s backoff
			const expectedRunAt = new Date(startTime.getTime() + 15000);
			expect(exec?.run_at.getTime()).toBe(expectedRunAt.getTime());
		});

		test("execution becomes permanently failed after max attempts", async () => {
			const db = new InMemoryDatabaseClient();

			await db.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "test-task",
						maxAttempts: 2,
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			});

			const id = await db.invoke({
				task_key: "test-task",
				queue: "default",
				payload: {},
			});

			// First failure
			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "test-task",
					status: "failed",
					error: "Failure 1",
				},
			]);

			expect(db.getExecution(id!)?.state).toBe("pending");
			expect(db.getExecution(id!)?.attempts).toBe(1);

			// Second failure - should become permanently failed
			db.advanceTime(16000);
			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "test-task",
					status: "failed",
					error: "Failure 2",
				},
			]);

			expect(db.getExecution(id!)?.state).toBe("failed");
			expect(db.getExecution(id!)?.attempts).toBe(2);
		});
	});

	describe("Cron Scheduling", () => {
		test("scheduleCronExecution creates initial execution", async () => {
			const db = new InMemoryDatabaseClient();

			const id = await db.scheduleCronExecution({
				spec: {
					task_key: "cron-task",
					queue: "default",
					payload: {},
					cron_expression: "*/5 * * * * *",
				},
				scheduleName: "test-schedule",
			});

			const exec = db.getExecution(id!);
			expect(exec).toBeDefined();
			expect(exec?.cron_expression).toBe("*/5 * * * * *");
			expect(exec?.dedupe_key).toBe("cron::test-schedule::cron-task::default");

			const schedules = db.getCronSchedules("cron-task");
			expect(schedules.length).toBe(1);
			expect(schedules[0]?.schedule_name).toBe("test-schedule");
		});

		test("completed cron execution schedules next run", async () => {
			const db = new InMemoryDatabaseClient();

			const id = await db.scheduleCronExecution({
				spec: {
					task_key: "cron-task",
					queue: "default",
					payload: {},
					cron_expression: "*/5 * * * * *",
				},
				scheduleName: "test-schedule",
			});

			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "cron-task",
					status: "completed",
				},
			]);

			// Should have scheduled next execution
			const pending = db.getPendingExecutions();
			const nextExec = pending.find(
				(e) => e.task_key === "cron-task" && e.cron_expression !== null,
			);

			expect(nextExec).toBeDefined();
			expect(nextExec?.dedupe_key).toBe("cron::test-schedule::cron-task::default");
		});

		test("unscheduleCronExecution removes schedule and cancels pending", async () => {
			const db = new InMemoryDatabaseClient();

			const id = await db.scheduleCronExecution({
				spec: {
					task_key: "cron-task",
					queue: "default",
					payload: {},
					cron_expression: "*/5 * * * * *",
				},
				scheduleName: "test-schedule",
			});

			expect(db.getCronSchedules("cron-task").length).toBe(1);

			await db.unscheduleCronExecution({
				taskKey: "cron-task",
				queue: "default",
				scheduleName: "test-schedule",
			});

			expect(db.getCronSchedules("cron-task").length).toBe(0);

			// Pending execution should be cancelled
			const exec = db.getExecution(id!);
			expect(exec?.cancelled).toBe(true);
		});

		test("CRITICAL: unschedule prevents retry re-insertion", async () => {
			const db = new InMemoryDatabaseClient();

			await db.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "cron-task",
						maxAttempts: 3,
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			});

			// Schedule cron
			const id = await db.scheduleCronExecution({
				spec: {
					task_key: "cron-task",
					queue: "default",
					payload: {},
					cron_expression: "*/5 * * * * *",
				},
				scheduleName: "test-schedule",
			});

			// Advance time to make cron execution ready (*/5 means every 5 seconds)
			db.advanceTime(5001);

			// Get and "execute" (mark as running)			// Get and "execute" (mark as running)
			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			expect(db.getExecution(id!)?.state).toBe("running");

			// Unschedule while running
			await db.unscheduleCronExecution({
				taskKey: "cron-task",
				queue: "default",
				scheduleName: "test-schedule",
			});

			// Fail the execution (simulating task failure)
			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "cron-task",
					status: "failed",
					error: "Task failed",
				},
			]);

			// Execution should be scheduled for retry
			const exec = db.getExecution(id!);
			expect(exec?.state).toBe("pending");
			expect(exec?.attempts).toBe(1);

			// BUT: cron_expression should be cleared (no re-insertion)
			expect(exec?.cron_expression).toBeNull();

			// Advance time past backoff
			db.advanceTime(16000);

			// Execute retry
			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "cron-task",
					status: "completed",
				},
			]);

			// CRITICAL: No new cron executions should exist
			const allPending = db.getPendingExecutions();
			const futureCronExecs = allPending.filter(
				(e) => e.task_key === "cron-task" && e.cron_expression !== null,
			);

			expect(futureCronExecs.length).toBe(0);
		});

		test("unschedule cancels running execution and prevents re-insert on retry", async () => {
			const db = new InMemoryDatabaseClient();

			await db.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "slow-task",
						maxAttempts: 3,
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			});

			// 1. Schedule the cron task
			const id = await db.scheduleCronExecution({
				spec: {
					task_key: "slow-task",
					queue: "default",
					payload: {},
					cron_expression: "*/2 * * * * *",
				},
				scheduleName: "slow-schedule",
			});

			expect(db.getExecution(id)?.cron_expression).toBe("*/2 * * * * *");

			// Advance time to make cron execution ready (*/2 means every 2 seconds)
			db.advanceTime(2001);

			// 2. Get execution (marks as running)
			const batch = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			expect(batch.length).toBe(1);
			expect(batch[0]?.id).toBe(id);
			expect(db.getExecution(id!)?.state).toBe("running");

			// 3. Unschedule while task is running
			await db.unscheduleCronExecution({
				taskKey: "slow-task",
				queue: "default",
				scheduleName: "slow-schedule",
			});

			// Verify schedule was removed
			expect(db.getCronSchedules("slow-task").length).toBe(0);

			// 4. Task fails (simulating cancellation error)
			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "slow-task",
					status: "failed",
					error: "Task was cancelled",
				},
			]);

			// 5. Verify execution is pending retry but cron_expression is cleared
			const exec = db.getExecution(id!);
			expect(exec?.state).toBe("pending");
			expect(exec?.attempts).toBe(1);
			expect(exec?.cron_expression).toBeNull();
			expect(exec?.last_error).toBe("Task was cancelled");

			// 6. Advance time past backoff (15 seconds)
			db.advanceTime(16000);

			// 7. Execute the retry
			const retryBatch = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			expect(retryBatch.length).toBe(1);
			expect(retryBatch[0]?.id).toBe(id);

			// 8. Retry also fails
			await db.returnExecutions([
				{
					execution_id: id!,
					queue: "default",
					task_key: "slow-task",
					status: "failed",
					error: "Task was cancelled",
				},
			]);

			// 9. CRITICAL: Verify no future cron executions exist
			const allExecs = db.getAllExecutions();
			const futureCronExecs = allExecs.filter(
				(e) => e.task_key === "slow-task" && e.cron_expression !== null,
			);

			expect(futureCronExecs.length).toBe(0);

			// 10. Verify the original execution is still retryable (not completed)
			const finalExec = db.getExecution(id);
			expect(finalExec?.state).toBe("pending");
			expect(finalExec?.attempts).toBe(2);
		});
	});

	describe("Parent-Child Executions", () => {
		test("invoke_child creates child and sets parent waiting", async () => {
			const db = new InMemoryDatabaseClient();

			const parentId = await db.invoke({
				task_key: "parent",
				queue: "default",
				payload: {},
			});

			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: parentId!,
					queue: "default",
					task_key: "parent",
					status: "invoke_child",
					timeout_ms: 5000,
					step_key: "child-step",
					child_task_name: "child",
					child_task_queue: "default",
					child_payload: { data: "test" },
				},
			]);

			const parent = db.getExecution(parentId!);
			expect(parent?.state).toBe("pending");
			expect(parent?.waiting_on_execution_id).toBeTruthy();
			expect(parent?.waiting_step_key).toBe("child-step");

			const child = db.getExecution(parent!.waiting_on_execution_id!);
			expect(child).toBeDefined();
			expect(child?.task_key).toBe("child");
			expect(child?.parent_execution_id).toBe(parentId);
			expect(child?.parent_step_key).toBe("child-step");
		});

		test("child completion wakes up parent", async () => {
			const db = new InMemoryDatabaseClient();

			const parentId = await db.invoke({
				task_key: "parent",
				queue: "default",
				payload: {},
			});

			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: parentId!,
					queue: "default",
					task_key: "parent",
					status: "invoke_child",
					timeout_ms: 5000,
					step_key: "child-step",
					child_task_name: "child",
					child_task_queue: "default",
					child_payload: {},
				},
			]);

			const childId = db.getExecution(parentId!)!.waiting_on_execution_id!;

			// Execute child
			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: childId,
					queue: "default",
					task_key: "child",
					status: "completed",
					result: { value: 42 },
				},
			]);

			// Parent should be woken up
			const parent = db.getExecution(parentId!);
			expect(parent?.state).toBe("pending");
			expect(parent?.waiting_on_execution_id).toBeNull();
			expect(parent?.run_at.getTime()).toBeLessThanOrEqual(db.getCurrentTime().getTime());
		});

		test("child failure fails parent (cascade)", async () => {
			const db = new InMemoryDatabaseClient();

			await db.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "child",
						maxAttempts: 1,
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			});

			const parentId = await db.invoke({
				task_key: "parent",
				queue: "default",
				payload: {},
			});

			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: parentId!,
					queue: "default",
					task_key: "parent",
					status: "invoke_child",
					timeout_ms: 5000,
					step_key: "child-step",
					child_task_name: "child",
					child_task_queue: "default",
					child_payload: {},
				},
			]);

			const childId = db.getExecution(parentId!)!.waiting_on_execution_id!;

			// Fail child permanently
			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: childId,
					queue: "default",
					task_key: "child",
					status: "failed",
					error: "Child failed",
				},
			]);

			// Parent should cascade fail
			const parent = db.getExecution(parentId!);
			expect(parent?.state).toBe("failed");
			expect(parent?.last_error).toContain("Child execution failed");
		});
	});

	describe("Steps", () => {
		test("saveStep stores step result", async () => {
			const db = new InMemoryDatabaseClient();

			const execId = await db.invoke({
				task_key: "test",
				queue: "default",
				payload: {},
			});

			await db.saveStep({
				executionId: execId!,
				queue: "default",
				key: "step1",
				result: { data: "value" },
			});

			const result = await db.loadStep({
				executionId: execId!,
				key: "step1",
			});

			expect(result).toEqual({ data: "value" });
		});

		test("loadStep returns null for non-existent step", async () => {
			const db = new InMemoryDatabaseClient();

			const result = await db.loadStep({
				executionId: "nonexistent",
				key: "step1",
			});

			expect(result).toBeNull();
		});
	});

	describe("Time Control", () => {
		test("advanceTime moves current time forward", () => {
			const db = new InMemoryDatabaseClient(new Date("2024-01-01T00:00:00Z"));

			expect(db.getCurrentTime().toISOString()).toBe("2024-01-01T00:00:00.000Z");

			db.advanceTime(5000);

			expect(db.getCurrentTime().toISOString()).toBe("2024-01-01T00:00:05.000Z");
		});

		test("executions not ready until run_at reached", async () => {
			const db = new InMemoryDatabaseClient(new Date("2024-01-01T00:00:00Z"));

			await db.invoke({
				task_key: "test",
				queue: "default",
				payload: {},
				run_at: new Date("2024-01-01T00:00:10Z"),
			});

			// Should not be returned yet
			let executions = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			expect(executions.length).toBe(0);

			// Advance time
			db.advanceTime(10000);

			// Now should be returned
			executions = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			expect(executions.length).toBe(1);
		});
	});

	describe("Concurrency Control", () => {
		test("respects task concurrency limit", async () => {
			const db = new InMemoryDatabaseClient();

			await db.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "limited-task",
						concurrency: 2,
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			});

			// Create 5 executions
			for (let i = 0; i < 5; i++) {
				await db.invoke({
					task_key: "limited-task",
					queue: "default",
					payload: { i },
				});
			}

			// Should only get 2 (concurrency limit)
			const batch1 = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: ["limited-task"],
				filterTaskKeys: [],
			});

			expect(batch1.length).toBe(2);

			// Trying to get more should return 0 (limit reached)
			const batch2 = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: ["limited-task"],
				filterTaskKeys: [],
			});

			expect(batch2.length).toBe(0);

			// Complete one
			await db.returnExecutions([
				{
					execution_id: batch1[0]!.id,
					queue: "default",
					task_key: "limited-task",
					status: "completed",
				},
			]);

			// Now should get 1 more
			const batch3 = await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: ["limited-task"],
				filterTaskKeys: [],
			});

			expect(batch3.length).toBe(1);
		});
	});

	describe("Deduplication", () => {
		test("dedupe_key prevents duplicate executions", async () => {
			const db = new InMemoryDatabaseClient();

			const id1 = await db.invoke({
				task_key: "test",
				queue: "default",
				payload: {},
				dedupe_key: "unique-key",
			});

			const id2 = await db.invoke({
				task_key: "test",
				queue: "default",
				payload: {},
				dedupe_key: "unique-key",
			});

			// Should return same ID
			expect(id2).toBe(id1);

			// Should only have one execution
			expect(db.getAllExecutions().length).toBe(1);
		});

		test("dedupe allows duplicate after completion", async () => {
			const db = new InMemoryDatabaseClient();

			const id1 = await db.invoke({
				task_key: "test",
				queue: "default",
				payload: {},
				dedupe_key: "unique-key",
			});

			await db.getExecutions({
				orchestratorId: "test-orch",
				queueName: "default",
				batchSize: 10,
				taskKeysWithConcurrency: [],
				filterTaskKeys: [],
			});

			await db.returnExecutions([
				{
					execution_id: id1!,
					queue: "default",
					task_key: "test",
					status: "completed",
				},
			]);

			// Now can create another with same dedupe key
			const id2 = await db.invoke({
				task_key: "test",
				queue: "default",
				payload: {},
				dedupe_key: "unique-key",
			});

			expect(id2).not.toBe(id1);
			expect(db.getPendingExecutions().length).toBe(1);
		});
	});
});

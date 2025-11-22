import { mock } from "bun:test";
import type { DatabaseClient } from "../../src/database-client";

type PublicMethodsOf<T> = {
	[K in keyof T as T[K] extends Function ? K : never]: T[K];
};

type IDatabaseClient = PublicMethodsOf<DatabaseClient>;

export class MockDatabaseClient implements IDatabaseClient {
	close = mock(async () => {});
	orchestratorHeartbeat = mock(async () => false);
	recoverStaleOrchestrators = mock(async () => {});
	sweepOrchestrators = mock(async () => {});
	getInstalledMigrationNumber = mock(async () => -1);
	applyMigration = mock(async () => "applied" as const);
	countActiveOrchestratorsBelow = mock(async () => 0);
	orchestratorShutdown = mock(async () => {});
	getExecutions = mock(async () => []);
	returnExecutions = mock(async (results, signal) => {});
	removeExecutions = mock(async () => false);
	registerWorker = mock(async () => {});
	invoke = mock(async () => "mock-id");
	invokeChild = mock(async () => "mock-child-id");
	invokeBatch = mock(async () => ["mock-id"]);
	loadStep = mock(async () => null);
	saveStep = mock(async (executionId, queue, key, result, runAtMs, signal) => {});
	clearWaitingState = mock(async () => {});
	setFakeTime = mock(async () => {});
	clearFakeTime = mock(async () => {});
	subscribeEvent = mock(async () => "mock-subscription-id");
	subscribeDbChange = mock(async () => "mock-subscription-id");
	emitEvent = mock(async () => "mock-event-id");

	constructor(overrides: Partial<IDatabaseClient> = {}) {
		Object.assign(this, overrides);
	}
}
